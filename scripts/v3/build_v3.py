#!/usr/bin/env python3
"""
v3 后处理流水线 — 在 v2 build 之后做以下三件事：

1. 词条白名单 / 黑名单过滤：
   - terms_content：仅保留单字 OR 双字白名单（211 真题实词 + 安全合成词集）。
   - terms_function：仅保留高考考查 18 虚词 + 真题已考虚词 = 24 字。
   - 教材题库 textbook_article_bank：保留 v2 已筛成词级目标的教材实词题，避免按真题字头过度删减篇目覆盖。

2. 北京卷 2002–2025 真题题库重做：
   - 读 data/manual/v3/exam_solutions_*.json
   - 为每条 v3 解析生成 BankItem + AnswerKey
   - 写入 exam_questions.challenge_bank 和 answer_keys
   - 旧的 v2 真题挑战（推断答案）被替换或剔除。

3. 写出 runtime / public / src/generated 三处文件，并刷新 manifest sha256/size。

用法：
    python3 scripts/v3/build_v3.py
（必须先跑 npm run build:data 或 build_runtime_data.py 生成 v2 输出）
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "v3"))

from whitelist import (  # noqa: E402
    FUNCTION_WHITELIST,
    HEADWORD_BLACKLIST_EXACT,
    SAFE_TWO_CHAR_CONTENT,
    is_acceptable_content_headword,
    is_acceptable_function_headword,
    load_real_exam_shici,
)


RUNTIME_DIR = REPO_ROOT / "data" / "runtime"
PUBLIC_RUNTIME_DIR = REPO_ROOT / "public" / "runtime"
PRIVATE_RUNTIME_DIR = REPO_ROOT / "data" / "runtime_private"
GENERATED_DIR = REPO_ROOT / "src" / "generated"
V3_SOLUTIONS_DIR = REPO_ROOT / "data" / "manual" / "v3"
DOCS_DIR = REPO_ROOT / "docs"

ASSET_MAX_BYTES = 5 * 1024 * 1024


# ────────────────────────────────────────────────────────────────────────────
# Utility


def _read_json(p: Path) -> Any:
    with p.open() as f:
        return json.load(f)


def _write_json(p: Path, data: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _split_list_into_shards(items: list, base_name: str, target_dir: Path) -> list[dict]:
    """Split a long list into multiple part files of <= ASSET_MAX_BYTES."""
    target_dir.mkdir(parents=True, exist_ok=True)
    shards: list[dict] = []
    cur: list = []
    cur_bytes = 2  # opening []
    part = 1
    sep_bytes = 2  # ', '

    def flush() -> None:
        nonlocal cur, cur_bytes, part
        if not cur:
            return
        fn = f"{base_name}.part{part}.json" if part > 1 or len(cur) < len(items) else f"{base_name}.json"
        path = target_dir / fn
        _write_json(path, cur)
        shards.append({"file_name": fn, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
        cur = []
        cur_bytes = 2
        part += 1

    for it in items:
        s = json.dumps(it, ensure_ascii=False)
        b = len(s.encode("utf-8"))
        if cur and cur_bytes + sep_bytes + b > ASSET_MAX_BYTES:
            flush()
        cur.append(it)
        cur_bytes += sep_bytes + b
    flush()

    if len(shards) == 1:
        # Rename single shard back to base name
        old = target_dir / shards[0]["file_name"]
        new = target_dir / f"{base_name}.json"
        if old != new:
            old.rename(new)
            shards[0] = {"file_name": new.name, "size_bytes": new.stat().st_size, "sha256": _sha256(new)}
    return shards


def _split_object_into_shards(obj: dict, base_name: str, target_dir: Path) -> list[dict]:
    """Split a large object into shards by partitioning keys."""
    target_dir.mkdir(parents=True, exist_ok=True)
    shards: list[dict] = []
    keys = list(obj.keys())
    cur: dict = {}
    cur_bytes = 2
    part = 1
    sep_bytes = 2

    def flush() -> None:
        nonlocal cur, cur_bytes, part
        if not cur:
            return
        fn = f"{base_name}.part{part}.json" if part > 1 or len(cur) < len(keys) else f"{base_name}.json"
        path = target_dir / fn
        _write_json(path, cur)
        shards.append({"file_name": fn, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
        cur = {}
        cur_bytes = 2
        part += 1

    for k in keys:
        s = json.dumps({k: obj[k]}, ensure_ascii=False)
        b = len(s.encode("utf-8")) - 2  # subtract surrounding {}
        if cur and cur_bytes + sep_bytes + b > ASSET_MAX_BYTES:
            flush()
        cur[k] = obj[k]
        cur_bytes += sep_bytes + b
    flush()

    if len(shards) == 1:
        old = target_dir / shards[0]["file_name"]
        new = target_dir / f"{base_name}.json"
        if old != new:
            old.rename(new)
            shards[0] = {"file_name": new.name, "size_bytes": new.stat().st_size, "sha256": _sha256(new)}
    return shards


# ────────────────────────────────────────────────────────────────────────────
# Step 1 : 过滤词条 & 教材 challenge


def _real_exam_term_to_content_record(head: str, src: dict) -> dict:
    """Convert a `dict_exam_shici.terms[i]` record to a wy `terms_content` term record."""
    occurrences = src.get("occurrences", []) or []
    sample_glosses = src.get("sample_glosses", []) or []
    years = src.get("years", []) or []
    year_range = (min(years), max(years)) if years else (None, None)
    must_master_basis = []
    for occ in occurrences[:6]:
        must_master_basis.append({
            "basis_type": "direct_choice",
            "exam_year": occ.get("year"),
            "question_number": occ.get("question_number"),
            "evidence_sentence": occ.get("sentence", "") or occ.get("source_text", ""),
            "answer_span": occ.get("answer_span", "") or occ.get("gloss", ""),
            "why_required": f"{occ.get('year')} 真题中考查 {head} 的证据。",
            "confidence": 0.92,
            "needs_manual_review": False,
        })
    return {
        "term_id": f"content::{head}",
        "kind": "content_word",
        "headword": head,
        "display_headword": head,
        "must_master": True,
        "must_master_basis": must_master_basis,
        "beijing_frequency": int(src.get("beijing_occurrences", 0) or 0),
        "national_frequency": int(src.get("national_occurrences", 0) or 0),
        "year_range": list(year_range),
        "question_type_counts": dict(src.get("question_type_counts", {}) or {}),
        "frequencies": {
            "total": int(src.get("total_occurrences", 0) or 0),
            "beijing": int(src.get("beijing_occurrences", 0) or 0),
            "national": int(src.get("national_occurrences", 0) or 0),
        },
        "usage_relations": [
            {"semantic_value": g, "evidence_count": 1} for g in sample_glosses[:6]
        ],
        "sample_glosses": sample_glosses,
        "textbook_refs": [],
        "dict_refs": [],
        "idiom_refs": [],
        "priority_level": "core" if int(src.get("total_occurrences", 0) or 0) >= 1 else "secondary",
        "needs_manual_review": False,
        "exam_evidence_only": True,
    }


def _ensure_function_record(head: str) -> dict:
    return {
        "term_id": f"function::{head}",
        "kind": "function_word",
        "headword": head,
        "display_headword": head,
        "must_master": True,
        "must_master_basis": [],
        "beijing_frequency": 0,
        "national_frequency": 0,
        "year_range": [None, None],
        "question_type_counts": {},
        "frequencies": {"total": 0, "beijing": 0, "national": 0},
        "usage_relations": [],
        "sample_glosses": [],
        "textbook_refs": [],
        "dict_refs": [],
        "idiom_refs": [],
        "priority_level": "core",
        "needs_manual_review": False,
        "synthesized_from_outline": True,
    }


def filter_terms_content(real_exam_shici: dict) -> tuple[list, list]:
    parts = sorted([p for p in RUNTIME_DIR.glob("terms_content.part*.json")] + [RUNTIME_DIR / "terms_content.json"])
    parts = [p for p in parts if p.exists()]
    all_terms: list = []
    for p in parts:
        all_terms.extend(_read_json(p))
    kept: list = []
    kept_heads: set[str] = set()
    dropped: list[tuple[str, str]] = []
    for t in all_terms:
        head = (t.get("display_headword") or t.get("headword") or "").strip()
        ok, reason = is_acceptable_content_headword(head, real_exam_shici)
        if ok:
            kept.append(t)
            kept_heads.add(head)
        else:
            dropped.append((head, reason))
    # Merge in 真题 字头 missing from textbook annotations (these are exam-evidence-only)
    injected = 0
    for head, src in real_exam_shici.items():
        if head in kept_heads:
            continue
        ok, _ = is_acceptable_content_headword(head, real_exam_shici)
        if not ok:
            continue
        kept.append(_real_exam_term_to_content_record(head, src))
        kept_heads.add(head)
        injected += 1
    print(f"      injected {injected} 真题 only entries")
    return kept, dropped


def filter_terms_function() -> tuple[list, list]:
    p = RUNTIME_DIR / "terms_function.json"
    terms = _read_json(p)
    kept: list = []
    kept_heads: set[str] = set()
    dropped: list[tuple[str, str]] = []
    for t in terms:
        head = (t.get("display_headword") or t.get("headword") or "").strip()
        ok, reason = is_acceptable_function_headword(head)
        if ok:
            kept.append(t)
            kept_heads.add(head)
        else:
            dropped.append((head, reason))
    # Inject 18 高考虚词清单中缺失的字头
    from whitelist import EXAM_OUTLINE_FUNCTION_WORDS  # local import to avoid circular at top
    injected = 0
    for head in EXAM_OUTLINE_FUNCTION_WORDS:
        if head in kept_heads:
            continue
        kept.append(_ensure_function_record(head))
        kept_heads.add(head)
        injected += 1
    print(f"      injected {injected} 高考虚词清单字头")
    return kept, dropped


def filter_textbook_article_bank() -> tuple[dict, dict]:
    parts = sorted(RUNTIME_DIR.glob("textbook_article_bank.part*.json")) + [RUNTIME_DIR / "textbook_article_bank.json"]
    parts = [p for p in parts if p.exists()]
    bank: dict = {}
    for p in parts:
        bank.update(_read_json(p))
    out_bank: dict = {}
    drop_stats = {"items_kept": 0, "items_dropped": 0, "articles_with_zero_after": 0}
    for article_id, payload in bank.items():
        items = payload.get("items", [])
        kept_items: list = []
        for item in items:
            term_id = str(item.get("term_id") or "")
            term_ids = [str(t) for t in (item.get("term_ids") or [])]
            check_ids = [term_id] + term_ids
            check_ids = [t for t in check_ids if t]
            if not check_ids:
                # No term info — drop
                drop_stats["items_dropped"] += 1
                continue
            kept_items.append(item)
            drop_stats["items_kept"] += 1
        if not kept_items:
            drop_stats["articles_with_zero_after"] += 1
            # Still keep the article entry but with empty items, so catalog references stay valid
        new_payload = dict(payload)
        new_payload["items"] = kept_items
        # Update article counters
        article_meta = new_payload.get("article", {}) or {}
        article_meta["challenge_count"] = len(kept_items)
        article_meta["content_count"] = sum(1 for it in kept_items if it.get("kind") == "content_word")
        article_meta["function_count"] = sum(1 for it in kept_items if it.get("kind") == "function_word")
        new_payload["article"] = article_meta
        out_bank[article_id] = new_payload
    return out_bank, drop_stats


# ────────────────────────────────────────────────────────────────────────────
# Step 2 : 真题 v3 解析 → BankItem + AnswerKey


SUBTYPE_TO_QTYPE = {
    "shici_explanation": "content_gloss",
    "shici_modern_diff": "content_gloss",
    "shici_multi_explanation": None,  # Skip; complex 8-option layout not in default UI
    "xuci_pair_compare": "xuci_pair_compare",
    "xuci_explanation": "function_gloss",
    "sentence_meaning": "sentence_meaning",
}


# ── Context window assembly ──────────────────────────────────────────────────
import re as _re

_PUNCT_STRIP_RE = _re.compile(
    r"[，。：；！？、,.:;!?…．·\s　\"\"''《》〈〉「」『』（）()0-9​]+"
)
_SENTENCE_SPLIT_RE = _re.compile(r"[^。！？；…]+(?:[。！？；…]+|[\"」』])?")
_ANNOTATION_RE = _re.compile(r"\*\*[^*]+\*\*")

_EXAM_CORPUS_PATH = REPO_ROOT / "data" / "runtime_private" / "exam_classical_corpus.md"
CONTEXT_WINDOW_RADIUS = 3


def _normalize_for_match(text: str) -> str:
    return _PUNCT_STRIP_RE.sub("", text or "")


def _split_passage_sentences(text: str) -> list[str]:
    out: list[str] = []
    if not text:
        return out
    for raw in text.split("\n"):
        raw = raw.strip()
        if not raw:
            continue
        pieces = _SENTENCE_SPLIT_RE.findall(raw)
        if not pieces:
            out.append(raw)
            continue
        for piece in pieces:
            piece = piece.strip()
            if piece:
                out.append(piece)
    return out


def _load_exam_passages() -> dict[str, list[str]]:
    """Return {paper_key: [sentence, ...]} from corpus + manual fallback."""
    passages: dict[str, list[str]] = {}
    if _EXAM_CORPUS_PATH.exists():
        text = _EXAM_CORPUS_PATH.read_text(encoding="utf-8")
        for part in text.split("\n## ")[1:]:
            lines = part.split("\n")
            if len(lines) < 2:
                continue
            m = _re.match(r"^(\S+)\s*/\s*(\d+)\s*/\s*(\S+)\s*/\s*(\S+)$", lines[1].strip())
            if not m:
                continue
            paper_key = m.group(4).strip()
            body = _ANNOTATION_RE.sub("", "\n".join(lines[2:])).strip()
            sentences = _split_passage_sentences(body)
            if sentences:
                passages[paper_key] = sentences
    # Fallback: passage_excerpt from manual files for any missing papers
    for f in sorted(V3_SOLUTIONS_DIR.glob("exam_solutions_*.json")):
        try:
            data = _read_json(f)
        except Exception:
            continue
        meta = data.get("_meta", {})
        pk = meta.get("paper_key")
        if not pk or pk in passages:
            continue
        excerpt = meta.get("passage_excerpt", "") or ""
        sentences = _split_passage_sentences(excerpt)
        if sentences:
            passages[pk] = sentences
    return passages


def _find_sentence_index(sentences_norm: list[str], target_norm: str) -> int:
    if not target_norm:
        return -1
    for idx, sn in enumerate(sentences_norm):
        if target_norm in sn:
            return idx
    if len(target_norm) >= 5:
        head = target_norm[:5]
        for idx, sn in enumerate(sentences_norm):
            if head in sn:
                return idx
    if len(target_norm) >= 6:
        for size in range(min(len(target_norm), 8), 4, -1):
            for start in range(0, len(target_norm) - size + 1):
                chunk = target_norm[start : start + size]
                for idx, sn in enumerate(sentences_norm):
                    if chunk in sn:
                        return idx
    return -1


def _build_option_context_window(
    passage_sentences: list[str],
    option_sentences: list[str],
) -> list[str]:
    if not passage_sentences:
        return []
    sentences_norm = [_normalize_for_match(s) for s in passage_sentences]
    picked: set[int] = set()
    for sent in option_sentences:
        idx = _find_sentence_index(sentences_norm, _normalize_for_match(sent))
        if idx < 0:
            continue
        start = max(0, idx - CONTEXT_WINDOW_RADIUS)
        end = min(len(passage_sentences), idx + CONTEXT_WINDOW_RADIUS + 1)
        for i in range(start, end):
            picked.add(i)
    return [passage_sentences[i] for i in sorted(picked)]


def _option_sentence_contexts(
    passage_sentences: list[str],
    option_sentences: list[str],
) -> list[list[str]]:
    """Return one context window per option sentence for tooltips/display metadata."""
    contexts: list[list[str]] = []
    for sent in option_sentences:
        if not sent:
            contexts.append([])
            continue
        contexts.append(_build_option_context_window(passage_sentences, [sent]))
    return contexts


_EXAM_PASSAGE_CACHE: dict[str, list[str]] | None = None


def _passages() -> dict[str, list[str]]:
    global _EXAM_PASSAGE_CACHE
    if _EXAM_PASSAGE_CACHE is None:
        _EXAM_PASSAGE_CACHE = _load_exam_passages()
    return _EXAM_PASSAGE_CACHE


def _challenge_id_for(paper_key: str, q: int, sub: int | None, qsubtype: str) -> str:
    base = f"{paper_key.lower()}-q{q}"
    if sub is not None:
        base += f"s{sub}"
    return f"v3-{qsubtype}-{base}"


def build_exam_challenges_and_keys() -> tuple[list[dict], dict, list[dict]]:
    """Read v3 solutions and emit (bank_items, answer_keys, doc_summaries)."""
    bank_items: list[dict] = []
    answer_keys: dict = {}
    doc_summaries: list[dict] = []

    files = sorted(V3_SOLUTIONS_DIR.glob("exam_solutions_*.json"))
    for f in files:
        data = _read_json(f)
        meta = data.get("_meta", {})
        year = meta.get("year")
        paper = meta.get("paper", "北京卷")
        paper_key = meta.get("paper_key")
        passage_title = meta.get("passage_title", "")
        passage_excerpt = meta.get("passage_excerpt", "")
        if not paper_key:
            continue

        for entry_key, entry in data.items():
            if entry_key.startswith("_"):
                continue
            qsubtype = entry.get("question_subtype", "")
            qtype = SUBTYPE_TO_QTYPE.get(qsubtype)
            if qtype is None:
                continue
            qnum = entry.get("question_number")
            sub_idx = entry.get("sub_index")
            challenge_id = _challenge_id_for(paper_key, qnum, sub_idx, qtype)

            stem = entry.get("stem", "")
            kind = entry.get("kind", "content_word")
            primary_term = entry.get("primary_term", "")
            asks_for = entry.get("asks_for", "find_wrong")

            options_dict = entry.get("options", {})
            options_payload = []
            term_ids_set: set[str] = set()
            primary_term_id = ""
            passage_sentences = _passages().get(paper_key, [])
            option_sentences_for_context: list[str] = []

            for label in ["A", "B", "C", "D"]:
                opt = options_dict.get(label, {})
                opt_term = opt.get("term", primary_term)
                opt_term_id = (
                    f"function::{opt_term}" if kind == "function_word" else f"content::{opt_term}"
                )
                term_ids_set.add(opt_term_id)
                if opt.get("is_target"):
                    primary_term_id = opt_term_id
                # Build option text
                if qsubtype == "xuci_pair_compare":
                    sents = opt.get("sentences", [])
                    text = "  ◆  ".join(sents)
                elif qsubtype == "shici_explanation":
                    sent = opt.get("sentence", "")
                    given = opt.get("given_gloss", "")
                    text = f"{sent} —— {given}" if given else sent
                elif qsubtype == "shici_modern_diff":
                    sent = opt.get("sentence", "")
                    text = sent
                elif qsubtype == "sentence_meaning":
                    sent = opt.get("sentence", "")
                    given = opt.get("given_reading", "")
                    text = f"{sent} —— {given}" if given else sent
                elif qsubtype == "xuci_explanation":
                    sent = opt.get("sentence", "")
                    given = opt.get("given_function", "")
                    text = f"{sent} —— {given}" if given else sent
                else:
                    text = opt.get("sentence", "") or opt.get("text", "")
                option_sentences = []
                if opt.get("sentence"):
                    option_sentences.append(opt["sentence"])
                for s in opt.get("sentences", []) or []:
                    if s:
                        option_sentences.append(s)
                option_sentences = list(dict.fromkeys(option_sentences))
                option_sentences_for_context.extend(option_sentences)

                option_payload = {
                    "label": label,
                    "term_id": opt_term_id,
                    "headword": opt_term,
                    "sentence": opt.get("sentence", "") or (opt.get("sentences", [""])[0] if opt.get("sentences") else ""),
                    "text": text,
                    "origin": "v3_manual",
                }
                if option_sentences:
                    option_payload["sentences"] = option_sentences
                    option_payload["sentence_contexts"] = _option_sentence_contexts(
                        passage_sentences, option_sentences
                    )
                    if option_payload["sentence_contexts"]:
                        option_payload["context_window"] = option_payload["sentence_contexts"][0]
                options_payload.append(option_payload)

            if not primary_term_id:
                primary_term_id = (
                    f"function::{primary_term}" if kind == "function_word" else f"content::{primary_term}"
                )
                term_ids_set.add(primary_term_id)

            sentence_focus = ""
            for opt in options_dict.values():
                if opt.get("is_target"):
                    sentence_focus = opt.get("sentence", "") or (opt.get("sentences", [""])[0] if opt.get("sentences") else "")
                    break
            if not sentence_focus:
                sentence_focus = passage_excerpt[:60]

            source_label = f"{year} 年{paper}《{passage_title}》第 {qnum}{f'·{sub_idx}' if sub_idx else ''} 题"

            # Build a per-question context window (±3 sentences around each option sentence
            # that belongs to the source passage).
            context_window = _build_option_context_window(passage_sentences, option_sentences_for_context)
            if not context_window:
                context_window = [passage_excerpt] if passage_excerpt else []

            bank_item = {
                "challenge_id": challenge_id,
                "question_type": qtype,
                "kind": kind,
                "source_kind": "exam",
                "term_id": primary_term_id,
                "term_ids": sorted(term_ids_set),
                "priority_level": "core",
                "source_label": source_label,
                "source_title": passage_title,
                "source_meta": {
                    "year": year,
                    "paper": paper,
                    "paper_key": paper_key,
                    "question_number": qnum,
                    "sub_index": sub_idx,
                    "question_subtype": qsubtype,
                    "asks_for": asks_for,
                    "verification": "manual_authored_v3",
                },
                "stem": stem,
                "sentence": sentence_focus,
                "context_window": context_window,
                "options": options_payload,
                "article_id": paper_key,
            }
            bank_items.append(bank_item)

            # Build answer_key
            correct_label = entry.get("correct_label", "")
            correct_opt = options_dict.get(correct_label, {})
            correct_text = ""
            if qsubtype == "xuci_pair_compare":
                correct_text = "  ◆  ".join(correct_opt.get("sentences", []))
            elif qsubtype in ("shici_explanation", "shici_modern_diff"):
                correct_text = correct_opt.get("sentence", "")
                gloss = correct_opt.get("actual_gloss") or correct_opt.get("given_gloss") or ""
                if gloss:
                    correct_text = f"{correct_text} —— {gloss}"
            elif qsubtype == "sentence_meaning":
                correct_text = correct_opt.get("sentence", "")
            elif qsubtype == "xuci_explanation":
                correct_text = correct_opt.get("sentence", "")

            option_analyses = []
            for label in ["A", "B", "C", "D"]:
                opt = options_dict.get(label, {})
                if qsubtype == "xuci_pair_compare":
                    sents = opt.get("sentences", [])
                    base_txt = "  ◆  ".join(sents)
                elif qsubtype in ("shici_explanation", "shici_modern_diff", "xuci_explanation"):
                    sent = opt.get("sentence", "")
                    given = opt.get("given_gloss") or opt.get("given_function") or opt.get("given_modern_reading", "")
                    base_txt = f"{sent} —— {given}" if given else sent
                elif qsubtype == "sentence_meaning":
                    sent = opt.get("sentence", "")
                    given = opt.get("given_reading", "")
                    base_txt = f"{sent} —— {given}" if given else sent
                else:
                    base_txt = opt.get("sentence", "") or opt.get("text", "")
                option_analyses.append({
                    "label": label,
                    "text": base_txt,
                    "is_correct": label == correct_label,
                    "analysis": opt.get("analysis", ""),
                })

            ak = {
                "challenge_id": challenge_id,
                "kind": kind,
                "question_type": qtype,
                "term_id": primary_term_id,
                "term_ids": sorted(term_ids_set),
                "priority_level": "core",
                "source_type": "exam",
                "source_ref": {
                    "year": year,
                    "paper": paper,
                    "paper_key": paper_key,
                    "question_number": qnum,
                    "sub_index": sub_idx,
                },
                "correct_label": correct_label,
                "correct_text": correct_text,
                "explanation": entry.get("explanation", ""),
                "dict_support": [],
                "textbook_support": [],
                "option_analyses": option_analyses,
                "verification": "manual_authored_v3",
            }
            answer_keys[challenge_id] = ak

        doc_summaries.append({
            "paper_key": paper_key,
            "year": year,
            "paper": paper,
            "passage_title": passage_title,
            "challenge_count": sum(1 for x in bank_items if x["source_meta"]["paper_key"] == paper_key),
        })

    return bank_items, answer_keys, doc_summaries


# ────────────────────────────────────────────────────────────────────────────
# Step 3 : 集成到 exam_questions / answer_keys 并写出


def integrate_and_write(
    kept_terms_content: list,
    kept_terms_function: list,
    new_textbook_bank: dict,
    v3_bank_items: list[dict],
    v3_answer_keys: dict,
) -> dict:
    # Load existing v2 outputs
    parts2 = sorted(RUNTIME_DIR.glob("exam_questions.part*.json"))
    eq_combined: dict = {}
    for p in parts2:
        eq_combined.update(_read_json(p))
    # Specifically: question_docs (part1), challenge_bank (part2), question_templates (part3)
    question_docs = eq_combined.get("question_docs", [])
    challenge_bank = eq_combined.get("challenge_bank", {})
    question_templates = eq_combined.get("question_templates", {})

    # Replace exam-source items in challenge_bank with v3 items per question_type
    new_bank: dict[str, list] = {}
    for qt in ["xuci_pair_compare", "function_gloss", "function_profile", "content_gloss", "sentence_meaning", "passage_meaning"]:
        existing = challenge_bank.get(qt, []) or []
        # Drop all source_kind == "exam" — v3 will replace
        textbook_items = [it for it in existing if str(it.get("source_kind", "")) != "exam"]
        # Add v3 items of this question_type
        v3_items_for_qt = [it for it in v3_bank_items if it["question_type"] == qt]
        new_bank[qt] = textbook_items + v3_items_for_qt

    def _ok_item(it: dict) -> bool:
        if str(it.get("source_kind", "")) == "exam":
            return True  # v3 exam items already filtered/built
        tid = str(it.get("term_id") or "")
        return bool(tid)

    for qt, items in new_bank.items():
        new_bank[qt] = [it for it in items if _ok_item(it)]

    # Rebuild assets & write
    built_at = datetime.now(timezone.utc).isoformat()
    assets_meta: dict[str, dict] = {}

    # 1. terms_function
    p = PUBLIC_RUNTIME_DIR / "terms_function.json"
    _write_json(p, kept_terms_function)
    _write_json(RUNTIME_DIR / "terms_function.json", kept_terms_function)
    assets_meta["terms_function"] = {
        "kind": "list",
        "shards": [{"file_name": "terms_function.json", "size_bytes": p.stat().st_size, "sha256": _sha256(p)}],
    }

    # 2. terms_content (sharded)
    # Wipe old shards in PUBLIC and RUNTIME first
    for d in (PUBLIC_RUNTIME_DIR, RUNTIME_DIR):
        for old in d.glob("terms_content*.json"):
            old.unlink()
    # Write into PUBLIC, then mirror
    shards_pub = _split_list_into_shards(kept_terms_content, "terms_content", PUBLIC_RUNTIME_DIR)
    for s in shards_pub:
        shutil.copy(PUBLIC_RUNTIME_DIR / s["file_name"], RUNTIME_DIR / s["file_name"])
    assets_meta["terms_content"] = {"kind": "list", "shards": shards_pub}

    # 3. textbook_article_bank (sharded by article_id)
    for d in (PUBLIC_RUNTIME_DIR, RUNTIME_DIR):
        for old in d.glob("textbook_article_bank*.json"):
            old.unlink()
    shards_pub = _split_object_into_shards(new_textbook_bank, "textbook_article_bank", PUBLIC_RUNTIME_DIR)
    for s in shards_pub:
        shutil.copy(PUBLIC_RUNTIME_DIR / s["file_name"], RUNTIME_DIR / s["file_name"])
    assets_meta["textbook_article_bank"] = {"kind": "object", "shards": shards_pub}

    # 4. textbook_article_catalog must mirror the filtered article bank counters.
    for d in (PUBLIC_RUNTIME_DIR, RUNTIME_DIR):
        for old in d.glob("textbook_article_catalog*.json"):
            old.unlink()
    new_textbook_catalog = sorted(
        [dict(payload.get("article") or {}) for payload in new_textbook_bank.values()],
        key=lambda item: (
            str(item.get("book_key") or ""),
            int(item.get("page_start") or 0),
            str(item.get("title") or ""),
        ),
    )
    shards_pub = _split_list_into_shards(new_textbook_catalog, "textbook_article_catalog", PUBLIC_RUNTIME_DIR)
    for s in shards_pub:
        shutil.copy(PUBLIC_RUNTIME_DIR / s["file_name"], RUNTIME_DIR / s["file_name"])
    assets_meta["textbook_article_catalog"] = {"kind": "list", "shards": shards_pub}

    # 5. exam_questions (3 parts: question_docs, challenge_bank, question_templates)
    for d in (PUBLIC_RUNTIME_DIR, RUNTIME_DIR):
        for old in d.glob("exam_questions*.json"):
            old.unlink()
    eq_p1 = {"built_at": built_at, "question_docs": question_docs}
    eq_p2 = {"challenge_bank": new_bank}
    eq_p3 = {"question_templates": question_templates}
    eq_shards: list[dict] = []
    for idx, payload in enumerate([eq_p1, eq_p2, eq_p3], start=1):
        fn = f"exam_questions.part{idx}.json"
        path = PUBLIC_RUNTIME_DIR / fn
        _write_json(path, payload)
        shutil.copy(path, RUNTIME_DIR / fn)
        eq_shards.append({"file_name": fn, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    assets_meta["exam_questions"] = {"kind": "object", "shards": eq_shards}

    # 6. Mirror previous unchanged assets (textbook_examples, dict_links, corpus_indexes,
    #    frequency tables, exam_tested_terms, function_usage_table,
    #    textbook_notes_*)
    pass_through_assets = [
        "textbook_examples", "dict_links", "corpus_indexes",
        "textbook_frequency_table", "exam_frequency_table", "union_frequency_table",
        "exam_tested_terms", "function_usage_table",
        "textbook_notes_table", "textbook_notes_content", "textbook_notes_function",
        "textbook_note_term_index", "textbook_note_stats",
    ]
    # Read old manifest for these
    old_manifest = _read_json(RUNTIME_DIR / "manifest.json")
    for asset_key in pass_through_assets:
        old_meta = old_manifest.get("assets", {}).get(asset_key)
        if not old_meta:
            continue
        # Re-checksum each shard from current PUBLIC dir
        new_shards = []
        for shard in old_meta.get("shards", []):
            fn = shard.get("file_name")
            path = PUBLIC_RUNTIME_DIR / fn
            if not path.exists():
                # Try copying from RUNTIME mirror
                src = RUNTIME_DIR / fn
                if src.exists():
                    shutil.copy(src, path)
            if path.exists():
                new_shards.append({"file_name": fn, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
        if new_shards:
            assets_meta[asset_key] = {"kind": old_meta.get("kind", "object"), "shards": new_shards}

    # 7. answer_keys.json (private + src/generated import)
    prev_keys: dict = {}
    ak_path_private = PRIVATE_RUNTIME_DIR / "answer_keys.json"
    if ak_path_private.exists():
        prev_keys = _read_json(ak_path_private)
    # Drop all v2 exam answer_keys (they used pipeline-inferred answers we don't trust)
    pruned = {k: v for k, v in prev_keys.items() if not (
        v.get("source_type") == "exam" and v.get("verification") != "manual_authored_v3"
    )}
    pruned.update(v3_answer_keys)
    _write_json(ak_path_private, pruned)
    _write_json(GENERATED_DIR / "answer_keys.json", pruned)

    # 8. data_quality.json + manifest.json
    stats = {
        "terms_content": len(kept_terms_content),
        "terms_function": len(kept_terms_function),
        "exam_question_docs": len(question_docs),
        "exam_v3_challenges": len(v3_bank_items),
        "textbook_article_count": len(new_textbook_catalog),
        "textbook_article_challenges": sum(int(item.get("challenge_count") or 0) for item in new_textbook_catalog),
        "challenge_counts": {qt: len(items) for qt, items in new_bank.items()},
        "answer_keys_total": len(pruned),
    }
    new_manifest = {
        "built_at": built_at,
        "v3_post_built_at": built_at,
        "asset_max_bytes": ASSET_MAX_BYTES,
        "assets": assets_meta,
        "stats": {**old_manifest.get("stats", {}), **stats},
    }
    _write_json(RUNTIME_DIR / "manifest.json", new_manifest)
    _write_json(PUBLIC_RUNTIME_DIR / "manifest.json", new_manifest)

    return stats


# ────────────────────────────────────────────────────────────────────────────


def main() -> int:
    print("[v3] loading upstream real-exam shici whitelist…")
    real_exam_shici = load_real_exam_shici()
    print(f"      {len(real_exam_shici)} 真题实词字头")

    print("[v3] filtering terms_content through whitelist…")
    kept_content, dropped_content = filter_terms_content(real_exam_shici)
    print(f"      kept {len(kept_content)} / dropped {len(dropped_content)}")

    print("[v3] filtering terms_function through 18+6 高考虚词清单…")
    kept_function, dropped_function = filter_terms_function()
    print(f"      kept {len(kept_function)} / dropped {len(dropped_function)}")

    print("[v3] retaining textbook_article_bank word-level challenge items…")
    new_textbook_bank, drop_stats = filter_textbook_article_bank()
    print(f"      kept {drop_stats['items_kept']} items / dropped {drop_stats['items_dropped']} items")

    print("[v3] building exam challenges from authored v3 solutions…")
    v3_bank_items, v3_answer_keys, doc_summaries = build_exam_challenges_and_keys()
    print(f"      v3 exam challenges: {len(v3_bank_items)} across {len(doc_summaries)} papers")

    print("[v3] integrating + writing runtime files + manifest…")
    stats = integrate_and_write(
        kept_content, kept_function, new_textbook_bank, v3_bank_items, v3_answer_keys
    )

    # Write a v3 audit doc
    audit = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "phase": "v3",
        "drop_content": len(dropped_content),
        "drop_function": len(dropped_function),
        "textbook_drop": drop_stats,
        "v3_exam_challenges": len(v3_bank_items),
        "v3_papers": len(doc_summaries),
        "answer_keys_total": stats["answer_keys_total"],
        "stats": stats,
        "drop_content_reasons": {
            r: sum(1 for _, x in dropped_content if x == r)
            for r in {x for _, x in dropped_content}
        },
        "v3_papers_breakdown": doc_summaries,
        "dropped_content_examples": [(h, r) for h, r in dropped_content[:50]],
    }
    _write_json(DOCS_DIR / "DATA_AUDIT_REPORT_V3.json", audit)
    audit_md = [
        f"# V3 数据审计报告\n",
        f"- 生成时间：{audit['built_at']}",
        f"- v3 阶段：词条白名单 + 教材词级题保留 + 北京卷 2002–2025 真题人工答案与解析\n",
        f"## 词条收口\n",
        f"- terms_content 保留：{stats['terms_content']}（剔除 {audit['drop_content']}）",
        f"- terms_function 保留：{stats['terms_function']}（剔除 {audit['drop_function']}）",
        f"- 教材题保留：{drop_stats['items_kept']} / 剔除：{drop_stats['items_dropped']}\n",
        f"## 北京卷真题题库\n",
        f"- 24 卷已逐题人工撰写答案与解析",
        f"- 共 {len(v3_bank_items)} 道独立 challenge 进入 GAOKAO 题池",
        f"- 题型分布：{stats['challenge_counts']}",
        f"- answer_keys 总数：{stats['answer_keys_total']}\n",
        f"## 剔除原因",
    ]
    for r, c in audit["drop_content_reasons"].items():
        audit_md.append(f"- {r}：{c}")
    audit_md.append("\n## 各年份题量")
    for d in doc_summaries:
        audit_md.append(f"- {d['paper_key']}（{d['year']}）：{d['challenge_count']} 题")
    (DOCS_DIR / "DATA_AUDIT_REPORT_V3.md").write_text("\n".join(audit_md))

    print("[v3] done.")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
