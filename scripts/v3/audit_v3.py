#!/usr/bin/env python3
"""
v3 数据硬门禁 — build_v3.py 跑完之后必须无失败地通过这些硬门：

  G1. 词条白名单实词 ≥ 211（上游真题字头）的覆盖率 ≥ 90%
  G2. 词条白名单虚词 = 高考考查 18 虚词全员覆盖
  G3. 每条 GAOKAO 池内 challenge 必须有 verification != "" 的 answer_key
  G4. 真题层每条 challenge 必须 verification == "manual_authored_v3"
       且 explanation 长度 ≥ 60；option_analyses[*].analysis 长度 ≥ 30
  G5. 教材层 challenge 必须是词级目标；每篇保留至少一道实词题，
       避免短语/句子题回流或篇目实词覆盖被过度删减。
  G6. 真题层每条 challenge 的 question-context 必须覆盖原文内每个选项句
       的上下 3 句；虚词比较题必须保留双句选项结构
  G7. 教材层每条 challenge 的正确项必须来自本题课下注释，干扰项必须
       来自其他教材注释，不允许回退到辞典义项或通用虚词目录。
  G8. 教材文章目录的 challenge_count/content_count/function_count 必须
       与过滤后的 textbook_article_bank 完全一致，避免首页统计漂移。

任一硬门失败即 exit(1)，并打印明确的失败摘要。
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "v3"))

from whitelist import (  # noqa: E402
    EXAM_OUTLINE_FUNCTION_WORDS,
    FUNCTION_WHITELIST,
    is_acceptable_content_headword,
    is_acceptable_function_headword,
    load_real_exam_shici,
)

PUBLIC_RUNTIME_DIR = REPO_ROOT / "public" / "runtime"
PRIVATE_RUNTIME_DIR = REPO_ROOT / "data" / "runtime_private"
GENERATED_DIR = REPO_ROOT / "src" / "generated"
DOCS_DIR = REPO_ROOT / "docs"
CONTEXT_WINDOW_RADIUS = 3

_PUNCT_STRIP_RE = re.compile(
    r"[，。：；！？、,.:;!?…．·\s　\"\"''《》〈〉「」『』（）()0-9​]+"
)
_SENTENCE_SPLIT_RE = re.compile(r"[^。！？；…]+(?:[。！？；…]+|[\"」』])?")
_ANNOTATION_RE = re.compile(r"\*\*[^*]+\*\*")


def _read_json(p: Path):
    with p.open() as f:
        return json.load(f)


def _read_sharded(base_name: str) -> list:
    parts = sorted(PUBLIC_RUNTIME_DIR.glob(f"{base_name}.part*.json")) + [PUBLIC_RUNTIME_DIR / f"{base_name}.json"]
    parts = [p for p in parts if p.exists()]
    out: list = []
    for p in parts:
        out.extend(_read_json(p))
    return out


def _read_sharded_dict(base_name: str) -> dict:
    parts = sorted(PUBLIC_RUNTIME_DIR.glob(f"{base_name}.part*.json")) + [PUBLIC_RUNTIME_DIR / f"{base_name}.json"]
    parts = [p for p in parts if p.exists()]
    out: dict = {}
    for p in parts:
        out.update(_read_json(p))
    return out


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
    path = PRIVATE_RUNTIME_DIR / "exam_classical_corpus.md"
    passages: dict[str, list[str]] = {}
    if not path.exists():
        return passages
    text = path.read_text(encoding="utf-8")
    for part in text.split("\n## ")[1:]:
        lines = part.split("\n")
        if len(lines) < 2:
            continue
        m = re.match(r"^(\S+)\s*/\s*(\d+)\s*/\s*(\S+)\s*/\s*(\S+)$", lines[1].strip())
        if not m:
            continue
        paper_key = m.group(4).strip()
        body = _ANNOTATION_RE.sub("", "\n".join(lines[2:])).strip()
        sentences = _split_passage_sentences(body)
        if sentences:
            passages[paper_key] = sentences
    return passages


def _find_sentence_index(sentences_norm: list[str], target_norm: str) -> int:
    if not target_norm:
        return -1
    for idx, sentence_norm in enumerate(sentences_norm):
        if target_norm in sentence_norm:
            return idx
    if len(target_norm) >= 5:
        head = target_norm[:5]
        for idx, sentence_norm in enumerate(sentences_norm):
            if head in sentence_norm:
                return idx
    if len(target_norm) >= 6:
        for size in range(min(len(target_norm), 8), 4, -1):
            for start in range(0, len(target_norm) - size + 1):
                chunk = target_norm[start : start + size]
                for idx, sentence_norm in enumerate(sentences_norm):
                    if chunk in sentence_norm:
                        return idx
    return -1


def _option_sentences(option: dict) -> list[str]:
    sentences: list[str] = []
    if option.get("sentence"):
        sentences.append(str(option["sentence"]))
    if isinstance(option.get("sentences"), list):
        for sentence in option["sentences"]:
            if sentence:
                sentences.append(str(sentence))
    return list(dict.fromkeys(sentences))


def main() -> int:
    failures: list[str] = []
    real_exam = load_real_exam_shici()

    # ---- G1
    terms_content = _read_sharded("terms_content")
    kept_heads = {t["headword"] for t in terms_content}
    real_heads = set(real_exam.keys())
    cov = len(kept_heads & real_heads) / max(1, len(real_heads))
    if cov < 0.90:
        failures.append(f"G1 真题实词覆盖率 {cov:.2%} < 90%")
    print(f"  G1 实词白名单实词覆盖率 = {cov:.2%}（{len(kept_heads & real_heads)} / {len(real_heads)}）")

    # ---- G2
    terms_function = _read_sharded("terms_function")
    fn_heads = {t["headword"] for t in terms_function}
    missing_outline = EXAM_OUTLINE_FUNCTION_WORDS - fn_heads
    if missing_outline:
        failures.append(f"G2 高考虚词清单缺：{sorted(missing_outline)}")
    print(f"  G2 高考虚词 18 全员到位：{not missing_outline}（清单计{len(EXAM_OUTLINE_FUNCTION_WORDS)}，已收{len(EXAM_OUTLINE_FUNCTION_WORDS & fn_heads)}）")

    # ---- G3 + G4
    parts2 = sorted(PUBLIC_RUNTIME_DIR.glob("exam_questions.part*.json"))
    eq_combined: dict = {}
    for p in parts2:
        eq_combined.update(_read_json(p))
    challenge_bank = eq_combined.get("challenge_bank", {})
    answer_keys = _read_json(GENERATED_DIR / "answer_keys.json")

    GAOKAO_QTYPES = {"content_gloss", "xuci_pair_compare", "function_gloss", "sentence_meaning", "function_profile"}
    gaokao_pool = []
    for qt, items in challenge_bank.items():
        if qt not in GAOKAO_QTYPES:
            continue
        for it in items:
            if str(it.get("source_kind", "")) == "exam":
                gaokao_pool.append(it)
    print(f"  GAOKAO 池真题挑战数 = {len(gaokao_pool)}")

    g3_fail: list[str] = []
    g4_fail: list[str] = []
    for it in gaokao_pool:
        cid = it.get("challenge_id")
        ak = answer_keys.get(cid)
        if not ak:
            g3_fail.append(f"missing answer_key: {cid}")
            continue
        if not ak.get("verification"):
            g3_fail.append(f"empty verification: {cid}")
        if ak.get("verification") != "manual_authored_v3":
            g4_fail.append(f"非人工撰写: {cid} → {ak.get('verification')}")
            continue
        explanation = ak.get("explanation", "") or ""
        if len(explanation) < 60:
            g4_fail.append(f"explanation 过短: {cid} ({len(explanation)} 字)")
        for opt in ak.get("option_analyses", []):
            if len(opt.get("analysis", "") or "") < 30:
                g4_fail.append(f"option 解析过短: {cid} {opt.get('label')}")
                break
    if g3_fail:
        failures.append(f"G3 GAOKAO 池有 {len(g3_fail)} 条无可信 answer_key（示例：{g3_fail[:3]}）")
    if g4_fail:
        failures.append(f"G4 真题人工解析未达标 {len(g4_fail)} 条（示例：{g4_fail[:3]}）")
    print(f"  G3 missing answer_keys = {len(g3_fail)}")
    print(f"  G4 v3 人工解析瑕疵 = {len(g4_fail)}")

    # ---- G6
    passages = _load_exam_passages()
    g6_fail: list[str] = []
    xuci_shape_fail = 0
    checked_focus_sentences = 0
    for it in gaokao_pool:
        paper_key = str((it.get("source_meta") or {}).get("paper_key") or it.get("article_id") or "")
        passage_sentences = passages.get(paper_key, [])
        passage_norm = [_normalize_for_match(s) for s in passage_sentences]
        context_norm = {_normalize_for_match(s) for s in (it.get("context_window") or [])}
        for option in it.get("options", []) or []:
            option_sentences = _option_sentences(option)
            if it.get("question_type") == "xuci_pair_compare" and len(option_sentences) < 2:
                xuci_shape_fail += 1
                if len(g6_fail) < 8:
                    g6_fail.append(f"{it.get('challenge_id')} {option.get('label')} 缺 pair sentences")
                continue
            for sentence in option_sentences:
                idx = _find_sentence_index(passage_norm, _normalize_for_match(sentence))
                if idx < 0:
                    continue
                checked_focus_sentences += 1
                start = max(0, idx - CONTEXT_WINDOW_RADIUS)
                end = min(len(passage_sentences), idx + CONTEXT_WINDOW_RADIUS + 1)
                for expected in passage_sentences[start:end]:
                    if _normalize_for_match(expected) not in context_norm:
                        if len(g6_fail) < 8:
                            g6_fail.append(f"{it.get('challenge_id')} {option.get('label')} context missing: {expected[:24]}")
                        break
    if g6_fail:
        failures.append(f"G6 真题 question-context 覆盖失败 {len(g6_fail)} 类问题（xuci shape={xuci_shape_fail}，示例：{g6_fail[:3]}）")
    print(f"  G6 真题 question-context 覆盖 = {not g6_fail}（核查 focus 句 {checked_focus_sentences} 条，xuci shape fail={xuci_shape_fail}）")

    # ---- G5
    textbook_bank = _read_sharded_dict("textbook_article_bank")
    g5_fail: list[str] = []
    article_without_content: list[str] = []
    total_textbook_items = 0
    total_content_items = 0
    for article_id, payload in textbook_bank.items():
        article_content_items = 0
        for it in payload.get("items", []):
            total_textbook_items += 1
            cid = str(it.get("challenge_id") or "")
            ak = answer_keys.get(cid) or {}
            support = list(ak.get("textbook_support") or [])[:1]
            support_item = support[0] if support else {}
            kind = str(it.get("kind") or ak.get("kind") or "")
            raw_label = str(support_item.get("label_text") or (it.get("source_meta") or {}).get("label_text") or "")
            label_compact = re.sub(r"[^\u4e00-\u9fff]", "", raw_label)
            headword_compact = re.sub(r"[^\u4e00-\u9fff]", "", str(support_item.get("headword") or "") or str(it.get("term_id") or "").split("::")[-1])
            focus = label_compact or headword_compact
            if not str(it.get("term_id") or ""):
                g5_fail.append(f"{article_id}::{cid} missing term_id")
                continue
            if kind == "content_word":
                total_content_items += 1
                article_content_items += 1
                if not focus or len(focus) > 2:
                    g5_fail.append(f"{article_id}::{cid} phrase/sentence target={raw_label!r}")
            elif kind == "function_word":
                if not headword_compact or len(headword_compact) > 2:
                    g5_fail.append(f"{article_id}::{cid} function headword={headword_compact!r}")
        if article_content_items == 0:
            article_without_content.append(str(article_id))
    if g5_fail:
        failures.append(f"G5 教材题有 {len(g5_fail)} 条非词级目标/缺 term_id，示例：{g5_fail[:3]}")
    if article_without_content:
        failures.append(f"G5 有 {len(article_without_content)} 篇无实词题，示例：{article_without_content[:3]}")
    print(
        f"  G5 教材题词级目标 = {not g5_fail}（教材题 {total_textbook_items} 道，实词题 {total_content_items} 道，"
        f"无实词篇目 {len(article_without_content)}）"
    )

    # ---- G7
    g7_fail: list[str] = []
    checked_textbook_options = 0
    for article_id, payload in textbook_bank.items():
        for it in payload.get("items", []):
            cid = str(it.get("challenge_id") or "")
            ak = answer_keys.get(cid) or {}
            correct_label = str(ak.get("correct_label") or "")
            options = list(it.get("options") or [])
            if len(options) != 4:
                g7_fail.append(f"{article_id}::{cid} option_count={len(options)}")
                continue
            for option in options:
                checked_textbook_options += 1
                label = str(option.get("label") or "")
                origin = str(option.get("origin") or "")
                if label == correct_label:
                    if origin != "textbook_note":
                        g7_fail.append(f"{article_id}::{cid} {label} correct origin={origin}")
                elif origin != "textbook_note_distractor":
                    g7_fail.append(f"{article_id}::{cid} {label} distractor origin={origin}")
                if len(g7_fail) >= 12:
                    break
            if len(g7_fail) >= 12:
                break
        if len(g7_fail) >= 12:
            break
    if g7_fail:
        failures.append(f"G7 教材题选项来源失败 {len(g7_fail)} 条示例：{g7_fail[:3]}")
    print(f"  G7 教材题选项来源 = {not g7_fail}（核查 option {checked_textbook_options} 个）")

    # ---- G8
    catalog = _read_sharded("textbook_article_catalog")
    catalog_by_id = {str(item.get("article_id") or ""): item for item in catalog}
    g8_fail: list[str] = []
    for article_id, payload in textbook_bank.items():
        items = list(payload.get("items") or [])
        meta = catalog_by_id.get(str(article_id))
        if not meta:
            g8_fail.append(f"{article_id} missing catalog")
            continue
        expected = {
            "challenge_count": len(items),
            "content_count": sum(1 for item in items if item.get("kind") == "content_word"),
            "function_count": sum(1 for item in items if item.get("kind") == "function_word"),
        }
        for key, value in expected.items():
            if int(meta.get(key) or 0) != value:
                g8_fail.append(f"{article_id} {key}: catalog={meta.get(key)} bank={value}")
                break
        if len(g8_fail) >= 8:
            break
    if len(catalog_by_id) != len(textbook_bank):
        g8_fail.append(f"catalog articles={len(catalog_by_id)} bank articles={len(textbook_bank)}")
    if g8_fail:
        failures.append(f"G8 教材目录计数不一致 {len(g8_fail)} 条示例：{g8_fail[:3]}")
    print(f"  G8 教材目录计数同步 = {not g8_fail}（catalog {len(catalog_by_id)} 篇）")

    # Summary
    if failures:
        print("\n[v3-audit] FAIL")
        for f in failures:
            print(f"  ✗ {f}")
        return 1
    print("\n[v3-audit] OK — 8 硬门全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
