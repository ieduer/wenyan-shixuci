#!/usr/bin/env python3
"""Rebuild context_window for gaokao mixed exam questions.

For each exam question (source_kind == "exam"), populate context_window so that
every option's term sentence appears together with up to 3 surrounding sentences
on either side from the original passage.

Outputs are written in place to:
- data/runtime/exam_questions.part2.json (challenge_bank items)

The original passages are pulled from data/runtime_private/exam_classical_corpus.md.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS_PATH = ROOT / "data" / "runtime_private" / "exam_classical_corpus.md"
MANUAL_DIR = ROOT / "data" / "manual" / "v3"
RUNTIME_DIR = ROOT / "data" / "runtime"
PUBLIC_RUNTIME_DIR = ROOT / "public" / "runtime"
PART2_NAME = "exam_questions.part2.json"
PART2_PATH = RUNTIME_DIR / PART2_NAME
MANIFEST_PATH = RUNTIME_DIR / "manifest.json"
PUBLIC_MANIFEST_PATH = PUBLIC_RUNTIME_DIR / "manifest.json"

WINDOW = 3  # sentences before/after each focus

PUNCT_STRIP_RE = re.compile(
    r"[，。：；！？、,.:;!?…．·\s　\"\"''《》〈〉「」『』（）()0-9​]+"
)
SENTENCE_SPLIT_RE = re.compile(r"[^。！？；…]+(?:[。！？；…]+|[\"」』])?")
ANNOTATION_RE = re.compile(r"\*\*[^*]+\*\*")


def load_passages() -> dict[str, str]:
    """Parse the corpus markdown and return {paper_key: passage_text}."""
    text = CORPUS_PATH.read_text(encoding="utf-8")
    papers: dict[str, str] = {}
    parts = text.split("\n## ")
    for part in parts[1:]:
        lines = part.split("\n")
        if len(lines) < 2:
            continue
        meta_line = lines[1].strip()
        m = re.match(r"^(\S+)\s*/\s*(\d+)\s*/\s*(\S+)\s*/\s*(\S+)$", meta_line)
        if not m:
            continue
        paper_key = m.group(4).strip()
        body = "\n".join(lines[2:]).strip()
        # Strip the prompt markers like "**阅读下面文言文,完成7--11题**"
        body = ANNOTATION_RE.sub("", body)
        papers[paper_key] = body.strip()
    return papers


def split_sentences(text: str) -> list[str]:
    """Split a passage into sentence-sized pieces preserving punctuation."""
    out: list[str] = []
    for raw in text.split("\n"):
        raw = raw.strip()
        if not raw:
            continue
        pieces = SENTENCE_SPLIT_RE.findall(raw)
        if not pieces:
            out.append(raw)
            continue
        for piece in pieces:
            piece = piece.strip()
            if piece:
                out.append(piece)
    return out


def normalize(text: str) -> str:
    return PUNCT_STRIP_RE.sub("", text or "")


def find_sentence_index(sentences_norm: list[str], target_norm: str) -> int:
    """Return the index of the sentence containing target_norm; -1 if missing.

    Tries exact substring match first, then falls back to the first sentence
    containing a 5+ character head substring (to tolerate OCR drift)."""
    if not target_norm:
        return -1
    for idx, sn in enumerate(sentences_norm):
        if target_norm in sn:
            return idx
    # Fallback head-prefix match
    if len(target_norm) >= 5:
        head = target_norm[:5]
        for idx, sn in enumerate(sentences_norm):
            if head in sn:
                return idx
    # Fallback longest-substring match (5+ chars)
    if len(target_norm) >= 6:
        for size in range(min(len(target_norm), 8), 4, -1):
            for start in range(0, len(target_norm) - size + 1):
                chunk = target_norm[start : start + size]
                for idx, sn in enumerate(sentences_norm):
                    if chunk in sn:
                        return idx
    return -1


def collect_option_sentences(question: dict) -> list[str]:
    """Pull every option-anchor sentence out of a question entry."""
    out: list[str] = []
    for opt in (question.get("options") or {}).values():
        sent = opt.get("sentence")
        if sent:
            out.append(sent)
        for s in opt.get("sentences", []) or []:
            if s:
                out.append(s)
    return out


def build_context_window(passage_sentences: list[str], option_sentences: list[str]) -> list[str]:
    """Return a deduplicated, passage-ordered list of context sentences for the options."""
    if not passage_sentences:
        return []
    sentences_norm = [normalize(s) for s in passage_sentences]
    picked: set[int] = set()
    for sent in option_sentences:
        idx = find_sentence_index(sentences_norm, normalize(sent))
        if idx < 0:
            continue
        start = max(0, idx - WINDOW)
        end = min(len(passage_sentences), idx + WINDOW + 1)
        for i in range(start, end):
            picked.add(i)
    return [passage_sentences[i] for i in sorted(picked)]


def index_solutions(passages: dict[str, str]) -> dict[str, list[str]]:
    """For each paper_key, return its sentence list, falling back to manual passage_excerpt."""
    out: dict[str, list[str]] = {}
    for pk, body in passages.items():
        sentences = split_sentences(body)
        if sentences:
            out[pk] = sentences
    # Fill any gaps from manual passage_excerpt
    for f in sorted(MANUAL_DIR.glob("exam_solutions_*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        meta = data.get("_meta", {})
        pk = meta.get("paper_key")
        if not pk:
            continue
        if pk not in out or not out.get(pk):
            excerpt = meta.get("passage_excerpt", "") or ""
            sentences = split_sentences(excerpt)
            if sentences:
                out[pk] = sentences
    return out


def index_questions() -> dict[tuple[str, str | None, int | None], list[str]]:
    """Map (paper_key, qsubtype, question_number) -> [option_sentences]."""
    mapping: dict[tuple[str, str | None, int | None], list[str]] = {}
    for f in sorted(MANUAL_DIR.glob("exam_solutions_*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        meta = data.get("_meta", {})
        pk = meta.get("paper_key")
        if not pk:
            continue
        for k, q in data.items():
            if k.startswith("_"):
                continue
            qsubtype = q.get("question_subtype")
            qnum = q.get("question_number")
            sub_idx = q.get("sub_index")
            sentences = collect_option_sentences(q)
            mapping[(pk, qsubtype, qnum, sub_idx)] = sentences
    return mapping


def update_part2(passages_by_paper: dict[str, list[str]]) -> tuple[int, int, list[str]]:
    """Patch context_window in exam_questions.part2.json. Returns (touched, missing, warnings)."""
    payload = json.loads(PART2_PATH.read_text(encoding="utf-8"))
    bank = payload.get("challenge_bank", {})
    touched = 0
    missing = 0
    warnings: list[str] = []
    for qtype, items in bank.items():
        for item in items:
            if str(item.get("source_kind") or "") != "exam":
                continue
            paper_key = (item.get("source_meta") or {}).get("paper_key") or item.get("article_id")
            if not paper_key:
                continue
            passage_sentences = passages_by_paper.get(paper_key)
            if not passage_sentences:
                missing += 1
                warnings.append(f"no_passage:{paper_key}")
                continue
            options = item.get("options") or []
            option_sentences: list[str] = []
            for opt in options:
                if opt.get("sentence"):
                    option_sentences.append(opt["sentence"])
                if isinstance(opt.get("sentences"), list):
                    for s in opt["sentences"]:
                        if s:
                            option_sentences.append(s)
            window = build_context_window(passage_sentences, option_sentences)
            if not window:
                # fall back: keep whatever existed (likely the manual passage_excerpt)
                continue
            item["context_window"] = window
            touched += 1
    PART2_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return touched, missing, warnings


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def sync_public_and_manifest() -> None:
    """Copy updated part2 to public/runtime and refresh the sha/size in manifest."""
    PUBLIC_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PART2_PATH, PUBLIC_RUNTIME_DIR / PART2_NAME)
    new_size = PART2_PATH.stat().st_size
    new_sha = _sha256(PART2_PATH)
    for manifest_path in (MANIFEST_PATH, PUBLIC_MANIFEST_PATH):
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        asset = manifest.get("assets", {}).get("exam_questions")
        if not asset:
            continue
        for shard in asset.get("shards", []):
            if shard.get("file_name") == PART2_NAME:
                shard["size_bytes"] = new_size
                shard["sha256"] = new_sha
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )


def main() -> None:
    raw_passages = load_passages()
    passages_by_paper = index_solutions(raw_passages)
    touched, missing, warnings = update_part2(passages_by_paper)
    print(f"updated context_window for {touched} exam items; {missing} skipped (no passage)")
    for w in warnings[:10]:
        print(" ", w)
    sync_public_and_manifest()
    print("synced public/runtime and refreshed manifest sha/size")


if __name__ == "__main__":
    main()
