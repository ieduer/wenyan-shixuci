#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any

from check_sources import MANIFEST_PATH, REPO_ROOT, load_json
from runtime_generation_v2 import (
    MINERU_OUTPUT_ROOT,
    best_context_window,
    build_textbook_answer_text,
    clean_text,
    clean_text_keep_newlines,
    derive_textbook_dict_headwords,
    extract_note_headword,
    locate_progressive_probe,
    infer_textbook_term_kind,
    locate_section_by_title,
    normalize_title,
    parse_note_entries,
    resolve_language_book_meta,
    resolve_language_book_paths,
    split_context_units_with_offsets,
    split_section_body_and_notes,
    summarize_note_gloss,
    title_part_variants,
)


PRIVATE_DIR = REPO_ROOT / "data" / "runtime_private"
DOCS_DIR = REPO_ROOT / "docs"
ARTICLE_JSON = PRIVATE_DIR / "textbook_article_master_table.json"
ARTICLE_CSV = PRIVATE_DIR / "textbook_article_master_table.csv"
NOTE_JSON = PRIVATE_DIR / "textbook_note_master_table.json"
NOTE_CSV = PRIVATE_DIR / "textbook_note_master_table.csv"
NOTE_UNRESOLVED_JSON = PRIVATE_DIR / "textbook_note_unresolved_table.json"
NOTE_UNRESOLVED_CSV = PRIVATE_DIR / "textbook_note_unresolved_table.csv"
AUDIT_JSON = DOCS_DIR / "TEXTBOOK_SOURCE_AUDIT.json"
AUDIT_MD = DOCS_DIR / "TEXTBOOK_SOURCE_AUDIT.md"
STRUCTURED_TEXT_ROOT = REPO_ROOT.parent / "jks" / "_legacy" / "yuwen" / "public" / "data"
STRUCTURED_TEXT_FILES = ["1.json", "2.json", "3.json", "4.json", "5.json", "all.json"]

NOTE_MARK_RE = re.compile(r"\\textcircled\s*\{\s*([^}]+?)\s*\}")
CIRCLED_INLINE_RE = re.compile(r"[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳@]")
NON_HAN_RE = re.compile(r"[^\u4e00-\u9fff]+")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[。！？；!?\n])")
NOTE_BLOCK_RE = re.compile(r"\\textcircled\s*\{|[〔［\[][^〕］\]]+[〕］\]]")
PAGE_FOOTER_RE = re.compile(r"^\s*\d+\s*语文")
TEXTCIRCLED_TOKEN_RE = re.compile(r"(?<!\n)(\\textcircled\s*\{[^}]+\})")
PAREN_NOTE_RE = re.compile(r"[（(][^（）()]{1,20}[)）]")

TITLE_ALIASES = {
    "芣苢": ["荣苣", "茉苣"],
}

CORRECTED_TITLE_ALIASES = {
    "离骚（节选）": ["离骚"],
    "离骚(节选)": ["离骚"],
    "子路、曾皙、冉有、公西华侍坐": ["子路、曾晳、冉有、公西华侍坐"],
    "归去来兮辞并序": ["归去来兮辞(并序)"],
    "琵琶行并序": ["琵琶行(并序)"],
    "《老子》四章": ["《老子》八章"],
    "人皆有不忍人之心": ["《孟子》一则"],
    "五石之瓠": ["逍遥游"],
}

SECTION_START_OVERRIDES = {
    "齐桓晋文之事": ["《孟子》\n\n齐宣王问曰"],
}

BOUNDARY_TITLES = {
    "学习提示",
    "思考探究",
    "积累拓展",
    "单元学习任务",
    "单元研习任务",
    "写作",
    "口语交际",
    "综合性学习",
}


def normalize_marker_token(value: str) -> str:
    text = clean_text(str(value or ""))
    text = re.sub(r"\s+", "", text)
    mapping = {
        "@": "@",
        "①": "1",
        "②": "2",
        "③": "3",
        "④": "4",
        "⑤": "5",
        "⑥": "6",
        "⑦": "7",
        "⑧": "8",
        "⑨": "9",
        "⑩": "10",
        "⑪": "11",
        "⑫": "12",
        "⑬": "13",
        "⑭": "14",
        "⑮": "15",
        "⑯": "16",
        "⑰": "17",
        "⑱": "18",
        "⑲": "19",
        "⑳": "20",
    }
    if text in mapping:
        return mapping[text]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or text


def normalize_block_title(text: str) -> str:
    value = clean_text(text)
    value = NOTE_MARK_RE.sub("", value)
    value = re.sub(r"[0-9①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳@]", "", value)
    return normalize_title(value)


def article_title_variants(title: str) -> list[str]:
    raw_variants = list(title_part_variants(title))
    expanded: list[str] = []
    for raw in raw_variants:
        expanded.append(raw)
        expanded.extend(TITLE_ALIASES.get(clean_text(raw), []))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in expanded:
        normalized = normalize_title(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(clean_text(item))
    return deduped


def normalize_body_source_title_key(title: str) -> str:
    cleaned = clean_text(str(title or ""))
    cleaned = cleaned.replace("曾皙", "曾晳")
    cleaned = cleaned.replace("（", "(").replace("）", ")")
    cleaned = cleaned.replace("《", "").replace("》", "")
    cleaned = cleaned.replace("【", "").replace("】", "")
    cleaned = cleaned.replace("*", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    return normalize_title(cleaned)


def corrected_title_variants(title: str) -> list[str]:
    expanded: list[str] = []
    for raw in article_title_variants(title):
        expanded.append(raw)
        expanded.extend(CORRECTED_TITLE_ALIASES.get(clean_text(raw), []))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in expanded:
        cleaned = clean_text(item)
        if not cleaned:
            continue
        normalized = normalize_body_source_title_key(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(cleaned)
    return deduped


def expand_manifest_items(manifest_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for item in manifest_items:
        title = clean_text(str(item.get("title") or ""))
        if not title:
            continue
        parts = [clean_text(part) for part in re.split(r"/|／", title) if clean_text(part)]
        if len(parts) <= 1:
            expanded.append(
                {
                    "manifest_title": title,
                    "title": title,
                    "kind": clean_text(str(item.get("kind") or "")),
                    "page_start": item.get("page_start"),
                    "page_end": item.get("page_end"),
                }
            )
            continue
        for index, part in enumerate(parts, start=1):
            expanded.append(
                {
                    "manifest_title": title,
                    "title": part,
                    "part_index": index,
                    "kind": clean_text(str(item.get("kind") or "")),
                    "page_start": item.get("page_start"),
                    "page_end": item.get("page_end"),
                }
            )
    return expanded


def block_text(block: dict[str, Any]) -> str:
    lines: list[str] = []
    for line in block.get("lines", []):
        spans: list[str] = []
        for span in line.get("spans", []):
            content = span.get("content", span.get("text", ""))
            if content:
                spans.append(str(content))
        if spans:
            lines.append("".join(spans))
    return clean_text_keep_newlines("\n".join(lines))


def normalized_han_text(text: str) -> str:
    return "".join(NON_HAN_RE.sub("", clean_text_keep_newlines(text)).split())


def sanitize_note_probe_text(text: str) -> str:
    value = clean_text(str(text or ""))
    value = PAREN_NOTE_RE.sub("", value)
    value = value.replace("“", "").replace("”", "").replace("‘", "").replace("’", "")
    value = value.replace("〔", "").replace("〕", "").replace("[", "").replace("]", "")
    value = re.sub(r"\s+", "", value)
    return value


def sanitize_note_text_value(text: str) -> str:
    value = clean_text(str(text or ""))
    if not value:
        return ""
    clauses = [clean_text(part) for part in re.split(r"[。；;]", value) if clean_text(part)]
    if len(clauses) >= 2 and len(clauses[-1]) <= 2 and len(clauses[0]) >= 2:
        return clean_text("。".join(clauses[:-1])) + "。"
    return value


def is_noise_text(text: str) -> bool:
    value = clean_text_keep_newlines(text)
    normalized = normalize_title(value)
    if not value:
        return True
    if normalized in {"人民都育出版社", "人民教育出版社", "学习活动"}:
        return True
    if value.startswith(("人民都育出版社", "人民教育出版社", "仅供个人学习使用")):
        return True
    if PAGE_FOOTER_RE.match(value):
        return True
    return False


def is_note_block_text(text: str) -> bool:
    value = clean_text_keep_newlines(text)
    if not value:
        return False
    return bool(NOTE_BLOCK_RE.search(value))


def sentence_candidates(text: str) -> list[str]:
    parts = [clean_text_keep_newlines(part).strip() for part in SENTENCE_SPLIT_RE.split(text) if clean_text(part)]
    return [part for part in parts if part]


def choose_sentence(block_text_value: str, label_text: str, headword: str) -> str:
    label = clean_text(label_text)
    head = clean_text(headword)
    candidates = sentence_candidates(block_text_value) or [clean_text_keep_newlines(block_text_value)]
    for candidate in candidates:
        if label and label in candidate:
            return clean_text(candidate)
    for candidate in candidates:
        if head and head in candidate:
            return clean_text(candidate)
    return clean_text(candidates[0]) if candidates else ""


def context_window_from_blocks(blocks: list[dict[str, Any]], current_index: int) -> list[str]:
    start = max(0, current_index - 3)
    end = min(len(blocks), current_index + 4)
    return [clean_text(str(blocks[idx]["text"])) for idx in range(start, end) if clean_text(str(blocks[idx]["text"]))]


def clean_structured_main_text(main_text: str, markers: list[str]) -> str:
    value = clean_text_keep_newlines(main_text)
    for marker in sorted({clean_text(str(item)) for item in markers if clean_text(str(item))}, key=len, reverse=True):
        value = value.replace(marker, "")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return clean_text_keep_newlines(value).strip()


def load_corrected_poems() -> tuple[dict[str, dict[str, Any]], Path | None]:
    if not STRUCTURED_TEXT_ROOT.exists():
        return {}, None

    lookup: dict[str, dict[str, Any]] = {}
    for filename in STRUCTURED_TEXT_FILES:
        source_path = STRUCTURED_TEXT_ROOT / filename
        if not source_path.exists():
            continue
        payload = load_json(source_path)
        stack: list[Any] = [payload]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                title = clean_text(str(node.get("title") or ""))
                main_text = clean_text_keep_newlines(str(node.get("main_text") or ""))
                if title and main_text and "..." not in main_text:
                    markers = [clean_text(str(item.get("marker") or "")) for item in list(node.get("footnotes") or []) if isinstance(item, dict)]
                    full_text = clean_structured_main_text(main_text, markers)
                    if full_text:
                        entry = {
                            "source_mode": "legacy_structured_text",
                            "source_path": str(source_path),
                            "source_title": title,
                            "author": clean_text(str(node.get("author") or "")),
                            "full_text": full_text,
                        }
                        for variant in corrected_title_variants(title):
                            key = normalize_body_source_title_key(variant)
                            if key and key not in lookup:
                                lookup[key] = entry
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
    return lookup, STRUCTURED_TEXT_ROOT


def resolve_corrected_poem(title: str, lookup: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    for variant in corrected_title_variants(title):
        key = normalize_body_source_title_key(variant)
        if key and key in lookup:
            return lookup[key]
    return None


def corrected_entry_covers_article(
    corrected_entry: dict[str, Any] | None,
    ocr_body_text: str,
    parsed_notes: list[dict[str, Any]],
) -> bool:
    if not corrected_entry:
        return False
    corrected_norm = normalized_han_text(str(corrected_entry.get("full_text") or ""))
    if not corrected_norm:
        return False

    probes: list[str] = []
    for sentence in sentence_candidates(ocr_body_text):
        probe = normalized_han_text(sentence)
        if len(probe) >= 8:
            probes.append(probe)
            break
    for note in parsed_notes[:6]:
        label_text = sanitize_note_probe_text(note.get("label_text") or "")
        note_text = clean_text(str(note.get("note_text") or ""))
        headword = sanitize_note_probe_text(extract_note_headword(label_text, note_text))
        candidate = normalized_han_text(label_text)
        if len(candidate) < 2:
            candidate = normalized_han_text(headword)
        if len(candidate) >= 2:
            probes.append(candidate)

    unique_probes: list[str] = []
    seen: set[str] = set()
    for probe in probes:
        if probe and probe not in seen:
            seen.add(probe)
            unique_probes.append(probe)
    if not unique_probes:
        return False

    hits = sum(1 for probe in unique_probes if probe in corrected_norm)
    threshold = 1 if len(unique_probes) == 1 else max(2, min(4, (len(unique_probes) + 1) // 2))
    return hits >= threshold


def note_hits_source_text(note: dict[str, Any], source_text: str) -> bool:
    label_text = sanitize_note_probe_text(note.get("label_text") or "")
    note_text = clean_text(str(note.get("note_text") or ""))
    headword = sanitize_note_probe_text(extract_note_headword(label_text, note_text))
    _, position = locate_progressive_probe(source_text, label_text, headword, 0)
    return position >= 0


def trim_note_candidates(parsed_notes: list[dict[str, Any]], source_text: str) -> list[dict[str, Any]]:
    if len(parsed_notes) <= 3 or not clean_text(source_text):
        return parsed_notes
    flags = [note_hits_source_text(note, source_text) for note in parsed_notes]
    if sum(flags) < 2:
        return parsed_notes

    segments: list[tuple[int, int, int]] = []
    current_start = -1
    current_hits = 0
    unmatched_run = 0
    gap_threshold = 5

    for index, matched in enumerate(flags):
        if matched:
            if current_start < 0:
                current_start = index
                current_hits = 0
            current_hits += 1
            unmatched_run = 0
            continue
        if current_start < 0:
            continue
        unmatched_run += 1
        if unmatched_run >= gap_threshold:
            end = index - unmatched_run
            if end >= current_start:
                segments.append((current_start, end, current_hits))
            current_start = -1
            current_hits = 0
            unmatched_run = 0

    if current_start >= 0:
        segments.append((current_start, len(parsed_notes) - 1, current_hits))

    if not segments:
        return parsed_notes

    best_start, best_end, _ = max(segments, key=lambda item: (item[2], item[1] - item[0]))
    if best_start == 0 and best_end == len(parsed_notes) - 1:
        return parsed_notes
    return parsed_notes[best_start : best_end + 1]


def extract_author_and_body_text(title: str, body_blocks: list[dict[str, Any]]) -> tuple[str, str]:
    title_norms = {normalize_title(item) for item in article_title_variants(title)}
    author = ""
    content: list[str] = []
    skipped_title = False
    for block in body_blocks:
        text = clean_text_keep_newlines(str(block["text"]))
        normalized = normalize_block_title(text)
        if not skipped_title and normalized and normalized in title_norms:
            skipped_title = True
            continue
        compact = clean_text(text)
        if (
            not author
            and block["type"] == "text"
            and compact
            and len(compact) <= 12
            and not re.search(r"[，。！？；：、“”\"'（）()]", compact)
        ):
            author = compact
            continue
        content.append(text)
    return author, clean_text_keep_newlines("\n".join(content)).strip()


def build_note_lines(note_blocks: list[dict[str, Any]]) -> list[str]:
    merged: list[str] = []
    for block in note_blocks:
        text = clean_text_keep_newlines(str(block["text"]))
        if not text:
            continue
        text = TEXTCIRCLED_TOKEN_RE.sub(r"\n$\1$", text)
        merged.append(text)
    lines: list[str] = []
    for raw_line in "\n".join(merged).splitlines():
        line = raw_line.strip()
        if not clean_text(line):
            continue
        line = re.sub(r"^(\\textcircled\s*\{[^}]+\})", r"$\1$", line)
        lines.append(line)
    return lines


def build_page_blocks(middle_path: Path) -> list[dict[str, Any]]:
    payload = load_json(middle_path)
    rows: list[dict[str, Any]] = []
    for page in payload.get("pdf_info", []):
        if not isinstance(page, dict):
            continue
        para_blocks = list(page.get("para_blocks") or [])
        for block_index, block in enumerate(para_blocks):
            text = block_text(block)
            if not text:
                continue
            zone = "note" if is_note_block_text(text) else "body"
            rows.append(
                {
                    "page_idx": int(page.get("page_idx") or 0),
                    "block_index": block_index,
                    "type": str(block.get("type") or ""),
                    "zone": zone,
                    "bbox": block.get("bbox"),
                    "text": text,
                    "title_norm": normalize_block_title(text),
                    "han_norm": normalized_han_text(text),
                    "is_noise": is_noise_text(text),
                }
            )
    return rows


def find_heading_positions(book_blocks: list[dict[str, Any]], manifest_titles: list[str]) -> dict[str, dict[str, Any]]:
    title_blocks = [
        block
        for block in book_blocks
        if block["zone"] == "body" and block["type"] == "title" and block["title_norm"] and not block["is_noise"]
    ]
    results: dict[str, dict[str, Any]] = {}
    cursor: tuple[int, int] = (-1, -1)
    for title in manifest_titles:
        variants = [normalize_title(item) for item in article_title_variants(title) if normalize_title(item)]
        candidates: list[dict[str, Any]] = []
        for block in title_blocks:
            position = (int(block["page_idx"]), int(block["block_index"]))
            if position <= cursor:
                continue
            block_norm = str(block["title_norm"])
            if any(variant and (variant == block_norm or variant in block_norm or block_norm in variant) for variant in variants):
                candidates.append(block)
        if candidates:
            chosen = sorted(candidates, key=lambda item: (int(item["page_idx"]), int(item["block_index"])))[0]
            results[title] = chosen
            cursor = (int(chosen["page_idx"]), int(chosen["block_index"]))
    return results


def extract_section_by_variants(raw_md: str, title: str, all_titles: set[str]) -> str:
    alias_targets = {
        normalize_title(item)
        for other_title in all_titles
        for item in article_title_variants(other_title)
        if normalize_title(item)
    }
    for variant in article_title_variants(title):
        section_text = locate_section_by_title(raw_md, variant, alias_targets)
        if section_text:
            return section_text
    for marker in SECTION_START_OVERRIDES.get(title, []):
        start = raw_md.find(marker)
        if start < 0:
            continue
        tail = raw_md[start:]
        heading_matches = list(re.finditer(r"(?m)^#\s*(.+?)\s*$", tail))
        end = len(tail)
        for match in heading_matches:
            heading = clean_text(match.group(1))
            if heading in {"学习提示", "思考探究", "积累拓展", "单元学习任务", "单元研习任务"}:
                end = match.start()
                break
        return tail[:end]
    return ""


def build_section_text_map(raw_md: str, manifest_titles: list[str]) -> dict[str, dict[str, Any]]:
    normalized_targets = {
        normalize_title(item)
        for title in manifest_titles
        for item in article_title_variants(title)
        if normalize_title(item)
    }
    result: dict[str, dict[str, Any]] = {}
    for title in manifest_titles:
        section_text = extract_section_by_variants(raw_md, title, normalized_targets)
        if not section_text:
            continue
        body_text, note_lines = split_section_body_and_notes(section_text)
        parsed_notes = parse_note_entries(note_lines)
        result[title] = {
            "section_text": section_text,
            "body_text": body_text,
            "parsed_notes": parsed_notes,
        }
    return result


def block_position(block: dict[str, Any]) -> tuple[int, int]:
    return int(block["page_idx"]), int(block["block_index"])


def in_span(position: tuple[int, int], start: tuple[int, int], end: tuple[int, int] | None) -> bool:
    if position < start:
        return False
    if end is not None and position >= end:
        return False
    return True


def compute_page_offset(
    article_specs: list[dict[str, Any]],
    heading_positions: dict[str, dict[str, Any]],
) -> int | None:
    offsets: list[int] = []
    for spec in article_specs:
        page_start = spec.get("page_start")
        if not isinstance(page_start, int):
            continue
        heading = heading_positions.get(str(spec["title"]))
        if not heading:
            continue
        offsets.append(int(heading["page_idx"]) - page_start)
    if not offsets:
        return None
    return int(round(median(offsets)))


def find_fallback_position(
    title: str,
    article_spec: dict[str, Any],
    section: dict[str, Any] | None,
    book_blocks: list[dict[str, Any]],
    page_offset: int | None,
) -> dict[str, Any] | None:
    page_start = article_spec.get("page_start")
    page_end = article_spec.get("page_end")
    candidates = [
        block
        for block in book_blocks
        if block["zone"] == "body"
        and not block["is_noise"]
        and (page_offset is None or not isinstance(page_start, int) or int(block["page_idx"]) >= page_start + page_offset)
        and (page_offset is None or not isinstance(page_end, int) or int(block["page_idx"]) <= page_end + page_offset)
    ]
    if not candidates:
        return None

    search_units: list[str] = []
    if section:
        search_units.extend(sentence_candidates(str(section.get("body_text") or ""))[:3])
    for unit in search_units:
        needle = normalized_han_text(unit)
        if not needle:
            continue
        for block in candidates:
            if needle and needle in str(block["han_norm"]):
                return block
    return sorted(candidates, key=block_position)[0]


def find_article_positions(
    book_blocks: list[dict[str, Any]],
    article_specs: list[dict[str, Any]],
    section_map: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], int | None]:
    titles = [str(spec["title"]) for spec in article_specs]
    positions = find_heading_positions(book_blocks, titles)
    page_offset = compute_page_offset(article_specs, positions)
    for spec in article_specs:
        title = str(spec["title"])
        if title in positions:
            continue
        fallback = find_fallback_position(title, spec, section_map.get(title), book_blocks, page_offset)
        if fallback:
            positions[title] = fallback
    return positions, page_offset


def compute_article_end(
    book_blocks: list[dict[str, Any]],
    start: tuple[int, int],
    item: dict[str, Any],
    next_heading: dict[str, Any] | None,
    page_offset: int | None,
) -> tuple[int, int] | None:
    candidates: list[tuple[int, int]] = []
    if next_heading:
        candidates.append(block_position(next_heading))
    page_end = item.get("page_end")
    if isinstance(page_end, int) and page_offset is not None:
        candidates.append((page_end + page_offset + 1, 0))
    for block in book_blocks:
        position = block_position(block)
        if position <= start:
            continue
        if block["zone"] != "body" or block["type"] != "title":
            continue
        if clean_text(str(block["text"])) in BOUNDARY_TITLES:
            candidates.append(position)
            break
    if not candidates:
        return None
    return min(candidates)


def map_note_to_block(
    note: dict[str, Any],
    body_blocks: list[dict[str, Any]],
    start_index: int,
    start_offset: int,
) -> dict[str, Any]:
    label_text = clean_text(str(note.get("label_text") or ""))
    headword = clean_text(str(note.get("headword") or ""))
    label_norm = normalized_han_text(label_text)
    head_norm = normalized_han_text(headword)

    best: dict[str, Any] | None = None
    for idx in range(start_index, len(body_blocks)):
        block = body_blocks[idx]
        block_norm = str(block["han_norm"])
        offset = start_offset if idx == start_index else 0
        match_pos = -1
        mode = ""
        if label_norm:
            match_pos = block_norm.find(label_norm, offset)
            if match_pos >= 0:
                mode = "exact_label"
        if match_pos < 0 and head_norm:
            match_pos = block_norm.find(head_norm, offset)
            if match_pos >= 0:
                mode = "headword_fallback"
        if match_pos >= 0:
            best = {
                "block_idx": idx,
                "offset": match_pos,
                "mode": mode,
                "block": block,
            }
            break

    if not best:
        return {
            "status": "unresolved",
            "match_mode": "unresolved",
            "match_confidence": 0.0,
            "source_page_idx": None,
            "source_block_index": None,
            "source_sentence": "",
            "context_window": [],
            "next_block_idx": start_index,
            "next_offset": start_offset,
        }

    matched_block = best["block"]
    sentence = choose_sentence(str(matched_block["text"]), label_text, headword)
    return {
        "status": "matched",
        "match_mode": best["mode"],
        "match_confidence": 1.0 if best["mode"] == "exact_label" else 0.65,
        "source_page_idx": int(matched_block["page_idx"]),
        "source_block_index": int(matched_block["block_index"]),
        "source_sentence": sentence,
        "context_window": context_window_from_blocks(body_blocks, int(best["block_idx"])),
        "next_block_idx": int(best["block_idx"]),
        "next_offset": int(best["offset"]),
    }


def text_context_from_position(source_text: str, position: int, label_text: str, headword: str) -> tuple[str, list[str], int]:
    units = split_context_units_with_offsets(source_text)
    if not units:
        cleaned_label = clean_text(label_text) or clean_text(headword)
        return cleaned_label, ([cleaned_label] if cleaned_label else []), 0

    target_index = 0
    for index, unit in enumerate(units):
        start = int(unit["start"])
        end = int(unit["end"])
        if start <= position < end:
            target_index = index
            break
        if position >= end:
            target_index = index
    start_index = max(0, target_index - 3)
    end_index = min(len(units), target_index + 4)
    window = [clean_text(str(unit["text"])) for unit in units[start_index:end_index] if clean_text(str(unit["text"]))]
    sentence = clean_text(str(units[target_index]["text"]))
    label = clean_text(label_text)
    head = clean_text(headword)
    for unit in window:
        if label and label in unit:
            sentence = unit
            break
    else:
        for unit in window:
            if head and head in unit:
                sentence = unit
                break
    focus_index = next((idx for idx, item in enumerate(window) if item == sentence), 0)
    return sentence, window, focus_index


def map_note_to_text(
    note: dict[str, Any],
    source_text: str,
    start_index: int,
    fallback_probe_text: str = "",
) -> dict[str, Any]:
    label_text = sanitize_note_probe_text(note.get("label_text") or "")
    headword = sanitize_note_probe_text(note.get("headword") or "")
    probe_text, position = locate_progressive_probe(source_text, label_text, headword, start_index)
    if position < 0 and fallback_probe_text:
        fallback_window = [clean_text(item) for item in best_context_window(source_text, fallback_probe_text, headword) if clean_text(item)]
        if fallback_window:
            label_norm = normalized_han_text(label_text)
            head_norm = normalized_han_text(headword)
            probe_norm = normalized_han_text(sanitize_note_probe_text(fallback_probe_text))
            probe_chunks = {probe_norm[idx : idx + 2] for idx in range(max(0, len(probe_norm) - 1)) if len(probe_norm[idx : idx + 2]) == 2}
            best_sentence = fallback_window[min(len(fallback_window) // 2, len(fallback_window) - 1)]
            best_score = -1
            for item in fallback_window:
                item_norm = normalized_han_text(item)
                score = 0
                if label_norm and label_norm in item_norm:
                    score += 8
                if head_norm and head_norm in item_norm:
                    score += 5
                if probe_norm and probe_norm in item_norm:
                    score += 6
                score += sum(1 for chunk in probe_chunks if chunk and chunk in item_norm)
                if score > best_score:
                    best_score = score
                    best_sentence = item
            sentence = best_sentence
            return {
                "status": "matched",
                "match_mode": "fallback_probe",
                "match_confidence": 0.72,
                "source_sentence": sentence,
                "context_window": fallback_window[:7],
                "context_focus_index": next((idx for idx, item in enumerate(fallback_window) if item == sentence), 0),
                "next_start": start_index,
            }
    if position < 0:
        return {
            "status": "unresolved",
            "match_mode": "unresolved",
            "match_confidence": 0.0,
            "source_sentence": "",
            "context_window": [],
            "context_focus_index": 0,
            "next_start": start_index,
        }

    sentence, context_window, focus_index = text_context_from_position(source_text, position, label_text, headword)
    return {
        "status": "matched",
        "match_mode": "direct_probe",
        "match_confidence": 1.0 if label_text and label_text in sentence else 0.82,
        "source_sentence": sentence,
        "context_window": context_window,
        "context_focus_index": focus_index,
        "next_start": max(start_index, position + max(len(probe_text), 1)),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row)
            for key, value in list(payload.items()):
                if isinstance(value, list):
                    payload[key] = "｜".join(clean_text(str(item)) for item in value if clean_text(str(item)))
            writer.writerow(payload)


def build_tables() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    manifest = load_json(MANIFEST_PATH)
    book_meta = resolve_language_book_meta()
    book_paths = resolve_language_book_paths(book_meta)
    corrected_lookup, corrected_source_path = load_corrected_poems()

    article_rows: list[dict[str, Any]] = []
    note_rows: list[dict[str, Any]] = []
    unresolved_note_rows: list[dict[str, Any]] = []
    per_book_stats: list[dict[str, Any]] = []
    body_source_counter: Counter[str] = Counter()

    for book_key in sorted(key for key in manifest if key.startswith("高中_语文_")):
        md_path = book_paths.get(book_key)
        if not md_path or not md_path.exists():
            continue
        middle_path = next((path for path in md_path.parent.glob("*_middle.json")), None)
        if not middle_path or not middle_path.exists():
            continue

        raw_md = md_path.read_text(encoding="utf-8")
        book_title = book_meta.get(book_key, {}).get("display_title") or book_meta.get(book_key, {}).get("title") or book_key
        manifest_items = [item for item in manifest.get(book_key, []) if clean_text(str(item.get("title") or ""))]
        article_specs = expand_manifest_items(manifest_items)
        article_titles = [clean_text(str(item.get("title") or "")) for item in article_specs]
        section_map = build_section_text_map(raw_md, article_titles)
        page_blocks = build_page_blocks(middle_path)
        article_positions, page_offset = find_article_positions(page_blocks, article_specs, section_map)

        book_article_count = 0
        book_note_count = 0
        unresolved_count = 0
        corrected_article_count = 0

        for idx, item in enumerate(article_specs):
            title = clean_text(str(item.get("title") or ""))
            section = section_map.get(title)
            heading = article_positions.get(title)
            next_heading = None
            for next_item in article_specs[idx + 1 :]:
                next_heading = article_positions.get(clean_text(str(next_item.get("title") or "")))
                if next_heading:
                    break

            if not section or not heading:
                continue

            start = block_position(heading)
            end = compute_article_end(page_blocks, start, item, next_heading, page_offset)
            body_blocks = [
                block
                for block in page_blocks
                if block["zone"] == "body"
                and block["type"] in {"text", "title", "list"}
                and not block["is_noise"]
                and in_span(block_position(block), start, end)
            ]
            note_blocks = [
                block
                for block in page_blocks
                if block["zone"] == "note"
                and not block["is_noise"]
                and in_span(block_position(block), start, end)
            ]
            ocr_author, ocr_body_text = extract_author_and_body_text(title, body_blocks)
            parsed_notes = [
                {
                    "note_order": note_order,
                    **parsed_note,
                }
                for note_order, parsed_note in enumerate(parse_note_entries(build_note_lines(note_blocks)), start=1)
            ]
            parsed_notes = trim_note_candidates(parsed_notes, ocr_body_text)
            corrected_entry = resolve_corrected_poem(title, corrected_lookup)
            if corrected_entry and not corrected_entry_covers_article(corrected_entry, ocr_body_text, parsed_notes):
                corrected_entry = None
            body_text = clean_text_keep_newlines(str((corrected_entry or {}).get("full_text") or ocr_body_text)).strip()
            author = clean_text(str((corrected_entry or {}).get("author") or ocr_author))
            body_source_mode = str((corrected_entry or {}).get("source_mode") or "mineru_md")
            body_source_title = clean_text(str((corrected_entry or {}).get("source_title") or title))
            body_source_path = str((corrected_entry or {}).get("source_path") or md_path)

            end_page_idx = int(body_blocks[-1]["page_idx"]) if body_blocks else int(heading["page_idx"])
            accepted_note_count = 0
            body_source_counter[body_source_mode] += 1
            if corrected_entry:
                corrected_article_count += 1
            article_rows.append(
                {
                    "book_key": book_key,
                    "book_title": book_title,
                    "title": title,
                    "manifest_title": clean_text(str(item.get("manifest_title") or title)),
                    "kind": clean_text(str(item.get("kind") or "")),
                    "page_start_print": item.get("page_start"),
                    "page_end_print": item.get("page_end"),
                    "start_page_idx": int(heading["page_idx"]),
                    "end_page_idx": end_page_idx,
                    "md_path": str(md_path),
                    "middle_path": str(middle_path),
                    "article_start_mode": "heading" if heading.get("type") == "title" else "fallback",
                    "body_block_count": len(body_blocks),
                    "note_block_count": len(note_blocks),
                    "note_candidate_count": len(parsed_notes),
                    "note_count": 0,
                    "body_source_mode": body_source_mode,
                    "body_source_title": body_source_title,
                    "body_source_path": body_source_path,
                    "author": author,
                    "ocr_author": ocr_author,
                    "ocr_full_text": ocr_body_text,
                    "full_text": body_text,
                }
            )
            book_article_count += 1

            layout_pointer_idx = 0
            layout_pointer_offset = 0
            text_pointer = 0
            for parsed_note in parsed_notes:
                note_order = int(parsed_note.get("note_order") or 0) or 1
                label_text = clean_text(str(parsed_note.get("label_text") or ""))
                note_text = sanitize_note_text_value(str(parsed_note.get("note_text") or ""))
                headword = extract_note_headword(label_text, note_text)
                gloss = summarize_note_gloss(headword, note_text)
                answer_text = build_textbook_answer_text(label_text, note_text, gloss)
                term_kind = infer_textbook_term_kind(headword, label_text, note_text, gloss)
                layout_mapping = map_note_to_block(
                    {"label_text": label_text, "headword": headword},
                    body_blocks,
                    layout_pointer_idx,
                    layout_pointer_offset,
                )
                text_mapping = map_note_to_text(
                    {"label_text": label_text, "headword": headword},
                    body_text,
                    text_pointer,
                    fallback_probe_text=str(layout_mapping.get("source_sentence") or ""),
                )

                effective_mapping = (
                    text_mapping
                    if corrected_entry
                    else (layout_mapping if layout_mapping["status"] == "matched" else text_mapping)
                )
                if effective_mapping["status"] != "matched":
                    unresolved_count += 1
                    unresolved_note_rows.append(
                        {
                            "book_key": book_key,
                            "book_title": book_title,
                            "title": title,
                            "manifest_title": clean_text(str(item.get("manifest_title") or title)),
                            "page_start_print": item.get("page_start"),
                            "page_end_print": item.get("page_end"),
                            "note_order": note_order,
                            "kind": term_kind,
                            "headword": headword,
                            "label_text": label_text,
                            "gloss": gloss,
                            "answer_text": answer_text,
                            "note_text": note_text,
                            "dict_headwords": derive_textbook_dict_headwords({"headword": headword, "label_text": label_text}, headword),
                            "body_source_mode": body_source_mode,
                            "body_source_title": body_source_title,
                            "body_source_path": body_source_path,
                            "match_status": effective_mapping["status"],
                            "match_mode": effective_mapping["match_mode"],
                            "match_confidence": effective_mapping["match_confidence"],
                            "source_page_idx": layout_mapping["source_page_idx"],
                            "source_block_index": layout_mapping["source_block_index"],
                            "source_sentence": effective_mapping["source_sentence"],
                            "context_window": effective_mapping["context_window"],
                            "context_focus_index": effective_mapping.get("context_focus_index", 0),
                            "layout_match_status": layout_mapping["status"],
                            "layout_match_mode": layout_mapping["match_mode"],
                            "layout_match_confidence": layout_mapping["match_confidence"],
                            "layout_source_sentence": layout_mapping["source_sentence"],
                            "layout_context_window": layout_mapping["context_window"],
                            "text_match_status": text_mapping["status"],
                            "text_match_mode": text_mapping["match_mode"],
                            "text_match_confidence": text_mapping["match_confidence"],
                            "text_source_sentence": text_mapping["source_sentence"],
                            "text_context_window": text_mapping["context_window"],
                            "md_path": str(md_path),
                            "middle_path": str(middle_path),
                        }
                    )
                    continue
                if layout_mapping["status"] == "matched":
                    layout_pointer_idx = int(layout_mapping["next_block_idx"])
                    layout_pointer_offset = int(layout_mapping["next_offset"])
                if corrected_entry and text_mapping["status"] == "matched":
                    text_pointer = int(text_mapping["next_start"])
                note_rows.append(
                    {
                        "book_key": book_key,
                        "book_title": book_title,
                        "title": title,
                        "manifest_title": clean_text(str(item.get("manifest_title") or title)),
                        "page_start_print": item.get("page_start"),
                        "page_end_print": item.get("page_end"),
                        "note_order": note_order,
                        "kind": term_kind,
                        "headword": headword,
                        "label_text": label_text,
                        "gloss": gloss,
                        "answer_text": answer_text,
                        "note_text": note_text,
                        "dict_headwords": derive_textbook_dict_headwords({"headword": headword, "label_text": label_text}, headword),
                        "body_source_mode": body_source_mode,
                        "body_source_title": body_source_title,
                        "body_source_path": body_source_path,
                        "match_status": effective_mapping["status"],
                        "match_mode": effective_mapping["match_mode"],
                        "match_confidence": effective_mapping["match_confidence"],
                        "source_page_idx": layout_mapping["source_page_idx"],
                        "source_block_index": layout_mapping["source_block_index"],
                        "source_sentence": effective_mapping["source_sentence"],
                        "context_window": effective_mapping["context_window"],
                        "context_focus_index": effective_mapping.get("context_focus_index", 0),
                        "layout_match_status": layout_mapping["status"],
                        "layout_match_mode": layout_mapping["match_mode"],
                        "layout_match_confidence": layout_mapping["match_confidence"],
                        "layout_source_sentence": layout_mapping["source_sentence"],
                        "layout_context_window": layout_mapping["context_window"],
                        "text_match_status": text_mapping["status"],
                        "text_match_mode": text_mapping["match_mode"],
                        "text_match_confidence": text_mapping["match_confidence"],
                        "text_source_sentence": text_mapping["source_sentence"],
                        "text_context_window": text_mapping["context_window"],
                        "md_path": str(md_path),
                        "middle_path": str(middle_path),
                    }
                )
                accepted_note_count += 1
                book_note_count += 1

            article_rows[-1]["note_count"] = accepted_note_count

        per_book_stats.append(
            {
                "book_key": book_key,
                "book_title": book_title,
                "article_count": book_article_count,
                "corrected_article_count": corrected_article_count,
                "note_count": book_note_count,
                "unresolved_note_count": unresolved_count,
                "page_offset": page_offset,
            }
        )

    match_counter = Counter(str(row.get("match_mode") or "") for row in note_rows)
    report = {
        "book_count": len(per_book_stats),
        "article_count": len(article_rows),
        "note_count": len(note_rows),
        "unresolved_note_count": len(unresolved_note_rows),
        "body_source_counts": dict(body_source_counter),
        "match_mode_counts": dict(match_counter),
        "per_book": per_book_stats,
        "examples": {
            "matched": note_rows[:8],
            "unresolved": unresolved_note_rows[:20],
        },
        "source_shape": {
            "mineru_files": [
                "*.md",
                "*_middle.json",
                "*_content_list.json",
                "*_model.json",
                "*_origin.pdf",
                "images/*.jpg",
            ],
            "structured_text_source": str(corrected_source_path) if corrected_source_path else None,
            "recommended_authoritative_text_source": "jks 结构化教材全文 data/*.json（覆盖到的篇目优先使用）",
            "recommended_layout_anchor_source": "单册 MinerU middle.json / content_list.json",
            "recommended_note_source": "单册 MinerU md / middle.json 注释块",
            "non_authoritative_summary_source": "export/notebooklm/高中_语文.md",
        },
    }
    return article_rows, note_rows, unresolved_note_rows, report


def write_reports(
    article_rows: list[dict[str, Any]],
    note_rows: list[dict[str, Any]],
    unresolved_note_rows: list[dict[str, Any]],
    report: dict[str, Any],
) -> None:
    PRIVATE_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    ARTICLE_JSON.write_text(json.dumps(article_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    NOTE_JSON.write_text(json.dumps(note_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    NOTE_UNRESOLVED_JSON.write_text(json.dumps(unresolved_note_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(ARTICLE_CSV, article_rows)
    write_csv(NOTE_CSV, note_rows)
    write_csv(NOTE_UNRESOLVED_CSV, unresolved_note_rows)
    AUDIT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 高中语文教材源数据抽取审计",
        "",
        "## 结论",
        f"- 教材册数：{report['book_count']}",
        f"- 文言篇目数：{report['article_count']}",
        f"- 注释总数：{report['note_count']}",
        f"- 未精确定位注释数：{report['unresolved_note_count']}",
        f"- 正文来源分布：{json.dumps(report['body_source_counts'], ensure_ascii=False)}",
        "",
        "## 文件样貌",
        "- 每册目录下都存在单册 `*.md`、`*_middle.json`、`*_content_list.json`、`*_origin.pdf`、`images/*.jpg`。",
        "- 单册 `*.md` 适合抽线性注释和 OCR 正文兜底。",
        "- `*_middle.json` / `*_content_list.json` 提供 `page_idx`、`bbox`、block type，适合做页内精确定位。",
        "- `export/notebooklm/高中_语文.md` 只适合汇总阅读，不适合作为精确映射底座。",
        "",
        "## 推荐抽取规则",
        "- 正文：优先使用本机 `jks/_legacy/yuwen/public/data/*.json` 中结构化的 `main_text` 全文；缺失篇目再退回单册 MinerU OCR 正文。",
        "- 注释：继续以单册 MinerU md / `middle.json` 注释块为权威注释文本。",
        "- 对位：正文命中与页内定位分开保存。`source_sentence/context_window` 走校对正文；`source_page_idx/source_block_index` 保留教材页块锚点。",
        "- 低质量条目：如果校对正文无法精确命中注释标签，则进入 `textbook_note_unresolved_table`，不混入主表。",
        "",
        "## 分册统计",
    ]
    for item in report["per_book"]:
        lines.append(
            f"- {item['book_title']}：篇目 {item['article_count']}，校对正文 {item['corrected_article_count']}，注释 {item['note_count']}，未定位 {item['unresolved_note_count']}"
        )
    lines.extend(
        [
            "",
            "## 匹配模式统计",
            f"- {json.dumps(report['match_mode_counts'], ensure_ascii=False)}",
            "",
            "## 主要产物",
            f"- {ARTICLE_JSON}",
            f"- {NOTE_JSON}",
            f"- {NOTE_UNRESOLVED_JSON}",
            f"- {ARTICLE_CSV}",
            f"- {NOTE_CSV}",
            f"- {NOTE_UNRESOLVED_CSV}",
            f"- {AUDIT_JSON}",
        ]
    )
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    article_rows, note_rows, unresolved_note_rows, report = build_tables()
    write_reports(article_rows, note_rows, unresolved_note_rows, report)
    print(
        json.dumps(
            {
                "ok": True,
                "articles": len(article_rows),
                "notes": len(note_rows),
                "unresolved": len(unresolved_note_rows),
                "report": str(AUDIT_JSON),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
