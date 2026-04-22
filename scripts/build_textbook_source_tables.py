#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import csv
import html
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
FORUM_CACHE_JSON = PRIVATE_DIR / "forum_textbook_source_cache.json"
FORUM_TOPICS_JSON = PRIVATE_DIR / "forum_textbook_topics_raw.json"
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
FORUM_TITLE_PREFIX_RE = re.compile(r"^高中語文\s*-\s*")
FORUM_TOPIC_PREFIX_RE = re.compile(r"^[0-9*、，,\s]+")
FORUM_NOTE_BRACKET_RE = re.compile(r"^[〔［\[]([^〕］\]]+)[〕］\]]\s*(.+)$|^[【]([^】]+)[】]\s*(.+)$")
FORUM_NOTE_COLON_RE = re.compile(r"^(.{1,24}?)[：:]\s*(.+)$")
FOOTNOTE_TOKEN_RE = re.compile(r"\[\d+\]")
RAW_FOOTNOTE_REF_RE = re.compile(r"\[\^(\d+)\]")
RAW_FOOTNOTE_DEF_RE = re.compile(r"^\[\^(\d+)\]:\s*(.+)$")
RAW_COLOR_TAG_RE = re.compile(r"\[color=red\](.*?)\[/color\]", re.I | re.S)
RAW_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

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
        stripped_parenthetical = clean_text(re.sub(r"[（(][^）)]+[）)]", "", raw))
        if stripped_parenthetical and stripped_parenthetical != clean_text(raw):
            expanded.append(stripped_parenthetical)
        if clean_text(raw).endswith("并序"):
            expanded.append(clean_text(raw)[:-2])
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


def load_forum_topic_payload() -> dict[str, Any]:
    if not FORUM_TOPICS_JSON.exists():
        return {}
    payload = load_json(FORUM_TOPICS_JSON)
    return payload if isinstance(payload, dict) else {}


def normalize_forum_topic_title(title: str) -> str:
    cleaned = clean_text(str(title or ""))
    cleaned = FORUM_TITLE_PREFIX_RE.sub("", cleaned)
    cleaned = cleaned.replace("／", "/").replace("*", " ")
    cleaned = FORUM_TOPIC_PREFIX_RE.sub("", cleaned)
    return normalize_body_source_title_key(cleaned)


def strip_raw_markup(text: str, *, remove_refs: bool = False) -> str:
    value = str(text or "")
    value = RAW_COLOR_TAG_RE.sub(r"\1", value)
    value = RAW_MD_LINK_RE.sub(r"\1", value)
    if remove_refs:
        value = RAW_FOOTNOTE_REF_RE.sub("", value)
    value = value.replace("**", "").replace("*", "")
    value = value.replace("&nbsp;", " ")
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value)
    return clean_text_keep_newlines(value)


def raw_topic_lines(raw: str) -> list[str]:
    lines: list[str] = []
    for line in str(raw or "").splitlines():
        cleaned = strip_raw_markup(line.rstrip("\n"))
        if not clean_text(cleaned):
            lines.append("")
            continue
        if cleaned in {"</p>", "<hr>"}:
            continue
        if cleaned.startswith("This is a companion discussion topic for the original entry at"):
            break
        lines.append(cleaned)
    while lines and not clean_text(lines[0]):
        lines.pop(0)
    while lines and not clean_text(lines[-1]):
        lines.pop()
    return lines


def normalize_forum_heading_line(line: str) -> str:
    value = strip_raw_markup(line, remove_refs=True)
    value = value.lstrip(">").strip()
    return normalize_body_source_title_key(value)


def is_forum_heading_candidate(line: str) -> bool:
    raw_line = str(line or "")
    cleaned = clean_text(strip_raw_markup(raw_line, remove_refs=True))
    if not cleaned:
        return False
    if raw_line.lstrip().startswith(">"):
        return False
    if RAW_FOOTNOTE_DEF_RE.match(clean_text(raw_line)):
        return False
    if cleaned.startswith(("!image", "This is a companion discussion topic for the original entry at")):
        return False
    if len(cleaned) > 40:
        return False
    return True


def find_forum_heading_positions(lines: list[str], titles: list[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    variant_map = {
        title: [normalize_body_source_title_key(variant) for variant in corrected_title_variants(title) if normalize_body_source_title_key(variant)]
        for title in titles
    }
    for index, line in enumerate(lines):
        if not is_forum_heading_candidate(line):
            continue
        normalized_line = normalize_forum_heading_line(line)
        if not normalized_line:
            continue
        for title in titles:
            if title in result:
                continue
            variants = variant_map.get(title) or []
            if any(
                variant
                and (
                    normalized_line == variant
                    or normalized_line.startswith(variant)
                    or (variant in normalized_line and len(normalized_line) <= len(variant) + 6)
                )
                for variant in variants
            ):
                result[title] = index
    return result


def detect_forum_leading_title(lines: list[str], titles: list[str]) -> str:
    variant_map = {
        title: [normalize_body_source_title_key(variant) for variant in corrected_title_variants(title) if normalize_body_source_title_key(variant)]
        for title in titles
    }
    inspected = 0
    for line in lines:
        cleaned = clean_text(strip_raw_markup(line, remove_refs=True))
        if not cleaned:
            continue
        if RAW_FOOTNOTE_DEF_RE.match(clean_text(line)):
            break
        if line.lstrip().startswith(">"):
            continue
        if cleaned.startswith(("!image", "---")):
            continue
        normalized_line = normalize_body_source_title_key(cleaned)
        if not normalized_line:
            continue
        inspected += 1
        for title in titles:
            variants = variant_map.get(title) or []
            if any(variant and (normalized_line == variant or normalized_line.startswith(variant)) for variant in variants):
                return title
        if inspected >= 8:
            break
    return ""


def looks_like_author_line(line: str) -> bool:
    cleaned = clean_text(strip_raw_markup(line, remove_refs=True)).replace(" ", "")
    return bool(cleaned) and not RAW_FOOTNOTE_REF_RE.search(line) and bool(re.fullmatch(r"[\u4e00-\u9fff·]{2,8}", cleaned))


def looks_like_forum_commentary_line(line: str) -> bool:
    cleaned = clean_text(strip_raw_markup(line, remove_refs=True))
    if not cleaned:
        return False
    if cleaned.startswith(">"):
        return True
    commentary_prefixes = (
        "本单元",
        "学习时",
        "学习要",
        "阅读时",
        "阅读课文",
        "前人评论",
        "课文中",
        "背诵",
        "这里各选入",
        "这首",
        "这篇",
        "两首诗",
        "李白、杜甫",
    )
    return cleaned.startswith(commentary_prefixes)


def looks_like_forum_note_label(label: str) -> bool:
    cleaned = clean_text(str(label or ""))
    if not cleaned:
        return False
    if cleaned.startswith(("#", "##", "###")):
        return False
    if re.match(r"^[0-9一二三四五六七八九十]+[.、]", cleaned):
        return False
    if cleaned.startswith(("http://", "https://", "www.")):
        return False
    if any(token in cleaned for token in ("高考", "教材", "学习", "链接", "下载")):
        return False
    compact = cleaned.replace(" ", "")
    if len(compact) > 16:
        return False
    return True


def looks_like_source_preface_line(text: str) -> bool:
    cleaned = clean_text(str(text or ""))
    if not cleaned:
        return False
    prefixes = (
        "选自《",
        "选自《论语译注》",
        "题目是编者加的",
        "[color=",
        "[/color]",
        "!Line ",
    )
    return cleaned.startswith(prefixes)


def parse_forum_note_parts(note_text: str) -> tuple[str, str]:
    cleaned = clean_text(strip_raw_markup(note_text, remove_refs=True))
    if not cleaned:
        return "", ""
    bracket_match = FORUM_NOTE_BRACKET_RE.match(cleaned)
    if bracket_match:
        label = clean_text(bracket_match.group(1) or bracket_match.group(3) or "")
        body = clean_text(bracket_match.group(2) or bracket_match.group(4) or "")
        return (label, body) if looks_like_forum_note_label(label) else ("", "")
    colon_match = FORUM_NOTE_COLON_RE.match(cleaned)
    if colon_match:
        label = clean_text(colon_match.group(1) or "")
        body = clean_text(colon_match.group(2) or "")
        return (label, body) if looks_like_forum_note_label(label) else ("", "")
    return "", cleaned


def derive_forum_label_from_line(line: str, marker: str) -> str:
    raw_line = strip_raw_markup(line)
    marker_token = f"[^{marker}]"
    position = raw_line.find(marker_token)
    if position < 0:
        return ""
    before = RAW_FOOTNOTE_REF_RE.sub("", clean_text(raw_line[:position]))
    if not before:
        return ""
    clause = re.split(r"[，,。；;：:！？!?“”\"'‘’（）()\s]+", before)[-1]
    clause = clean_text(clause)
    clause = re.sub(r"^[之其而以于为则乃且若所又方并与及或将使令王楚秦齐晋周汉唐宋李白杜甫白居易贾谊司马迁欧阳修屈原班固陆游李贺高适王羲之陶渊明归有光柳宗元苏轼]+", "", clause)
    if clause and len(clause) <= 8:
        return clause
    han_tail = "".join(re.findall(r"[\u4e00-\u9fff]+", before))
    if len(han_tail) <= 4:
        return han_tail
    return han_tail[-4:]


def build_forum_context_window(lines: list[str], focus_index: int) -> tuple[str, list[str], int]:
    cleaned_lines = [clean_text(strip_raw_markup(line, remove_refs=True)) for line in lines if clean_text(strip_raw_markup(line, remove_refs=True))]
    if not cleaned_lines:
        return "", [], 0
    target = min(max(0, focus_index), len(cleaned_lines) - 1)
    start = max(0, target - 3)
    end = min(len(cleaned_lines), target + 4)
    window = cleaned_lines[start:end]
    return cleaned_lines[target], window, target - start


def collect_forum_footnote_lines(lines: list[str]) -> list[str]:
    footnote_lines: list[str] = []
    for line in lines:
        cleaned = clean_text(line)
        if not cleaned:
            continue
        if RAW_FOOTNOTE_DEF_RE.match(cleaned):
            footnote_lines.append(cleaned)
            continue
        if footnote_lines and line.startswith(("  ", "\t")):
            footnote_lines[-1] = f"{footnote_lines[-1]} {clean_text(line)}"
    return footnote_lines


def build_forum_article_entry(
    title: str,
    topic: dict[str, Any],
    section_lines: list[str],
    parsed_notes: list[dict[str, Any]],
    note_source_lines: list[str] | None = None,
) -> dict[str, Any] | None:
    body_lines: list[str] = []
    quote_note_lines: list[str] = []
    phase = "body"
    content_started = False
    section_has_inline_refs = any(RAW_FOOTNOTE_REF_RE.search(line) for line in section_lines)
    title_norms = {normalize_body_source_title_key(v) for v in corrected_title_variants(title)}
    for line in section_lines:
        cleaned = clean_text(line)
        if not cleaned:
            continue
        if looks_like_source_preface_line(cleaned):
            continue
        quote_candidate = clean_text(strip_raw_markup(line, remove_refs=True)).lstrip(">").strip()
        quote_label, quote_body = parse_forum_note_parts(quote_candidate)
        if RAW_FOOTNOTE_DEF_RE.match(cleaned):
            continue
        if phase == "body":
            normalized_line = normalize_forum_heading_line(line)
            is_title_heading = normalized_line in title_norms
            if not content_started and (
                line.lstrip().startswith(">")
                or cleaned.startswith("!")
                or cleaned == "---"
                or (len(cleaned) <= 18 and cleaned.endswith("作"))
            ):
                continue
            if not content_started and section_has_inline_refs and not is_title_heading and not RAW_FOOTNOTE_REF_RE.search(line):
                continue
            if content_started and line.lstrip().startswith(">") and quote_label and quote_body:
                phase = "quote_note"
                quote_note_lines.append(quote_candidate)
                continue
            if is_title_heading or (not line.lstrip().startswith(">") and not cleaned.startswith("!") and cleaned != "---"):
                content_started = True
            body_lines.append(cleaned)
            continue
        if phase == "quote_note":
            if line.lstrip().startswith(">") and not quote_candidate:
                continue
            if line.lstrip().startswith(">") and quote_label and quote_body:
                quote_note_lines.append(quote_candidate)
                continue
            if looks_like_forum_commentary_line(cleaned):
                break
            break

    footnote_lines = collect_forum_footnote_lines(note_source_lines or section_lines)

    heading_index = next((idx for idx, line in enumerate(body_lines) if normalize_forum_heading_line(line) in {normalize_body_source_title_key(v) for v in corrected_title_variants(title)}), None)
    full_text_lines: list[str] = []
    ref_lines: list[dict[str, Any]] = []
    for index, line in enumerate(body_lines):
        display = clean_text(strip_raw_markup(line, remove_refs=True))
        if not display:
            continue
        is_heading = heading_index is not None and index == heading_index
        is_author = looks_like_author_line(line)
        is_blockquote = display.startswith(">")
        if not (is_heading or is_author or is_blockquote or display.startswith("!image")):
            full_text_lines.append(display.lstrip(">").strip())
        if not RAW_FOOTNOTE_REF_RE.search(line):
            continue
        annotation_scope = "meta" if is_heading or is_author or is_blockquote else "body"
        ref_lines.append(
            {
                "line": line,
                "display": display.lstrip(">").strip(),
                "scope": annotation_scope,
            }
        )
    if not full_text_lines:
        return None

    forum_notes: list[dict[str, Any]] = []
    ocr_note_count = len(parsed_notes)
    if footnote_lines:
        for footnote_index, line in enumerate(footnote_lines, start=1):
            match = RAW_FOOTNOTE_DEF_RE.match(line)
            if not match:
                continue
            marker = str(match.group(1))
            note_raw = str(match.group(2) or "")
            line_index = next((idx for idx, item in enumerate(ref_lines) if f"[^{marker}]" in item["line"]), -1)
            if line_index < 0:
                continue
            ref_line = ref_lines[line_index]
            sentence, context_window, focus_index = build_forum_context_window([item["line"] for item in ref_lines], line_index)
            explicit_label, note_text = parse_forum_note_parts(note_raw)
            ocr_note = parsed_notes[footnote_index - 1] if footnote_index - 1 < ocr_note_count else {}
            derived_label = derive_forum_label_from_line(ref_line["line"], marker)
            label_text = RAW_FOOTNOTE_REF_RE.sub("", clean_text(str(explicit_label or derived_label or ocr_note.get("label_text") or "")))
            headword = clean_text(str(ocr_note.get("headword") or "")) or extract_note_headword(label_text, note_text)
            forum_notes.append(
                {
                    "note_order": footnote_index,
                    "marker": marker,
                    "label_text": label_text,
                    "headword": headword,
                    "note_text": note_text,
                    "annotation_scope": str(ref_line["scope"]),
                    "source_sentence": sentence,
                    "context_window": context_window,
                    "context_focus_index": focus_index,
                }
            )
    else:
        for note_index, line in enumerate(quote_note_lines, start=1):
            explicit_label, note_text = parse_forum_note_parts(line)
            if not explicit_label or not note_text:
                continue
            ocr_note = parsed_notes[note_index - 1] if note_index - 1 < ocr_note_count else {}
            label_text = clean_text(str(explicit_label or ocr_note.get("label_text") or ""))
            headword = clean_text(str(ocr_note.get("headword") or "")) or extract_note_headword(label_text, note_text)
            forum_notes.append(
                {
                    "note_order": note_index,
                    "marker": "",
                    "label_text": label_text,
                    "headword": headword,
                    "note_text": note_text,
                    "annotation_scope": "body",
                    "source_sentence": "",
                    "context_window": [],
                    "context_focus_index": 0,
                }
            )

    if not forum_notes:
        return None

    author = ""
    if heading_index is not None and heading_index + 1 < len(body_lines):
        maybe_author = body_lines[heading_index + 1]
        if looks_like_author_line(maybe_author):
            author = clean_text(strip_raw_markup(maybe_author, remove_refs=True))
    if not author and "/" in str(topic.get("topic_title") or ""):
        author = clean_text(str(topic.get("topic_title") or "").split("/")[-1])

    entry = {
        "source_mode": "forum_raw",
        "source_path": f"forum-backend:topic:{topic.get('topic_id')}",
        "source_title": clean_text(str(topic.get("topic_title") or "")),
        "source_topic_id": topic.get("topic_id"),
        "title": title,
        "author": author,
        "full_text": clean_text_keep_newlines("\n".join(full_text_lines)).strip(),
        "notes": forum_notes,
    }
    return entry if entry["full_text"] else None


def build_forum_source_lookup(manifest_titles: list[str], parsed_note_lookup: dict[str, list[dict[str, Any]]]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    payload = load_forum_topic_payload()
    posts = payload.get("posts") or payload.get("topics") or []
    if not isinstance(posts, list):
        return {}, {"topic_count": 0, "article_hits": 0}

    lookup: dict[str, dict[str, Any]] = {}
    title_norms = {
        title: [normalize_body_source_title_key(variant) for variant in corrected_title_variants(title) if normalize_body_source_title_key(variant)]
        for title in manifest_titles
    }
    topic_ids: set[str] = set()
    post_count = 0
    for post in posts:
        if not isinstance(post, dict):
            continue
        raw = str(post.get("raw") or "")
        if not raw:
            continue
        lines = raw_topic_lines(raw)
        if not lines:
            continue
        post_count += 1
        topic_ids.add(str(post.get("topic_id") or ""))
        topic_title_norm = normalize_forum_topic_title(str(post.get("topic_title") or ""))
        matched_titles_from_title = [
            title
            for title in manifest_titles
            if any(variant and variant in topic_title_norm for variant in title_norms.get(title, []))
        ]
        global_leading_title = detect_forum_leading_title(lines, manifest_titles)
        global_heading_positions = find_forum_heading_positions(lines, manifest_titles)
        if matched_titles_from_title:
            candidate_titles = list(dict.fromkeys(matched_titles_from_title))
            if global_leading_title:
                candidate_titles.append(global_leading_title)
            if int(post.get("post_number") or 1) == 1:
                candidate_titles.extend(global_heading_positions.keys())
            candidate_titles = list(dict.fromkeys(candidate_titles))
        else:
            candidate_titles = manifest_titles
        leading_title = detect_forum_leading_title(lines, candidate_titles)
        heading_positions = find_forum_heading_positions(lines, candidate_titles)
        if matched_titles_from_title and int(post.get("post_number") or 1) > 1 and not leading_title:
            heading_positions = {title: position for title, position in heading_positions.items() if position <= 3}
        start_positions: dict[str, int] = {}
        if leading_title:
            start_positions[leading_title] = 0
        elif matched_titles_from_title and int(post.get("post_number") or 1) == 1:
            start_positions[matched_titles_from_title[0]] = 0
        for title, position in heading_positions.items():
            current = start_positions.get(title)
            if current is None or position < current:
                start_positions[title] = position
        if not start_positions:
            continue
        sorted_titles = sorted(start_positions, key=lambda item: (start_positions[item], manifest_titles.index(item)))
        for index, title in enumerate(sorted_titles):
            if title in lookup:
                continue
            start = start_positions[title]
            next_candidates = [start_positions[other] for other in sorted_titles[index + 1 :] if start_positions[other] > start]
            end = min(next_candidates) if next_candidates else len(lines)
            section_lines = lines[start:end]
            entry = build_forum_article_entry(
                title,
                post,
                section_lines,
                parsed_note_lookup.get(title, []),
                note_source_lines=lines,
            )
            if not entry:
                continue
            lookup[title] = entry

    FORUM_CACHE_JSON.write_text(
        json.dumps(
            {
                "built_from": str(FORUM_TOPICS_JSON),
                "article_count": len(lookup),
                "articles": lookup,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return lookup, {"topic_count": len(topic_ids), "post_count": post_count, "article_hits": len(lookup)}


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
    precomputed_note_lookup: dict[str, list[dict[str, Any]]] = {}
    all_article_titles: list[str] = []

    for book_key in sorted(key for key in manifest if key.startswith("高中_语文_")):
        md_path = book_paths.get(book_key)
        if not md_path or not md_path.exists():
            continue
        middle_path = next((path for path in md_path.parent.glob("*_middle.json")), None)
        if not middle_path or not middle_path.exists():
            continue

        raw_md = md_path.read_text(encoding="utf-8")
        manifest_items = [item for item in manifest.get(book_key, []) if clean_text(str(item.get("title") or ""))]
        article_specs = expand_manifest_items(manifest_items)
        article_titles = [clean_text(str(item.get("title") or "")) for item in article_specs]
        section_map = build_section_text_map(raw_md, article_titles)
        page_blocks = build_page_blocks(middle_path)
        article_positions, page_offset = find_article_positions(page_blocks, article_specs, section_map)
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
            precomputed_note_lookup[title] = parsed_notes
            if title not in all_article_titles:
                all_article_titles.append(title)

    forum_lookup, forum_meta = build_forum_source_lookup(all_article_titles, precomputed_note_lookup)

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
            forum_entry = forum_lookup.get(title)
            corrected_entry = None
            if not forum_entry:
                corrected_entry = resolve_corrected_poem(title, corrected_lookup)
                if corrected_entry and not corrected_entry_covers_article(corrected_entry, ocr_body_text, parsed_notes):
                    corrected_entry = None

            source_entry = forum_entry or corrected_entry or {}
            body_text = clean_text_keep_newlines(str(source_entry.get("full_text") or ocr_body_text)).strip()
            author = clean_text(str(source_entry.get("author") or ocr_author))
            body_source_mode = str(source_entry.get("source_mode") or "mineru_md")
            body_source_title = clean_text(str(source_entry.get("source_title") or title))
            body_source_path = str(source_entry.get("source_path") or md_path)
            note_candidates = list(source_entry.get("notes") or parsed_notes)

            end_page_idx = int(body_blocks[-1]["page_idx"]) if body_blocks else int(heading["page_idx"])
            accepted_note_count = 0
            body_source_counter[body_source_mode] += 1
            if forum_entry or corrected_entry:
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
                    "note_candidate_count": len(note_candidates),
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
            for note_candidate in note_candidates:
                note_order = int(note_candidate.get("note_order") or 0) or 1
                label_text = clean_text(str(note_candidate.get("label_text") or ""))
                note_text = sanitize_note_text_value(str(note_candidate.get("note_text") or ""))
                headword = clean_text(str(note_candidate.get("headword") or "")) or extract_note_headword(label_text, note_text)
                gloss = summarize_note_gloss(headword, note_text)
                answer_text = build_textbook_answer_text(label_text, note_text, gloss)
                term_kind = infer_textbook_term_kind(headword, label_text, note_text, gloss)
                annotation_scope = clean_text(str(note_candidate.get("annotation_scope") or "body")) or "body"
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
                if forum_entry:
                    source_sentence = clean_text(str(note_candidate.get("source_sentence") or ""))
                    context_window = [clean_text(str(item)) for item in list(note_candidate.get("context_window") or []) if clean_text(str(item))]
                    effective_mapping = {
                        "status": "matched" if source_sentence else text_mapping["status"],
                        "match_mode": "forum_raw_marker" if source_sentence else text_mapping["match_mode"],
                        "match_confidence": 1.0 if source_sentence and annotation_scope == "body" else 0.9 if source_sentence else text_mapping["match_confidence"],
                        "source_sentence": source_sentence or text_mapping["source_sentence"],
                        "context_window": context_window or text_mapping["context_window"],
                        "context_focus_index": int(note_candidate.get("context_focus_index") or text_mapping.get("context_focus_index") or 0),
                    }
                else:
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
                            "annotation_scope": annotation_scope,
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
                if (forum_entry or corrected_entry) and text_mapping["status"] == "matched":
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
                        "annotation_scope": annotation_scope,
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
        "forum_cache": {
            "raw_topics_path": str(FORUM_TOPICS_JSON),
            "parsed_cache_path": str(FORUM_CACHE_JSON),
            **forum_meta,
        },
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
            "forum_raw_source": str(FORUM_TOPICS_JSON),
            "structured_text_source": str(corrected_source_path) if corrected_source_path else None,
            "recommended_authoritative_text_source": "forum-backend 数据库中的 Discourse 首帖 raw（五册高中语文教材分类）",
            "recommended_layout_anchor_source": "单册 MinerU middle.json / content_list.json",
            "recommended_note_source": "forum-backend 原始脚注文本；本机 MinerU 注释块只用于标签对位与页内锚点",
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
        f"- 论坛 raw 缓存：`{FORUM_TOPICS_JSON}`，来自 `forum-backend` 上 Discourse 数据库 `posts.raw`。",
        "- 每册目录下都存在单册 `*.md`、`*_middle.json`、`*_content_list.json`、`*_origin.pdf`、`images/*.jpg`。",
        "- 单册 `*.md` / `middle.json` 保留作页内锚点、标签对位与兜底校验，不再作为正文权威源。",
        "- `*_middle.json` / `*_content_list.json` 提供 `page_idx`、`bbox`、block type，适合做页内精确定位。",
        "- `export/notebooklm/高中_语文.md` 只适合汇总阅读，不适合作为精确映射底座。",
        "",
        "## 推荐抽取规则",
        "- 正文：优先使用 `forum-backend` 中教材主题首帖 `raw` 的文言文正文。",
        "- 注释：优先使用论坛首帖 `raw` 内脚注；本机 MinerU 注释块用于脚注顺序对齐、标签补全和页内锚点。",
        "- 对位：`source_sentence/context_window` 以论坛 raw 的脚注标记定位；`source_page_idx/source_block_index` 保留教材页块锚点。",
        "- 低质量条目：如果论坛正文脚注无法稳定定位，或本机锚点无法补齐，则进入 `textbook_note_unresolved_table`，不混入主表。",
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
