#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from check_sources import (
    JUNIOR_MD_PATH,
    MANIFEST_PATH,
    MOE_IDIOMS_PATH,
    MOE_REVISED_PATH,
    REPO_ROOT,
    SENIOR_MD_PATH,
    SHICI_PATH,
    SOURCE_ROOT,
    XUCI_PATH,
    ensure_sources_or_raise,
    normalize_title,
    open_sqlite_readonly,
    collect_source_report,
    split_title_parts,
    title_part_variants,
)


ASSET_MAX_BYTES = int(os.environ.get("ASSET_MAX_BYTES", "26214400"))
RUNTIME_MIRROR_DIR = REPO_ROOT / "data" / "runtime"
PUBLIC_RUNTIME_DIR = REPO_ROOT / "public" / "runtime"
QUESTION_TEMPLATES_DIR = REPO_ROOT / "question_templates"
PRIVATE_RUNTIME_DIR = REPO_ROOT / "data" / "runtime_private"
GENERATED_DIR = REPO_ROOT / "src" / "generated"


QUESTION_TYPE_TO_BASIS = {
    "xuci_compare_same": "direct_choice",
    "xuci_compare_diff": "direct_choice",
    "function_gloss": "direct_choice",
    "function_profile": "direct_choice",
    "xuci_explanation": "direct_choice",
    "shici_explanation": "direct_choice",
    "national_raw_gloss_option": "direct_choice",
    "national_raw_translation_keyword": "direct_choice",
    "sentence_meaning": "sentence_meaning",
    "passage_meaning": "passage_meaning",
}

QUESTION_TYPES = [
    "xuci_pair_compare",
    "function_gloss",
    "function_profile",
    "content_gloss",
    "sentence_meaning",
    "passage_meaning",
]

CONTENT_PRIORITY_CORE = "core"
CONTENT_PRIORITY_SECONDARY = "secondary"
FUNCTION_PRIORITY_SUPPORT = "support"
BANNED_GLOSS_CANDIDATES = {
    "是",
    "有",
    "又",
    "来",
    "去",
    "即",
    "指",
    "这个",
    "这样",
    "这些",
    "那个",
    "那些",
    "之",
    "而",
    "于",
    "与",
}

GLOSS_BLOCK_MARKERS = (
    "翻译为",
    "译为",
    "句子",
    "注意点",
    "参考答案",
    "答案示例",
    "这里活用",
)

FUNCTION_WORD_PROFILES: dict[str, list[dict[str, str]]] = {
    "以": [
        {"part_of_speech": "介词", "semantic_value": "用/凭借/因为/把", "syntactic_function": "引出工具、对象、原因、时间", "relation": "因果"},
        {"part_of_speech": "连词", "semantic_value": "来/以致/因为", "syntactic_function": "连接前后分句", "relation": "承接"},
    ],
    "之": [
        {"part_of_speech": "代词", "semantic_value": "他/它/这件事", "syntactic_function": "作宾语或兼指前文内容", "relation": ""},
        {"part_of_speech": "助词", "semantic_value": "的/取消句子独立性", "syntactic_function": "结构助词或主谓之间助词", "relation": ""},
    ],
    "而": [
        {"part_of_speech": "连词", "semantic_value": "并且/然后/却", "syntactic_function": "连接词、短语或分句", "relation": "并列"},
        {"part_of_speech": "连词", "semantic_value": "并且/然后/却", "syntactic_function": "连接词、短语或分句", "relation": "承接"},
        {"part_of_speech": "连词", "semantic_value": "并且/然后/却", "syntactic_function": "连接词、短语或分句", "relation": "递进"},
        {"part_of_speech": "连词", "semantic_value": "却/但是", "syntactic_function": "连接前后分句", "relation": "转折"},
        {"part_of_speech": "连词", "semantic_value": "地", "syntactic_function": "连接状语与中心语", "relation": "修饰"},
    ],
    "为": [
        {"part_of_speech": "介词", "semantic_value": "替/给/为了/被", "syntactic_function": "引出对象、原因、目的或被动", "relation": "被动"},
        {"part_of_speech": "动词", "semantic_value": "是/成为", "syntactic_function": "判断或谓语动词", "relation": "判断"},
    ],
    "者": [
        {"part_of_speech": "助词", "semantic_value": "……的人/事/情况", "syntactic_function": "名词化标记", "relation": ""},
        {"part_of_speech": "助词", "semantic_value": "停顿提示", "syntactic_function": "句中停顿或提示判断", "relation": "语气"},
    ],
    "其": [
        {"part_of_speech": "代词", "semantic_value": "他/他们/其中/那", "syntactic_function": "作定语、主语或宾语", "relation": ""},
        {"part_of_speech": "副词", "semantic_value": "大概/还是/难道", "syntactic_function": "表揣测、反问、祈使", "relation": "语气"},
    ],
    "与": [
        {"part_of_speech": "介词", "semantic_value": "和/同/给", "syntactic_function": "引出比较对象或共同对象", "relation": "并列"},
        {"part_of_speech": "连词", "semantic_value": "和", "syntactic_function": "连接并列成分", "relation": "并列"},
    ],
    "于": [
        {"part_of_speech": "介词", "semantic_value": "在/到/向/对/比/被", "syntactic_function": "引出处所、对象、比较、被动", "relation": "被动"},
    ],
    "因": [
        {"part_of_speech": "介词", "semantic_value": "趁着/依据", "syntactic_function": "引出凭借条件", "relation": "条件"},
        {"part_of_speech": "连词", "semantic_value": "于是/因此", "syntactic_function": "连接前后分句", "relation": "因果"},
    ],
    "乃": [
        {"part_of_speech": "副词", "semantic_value": "才/竟然/于是", "syntactic_function": "修饰谓语", "relation": "承接"},
    ],
    "或": [
        {"part_of_speech": "代词", "semantic_value": "有人/有的", "syntactic_function": "作主语或宾语", "relation": ""},
        {"part_of_speech": "副词", "semantic_value": "或许", "syntactic_function": "表不确定", "relation": "语气"},
    ],
    "则": [
        {"part_of_speech": "连词", "semantic_value": "就/那么/却", "syntactic_function": "连接条件与结果或前后对比", "relation": "承接"},
        {"part_of_speech": "连词", "semantic_value": "就/那么/却", "syntactic_function": "连接条件与结果或前后对比", "relation": "转折"},
    ],
    "遂": [
        {"part_of_speech": "副词", "semantic_value": "于是/终于", "syntactic_function": "表承接结果", "relation": "承接"},
    ],
    "何": [
        {"part_of_speech": "代词", "semantic_value": "什么/哪里", "syntactic_function": "疑问代词", "relation": ""},
        {"part_of_speech": "副词", "semantic_value": "为什么/怎么", "syntactic_function": "疑问或反问", "relation": "语气"},
    ],
    "所": [
        {"part_of_speech": "助词", "semantic_value": "……的人/事/地方", "syntactic_function": "与后动词组成名词性结构", "relation": ""},
    ],
    "然": [
        {"part_of_speech": "代词", "semantic_value": "这样/如此", "syntactic_function": "指示前文情况", "relation": ""},
        {"part_of_speech": "助词", "semantic_value": "……的样子", "syntactic_function": "词尾或语气提示", "relation": "语气"},
    ],
    "乎": [
        {"part_of_speech": "语气词", "semantic_value": "吗/呢/吧", "syntactic_function": "表疑问、感叹、停顿", "relation": "语气"},
        {"part_of_speech": "介词", "semantic_value": "于/在/从", "syntactic_function": "引出处所、对象", "relation": ""},
    ],
    "若": [
        {"part_of_speech": "连词", "semantic_value": "如果", "syntactic_function": "引导假设分句", "relation": "假设"},
        {"part_of_speech": "代词", "semantic_value": "你/你们/这样", "syntactic_function": "指人或指示", "relation": ""},
    ],
    "焉": [
        {"part_of_speech": "代词", "semantic_value": "于此/于之/哪里", "syntactic_function": "兼词或疑问代词", "relation": ""},
        {"part_of_speech": "语气词", "semantic_value": "啊/呢", "syntactic_function": "句末语气", "relation": "语气"},
    ],
    "既": [
        {"part_of_speech": "副词", "semantic_value": "已经", "syntactic_function": "表动作已完成", "relation": "承接"},
        {"part_of_speech": "连词", "semantic_value": "既然", "syntactic_function": "引导前提分句", "relation": "因果"},
    ],
    "虽": [
        {"part_of_speech": "连词", "semantic_value": "虽然/即使", "syntactic_function": "引导让步分句", "relation": "让步"},
    ],
    "盖": [
        {"part_of_speech": "副词", "semantic_value": "大概/原来", "syntactic_function": "表推测或解释", "relation": "语气"},
        {"part_of_speech": "连词", "semantic_value": "因为/发语", "syntactic_function": "引出解释", "relation": "因果"},
    ],
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("_x000D_", " ")
    text = text.replace("*", "")
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_gloss(headword: str, gloss: str, excerpt: str = "") -> str:
    raw_gloss = clean_text(gloss)
    if not raw_gloss:
        return ""
    working = raw_gloss
    if headword:
        working = re.sub(rf"^{re.escape(headword)}\s*[:：,，]\s*", "", working)
    same_as_match = re.search(r"义同\s*[“\"'‘]?([\u4e00-\u9fff]{1,8})", raw_gloss)
    if same_as_match:
        return same_as_match.group(1)
    translated_match = re.search(r"译为\s*[“\"'‘]?([\u4e00-\u9fff]{1,12})", raw_gloss)
    if translated_match:
        return translated_match.group(1).split("、", 1)[0]
    quoted_match = re.search(r"[“\"'‘]([\u4e00-\u9fff]{1,12})[”\"'’]", raw_gloss)
    if quoted_match and all(marker not in raw_gloss for marker in ("亲贤", "推毂", "罪", "注意点")):
        return quoted_match.group(1).split("、", 1)[0]
    working = re.split(r"[。；;]\s*\d+\s*", working, maxsplit=1)[0]
    working = re.split(r"\s+\d+\s*[“\"'‘]", working, maxsplit=1)[0]
    for marker in GLOSS_BLOCK_MARKERS:
        working = working.split(marker, 1)[0]
    working = re.sub(r"[“”\"'‘’]", "", working)
    working = working.strip("，,；;、。:： ")
    if "、" in working:
        working = working.split("、", 1)[0]
    if any(marker in working for marker in ("这里活用", "义同")):
        working = re.split(r"[，,；;]", working, maxsplit=1)[0]
    if len(working) > 20 and any(sep in working for sep in ("，", ",", "；", ";", "。")):
        working = re.split(r"[，,；;。]", working, maxsplit=1)[0]
    return clean_text(working)


def looks_like_clean_gloss(gloss: str) -> bool:
    cleaned = clean_text(gloss)
    if not cleaned or len(cleaned) > 20:
        return False
    if cleaned in BANNED_GLOSS_CANDIDATES:
        return False
    if any(marker in cleaned for marker in GLOSS_BLOCK_MARKERS):
        return False
    if re.search(r"\d", cleaned):
        return False
    if re.search(r"[“”\"'‘’]", cleaned):
        return False
    if re.search(r"[A-Za-z]", cleaned):
        return False
    if any(mark in cleaned for mark in (":", "：", ";", "；", "/", "／", "D仍赐", "注意点", "可意译")):
        return False
    if cleaned.count("、") > 1:
        return False
    if cleaned.count(",") > 0 or cleaned.count("，") > 0:
        return False
    if re.search(r"[^\u4e00-\u9fff、（）()]", cleaned):
        return False
    cjk = "".join(re.findall(r"[\u4e00-\u9fff]", cleaned))
    if not cjk or len(cjk) > 8:
        return False
    return True


def extract_translation_probe(text: str) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    for pattern in (
        r"(?:句子\s*翻译为|翻译为)\s*[:：]\s*([^。！？]+)",
        r"(?:译[:：])\s*([^。！？]+)",
    ):
        match = re.search(pattern, cleaned)
        if match:
            return clean_text(match.group(1))
    return ""


def cjk_ngrams(text: str, size: int = 2) -> set[str]:
    chars = "".join(re.findall(r"[\u4e00-\u9fff]", clean_text(text)))
    if len(chars) < size:
        return {chars} if chars else set()
    return {chars[index : index + size] for index in range(len(chars) - size + 1)}


def score_clean_gloss(raw_gloss: str, cleaned_gloss: str) -> int:
    raw = clean_text(raw_gloss)
    gloss = clean_text(cleaned_gloss)
    if not gloss:
        return -1000
    score = 40
    if re.search(r"义同\s*[“\"'‘]?[\u4e00-\u9fff]{1,8}", raw):
        score = 120
    elif re.search(r"译为\s*[“\"'‘]?[\u4e00-\u9fff]{1,12}", raw):
        score = 110
    elif re.search(r"[“\"'‘][\u4e00-\u9fff]{1,12}[”\"'’]", raw):
        score = 80
    elif any(marker in raw for marker in (*GLOSS_BLOCK_MARKERS, "义同")):
        score -= 18
    score += min(len(re.findall(r"[\u4e00-\u9fff]", gloss)), 16)
    if any(punct in gloss for punct in ("，", ",", "、")):
        score += 3
    return score


def strip_inline_gloss(text: str, headword: str) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"\b([A-D])\s*(?=[\u4e00-\u9fff]{1,3}\s*[:：]\s*(?![“\"]))", "", cleaned)
    if headword:
        specific = rf"(?:\s|/|；|。|，|、)+{re.escape(headword)}\s*[:：]\s*(?![“\"])[^/；。！？]+[。！？]?"
        cleaned = re.sub(specific, "", cleaned)
    generic = r"(?:\s|/|；|。|，|、)+[\u4e00-\u9fff]{1,3}\s*[:：]\s*(?![“\"])[^/；。！？]+[。！？]?"
    cleaned = re.sub(generic, "", cleaned).strip(" /")
    return clean_text(cleaned)


def is_simple_headword(value: str) -> bool:
    return bool(re.fullmatch(r"[\u4e00-\u9fff]{1,3}", clean_text(value)))


def normalize_occurrence_headword(headword: str, excerpt: str) -> str:
    cleaned_headword = clean_text(headword)
    if len(cleaned_headword) <= 3:
        return cleaned_headword
    cleaned_excerpt = clean_text(excerpt)
    repeated_tail = re.search(rf"([\u4e00-\u9fff]){re.escape(cleaned_headword)}\s*[:：]\s*(?![“\"])", cleaned_excerpt)
    if repeated_tail and repeated_tail.group(1) == cleaned_headword[-1]:
        return repeated_tail.group(1)
    match = re.search(r"([\u4e00-\u9fff]{1,3})\s*[:：]\s*(?![“\"])", cleaned_excerpt)
    if match:
        return match.group(1)
    return cleaned_headword


def derive_canonical_content_headword(headword: str, occurrences: list[dict[str, Any]]) -> str:
    cleaned_headword = clean_text(headword)
    if is_simple_headword(cleaned_headword):
        return cleaned_headword
    candidates: Counter[str] = Counter()
    for occurrence in occurrences:
        excerpt = clean_text(str(occurrence.get("excerpt") or ""))
        repeated_tail = re.search(rf"([\u4e00-\u9fff]){re.escape(cleaned_headword)}\s*[:：]\s*(?![“\"])", excerpt)
        if repeated_tail and repeated_tail.group(1) == cleaned_headword[-1]:
            candidates[repeated_tail.group(1)] += 4
        match = re.search(r"([\u4e00-\u9fff]{1,3})\s*[:：]\s*(?![“\"])", excerpt)
        if match:
            candidates[match.group(1)] += 2
        for marked in re.findall(r"\*([\u4e00-\u9fff]{1,3})\*", str(occurrence.get("excerpt") or "")):
            candidates[marked] += 1
    if candidates:
        candidate, _score = candidates.most_common(1)[0]
        return candidate
    return cleaned_headword


def refine_content_headword_with_qdoc(
    raw_headword: str,
    candidate: str,
    occurrences: list[dict[str, Any]],
    question_docs: dict[str, dict[str, Any]] | None,
) -> str:
    cleaned_raw = clean_text(raw_headword)
    cleaned_candidate = clean_text(candidate)
    if not question_docs or len(cleaned_raw) <= 3:
        return cleaned_candidate
    if not any(":" in str(item.get("excerpt") or "") or "：" in str(item.get("excerpt") or "") for item in occurrences):
        return cleaned_candidate
    tail = cleaned_raw[-1]
    for occurrence in occurrences:
        qdoc_text = clean_text(str(question_docs.get(str(occurrence.get("paper_key") or ""), {}).get("text") or ""))
        if not qdoc_text:
            continue
        if tail:
            tail_context = find_sentence_context(qdoc_text, str(occurrence.get("excerpt") or ""), tail)
            if looks_like_sentence_context(tail_context, tail):
                return tail
        if cleaned_candidate in qdoc_text:
            return cleaned_candidate
    return cleaned_candidate


def merge_content_terms(
    raw_terms: list[dict[str, Any]],
    question_docs: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for term in raw_terms:
        canonical = derive_canonical_content_headword(str(term.get("headword") or ""), list(term.get("occurrences", [])))
        canonical = refine_content_headword_with_qdoc(
            str(term.get("headword") or ""),
            canonical,
            list(term.get("occurrences", [])),
            question_docs,
        )
        bucket = grouped.setdefault(
            canonical,
            {
                "headword": canonical,
                "display_headword": canonical,
                "occurrences": [],
                "_raw_headwords": set(),
            },
        )
        bucket["occurrences"].extend(term.get("occurrences", []))
        bucket["_raw_headwords"].add(clean_text(str(term.get("headword") or "")))

    merged: list[dict[str, Any]] = []
    for canonical, bucket in grouped.items():
        occurrences = sorted(
            bucket["occurrences"],
            key=lambda item: (
                int(item.get("year") or 0),
                str(item.get("paper_key") or ""),
                int(item.get("question_number") or 0),
                int(item.get("pair_index") or 0),
            ),
        )
        gloss_counter = Counter(
            clean_gloss(canonical, str(item.get("gloss") or ""), str(item.get("excerpt") or ""))
            for item in occurrences
            if clean_gloss(canonical, str(item.get("gloss") or ""), str(item.get("excerpt") or ""))
        )
        years = sorted(
            {
                int(item.get("year"))
                for item in occurrences
                if isinstance(item.get("year"), int)
            }
        )
        question_type_counts = Counter(
            str(item.get("question_subtype") or "")
            for item in occurrences
            if str(item.get("question_subtype") or "")
        )
        merged.append(
            {
                "headword": canonical,
                "display_headword": canonical,
                "occurrences": occurrences,
                "years": years,
                "total_occurrences": len(occurrences),
                "beijing_occurrences": sum(1 for item in occurrences if item.get("scope") == "beijing"),
                "national_occurrences": sum(1 for item in occurrences if item.get("scope") == "national"),
                "question_type_counts": dict(question_type_counts),
                "sample_glosses": [gloss for gloss, _count in gloss_counter.most_common(6)],
                "raw_headwords": sorted(bucket["_raw_headwords"]),
            }
        )
    return sorted(merged, key=lambda item: (-int(item["beijing_occurrences"]), item["headword"]))


def stable_slug(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "-", text)
    return text.strip("-") or hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_number(seed: str, modulo: int) -> int:
    if modulo <= 0:
        return 0
    return int(hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8], 16) % modulo


def answer_label_for_question(answer_text: str, question_number: int) -> str:
    match = re.search(rf"(?<!\d){question_number}\s*[\.．、]\s*([A-D])\b", answer_text or "")
    return match.group(1) if match else ""


def stable_pick(items: list[str], seed: str, count: int) -> list[str]:
    pool = [item for item in items if item]
    if len(pool) <= count:
        return pool
    start = int(hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8], 16) % len(pool)
    picked: list[str] = []
    for offset in range(len(pool)):
        candidate = pool[(start + offset) % len(pool)]
        if candidate in picked:
            continue
        picked.append(candidate)
        if len(picked) >= count:
            break
    return picked


def split_sentences(text: str) -> list[str]:
    prepared = clean_text(text)
    chunks = re.split(r"(?<=[。！？；!?])|(?<=/)", prepared)
    return [chunk.strip(" /") for chunk in chunks if chunk.strip(" /")]


def extract_source_passage(qdoc_text: str) -> str:
    raw = str(qdoc_text or "")
    if not raw:
        return ""
    for pattern in (
        r"\n\s*[\(（]\s*1\s*[\)）]",
        r"\n\s*1\s*[\.．、]",
        r"\n\s*第[一二三四五六七八九十]+题",
    ):
        match = re.search(pattern, raw)
        if match:
            return raw[: match.start()]
    return raw


def truncate_excerpt(text: str, limit: int = 200) -> str:
    cleaned = clean_text(text)
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 1] + "…"


def truncate_around_headword(text: str, headword: str, limit: int = 240) -> str:
    cleaned = clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    if headword and headword in cleaned:
        center = cleaned.find(headword)
        start = max(0, center - limit // 3)
        end = min(len(cleaned), start + limit - 1)
        start = max(0, end - (limit - 1))
        prefix = "…" if start > 0 else ""
        suffix = "…" if end < len(cleaned) else ""
        return prefix + cleaned[start:end] + suffix
    return truncate_excerpt(cleaned, limit)


def locate_single_section(text: str, title: str, window: int = 9000) -> str:
    normalized_targets = [normalize_title(item) for item in title_part_variants(title)]
    best_start = -1
    heading_matches = list(re.finditer(r"(?m)^#.*$", text))
    for index, match in enumerate(heading_matches):
        line = match.group(0)
        normalized_line = normalize_title(line)
        if any(target and target in normalized_line for target in normalized_targets):
            best_start = match.start()
            for next_match in heading_matches[index + 1 :]:
                if next_match.start() > best_start:
                    return text[best_start : next_match.start()]
            return text[best_start : best_start + window]
    if best_start < 0:
        normalized_text = normalize_title(text)
        for normalized_target in normalized_targets:
            pos = normalized_text.find(normalized_target)
            if pos >= 0:
                return text[max(0, pos - 400) : pos + window]
        return ""
    return text[best_start : best_start + window]


def locate_section(text: str, title: str, window: int = 9000) -> str:
    parts = split_title_parts(title)
    if len(parts) <= 1:
        return locate_single_section(text, title, window)
    sections: list[str] = []
    seen: set[str] = set()
    for part in parts:
        section = locate_single_section(text, part, window)
        signature = hash_text(clean_text(section))
        if section and signature not in seen:
            seen.add(signature)
            sections.append(section)
    if sections:
        return "\n\n".join(sections)
    return locate_single_section(text, title, window)


def build_classical_sections(manifest: dict[str, list[dict]], junior_md: str, senior_md: str) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for book_key, items in manifest.items():
        corpus = junior_md if "初中" in book_key else senior_md
        school_stage = "初中" if "初中" in book_key else "高中"
        for item in items:
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            section_text = locate_section(corpus, title)
            if not section_text:
                continue
            notes = []
            for line in section_text.splitlines():
                line = clean_text(line)
                if "〔" in line and "〕" in line:
                    notes.append(line)
            sections.append(
                {
                    "book_key": book_key,
                    "school_stage": school_stage,
                    "title": title,
                    "kind": str(item.get("kind") or ""),
                    "page_start": item.get("page_start"),
                    "page_end": item.get("page_end"),
                    "section_text": section_text,
                    "notes": notes,
                }
            )
    return sections


def looks_like_textbook_ref_sentence(sentence: str, headword: str, section_kind: str) -> bool:
    cleaned = clean_text(sentence)
    if not cleaned or headword not in cleaned:
        return False
    if cleaned.startswith("#") or re.search(r"[\\$#]", cleaned):
        return False
    blocked_markers = (
        "学习提示",
        "研习中",
        "比如",
        "这里",
        "意思是",
        "表示",
        "表现",
        "启示",
        "感叹",
        "注意",
        "典故",
        "典出",
        "经典",
        "古典",
    )
    if any(marker in cleaned for marker in blocked_markers):
        return False
    if len(headword) == 1 and any(marker in cleaned for marker in ("恩典", "典礼", "词典", "字典")):
        return False
    if section_kind not in {"古文", "古诗词"} and len(headword) == 1:
        return False
    return True


def build_textbook_refs(all_terms: list[dict[str, str]], sections: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    refs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for term in all_terms:
        term_id = term["term_id"]
        headword = term["headword"]
        for section in sections:
            text = section["section_text"]
            if headword not in text:
                continue
            sentences = []
            for sentence in split_sentences(text):
                if looks_like_textbook_ref_sentence(sentence, headword, str(section["kind"] or "")):
                    sentences.append(sentence)
            if not sentences:
                continue
            matched_notes = [note for note in section["notes"] if headword in note]
            refs[term_id].append(
                {
                    "ref_id": f"{term_id}:{stable_slug(section['title'])}",
                    "school_stage": section["school_stage"],
                    "book_key": section["book_key"],
                    "title": section["title"],
                    "kind": section["kind"],
                    "page_start": section["page_start"],
                    "page_end": section["page_end"],
                    "sentence": truncate_excerpt(sentences[0], 140),
                    "context_window": [truncate_excerpt(item, 140) for item in sentences[:3]],
                    "note_block": " ".join(matched_notes[:4]),
                }
            )
    return {key: value[:8] for key, value in refs.items()}


def load_question_templates() -> dict[str, dict[str, Any]]:
    templates: dict[str, dict[str, Any]] = {}
    for path in sorted(QUESTION_TEMPLATES_DIR.glob("*.json")):
        templates[path.stem] = load_json(path)
    return templates


def merge_question_docs(xuci: dict[str, Any], shici: dict[str, Any]) -> dict[str, dict[str, Any]]:
    merged = dict(xuci.get("question_docs", {}))
    merged.update(shici.get("question_docs", {}))
    return merged


def infer_basis_records(raw_occurrence: dict[str, Any], headword: str) -> list[dict[str, Any]]:
    base_type = QUESTION_TYPE_TO_BASIS.get(str(raw_occurrence.get("question_subtype") or ""), "direct_choice")
    evidence = truncate_excerpt(str(raw_occurrence.get("excerpt") or ""))
    year = raw_occurrence.get("year")
    question_number = raw_occurrence.get("question_number")
    base_record = {
        "basis_type": base_type,
        "exam_year": year,
        "question_number": question_number,
        "evidence_sentence": evidence,
        "answer_span": truncate_excerpt(str(raw_occurrence.get("gloss") or raw_occurrence.get("option_label") or "")),
        "why_required": f"{year} 年真题中直接出现 {headword} 的考查证据。",
        "confidence": 0.92 if base_type == "direct_choice" else 0.82,
        "needs_manual_review": False,
    }
    inferred: list[dict[str, Any]] = [base_record]
    if raw_occurrence.get("scope") == "beijing":
        for basis_type in ("sentence_meaning", "passage_meaning"):
            inferred.append(
                {
                    "basis_type": basis_type,
                    "exam_year": year,
                    "question_number": question_number,
                    "evidence_sentence": evidence,
                    "answer_span": "",
                    "why_required": f"要拿下 {year} 年北京卷古文阅读相关题，需稳定掌握 {headword} 在语境中的作用。",
                    "confidence": 0.58,
                    "needs_manual_review": True,
                }
            )
    return inferred


def select_clean_glosses(headword: str, occurrences: list[dict[str, Any]]) -> list[str]:
    ordered: list[tuple[int, str]] = []
    seen: set[str] = set()
    for occurrence in occurrences:
        raw_gloss = str(occurrence.get("gloss") or "")
        cleaned = clean_gloss(headword, raw_gloss, str(occurrence.get("excerpt") or ""))
        if not looks_like_clean_gloss(cleaned):
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append((score_clean_gloss(raw_gloss, cleaned), cleaned))
    ordered.sort(key=lambda item: (-item[0], len(item[1]), item[1]))
    return [item[1] for item in ordered]


def choose_gloss_distractors(
    same_term_glosses: list[str],
    all_glosses: list[str],
    correct_gloss: str,
    seed: str,
) -> list[str]:
    def valid(candidate: str) -> bool:
        cleaned = clean_text(candidate)
        if not looks_like_clean_gloss(cleaned):
            return False
        if cleaned == correct_gloss:
            return False
        if cleaned in BANNED_GLOSS_CANDIDATES:
            return False
        if cleaned in correct_gloss or correct_gloss in cleaned:
            return False
        return True

    preferred = [item for item in same_term_glosses if valid(item)]
    fallback = [item for item in all_glosses if valid(item) and item not in preferred]
    picked = stable_pick(preferred, seed, 3)
    if len(picked) < 3:
        picked.extend(stable_pick(fallback, seed + ":fallback", 3 - len(picked)))
    return unique_clean_strings(picked)


def unique_clean_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(clean_text(value) for value in values if clean_text(value)))


def priority_level_for_content_term(textbook_refs: list[dict[str, Any]], total_occurrences: int) -> str:
    has_senior_note = any(
        str(ref.get("school_stage") or "") == "高中" and clean_text(str(ref.get("note_block") or ""))
        for ref in textbook_refs
    )
    if has_senior_note and total_occurrences > 0:
        return CONTENT_PRIORITY_CORE
    return CONTENT_PRIORITY_SECONDARY


def priority_level_for_function_term(textbook_refs: list[dict[str, Any]], total_occurrences: int) -> str:
    has_textbook_note = any(clean_text(str(ref.get("note_block") or "")) for ref in textbook_refs)
    if has_textbook_note and total_occurrences > 0:
        return CONTENT_PRIORITY_CORE
    return FUNCTION_PRIORITY_SUPPORT


def first_support_snippet(term_record: dict[str, Any]) -> dict[str, str]:
    dict_summary = ""
    textbook_note = ""
    textbook_sentence = ""
    if term_record.get("dict_refs"):
        dict_summary = truncate_excerpt(str(term_record["dict_refs"][0].get("summary") or ""), 90)
    if term_record.get("textbook_refs"):
        textbook_note = truncate_excerpt(str(term_record["textbook_refs"][0].get("note_block") or ""), 84)
        textbook_sentence = truncate_excerpt(str(term_record["textbook_refs"][0].get("sentence") or ""), 72)
    return {
        "dict_summary": dict_summary,
        "textbook_note": textbook_note,
        "textbook_sentence": textbook_sentence,
    }


def format_usage_profile(profile: dict[str, str]) -> str:
    parts = [
        clean_text(profile.get("part_of_speech") or ""),
        clean_text(profile.get("semantic_value") or ""),
        clean_text(profile.get("syntactic_function") or ""),
    ]
    relation = clean_text(profile.get("relation") or "")
    if relation:
        parts.append(f"关系偏向{relation}")
    return "，".join(part for part in parts if part)


def format_function_explanation(headword: str, profile: dict[str, str], support: dict[str, str]) -> str:
    pieces = [f"“{headword}”常见考法可概括为：{format_usage_profile(profile)}。"]
    if support["dict_summary"]:
        pieces.append(f"辞典关联可参照：{support['dict_summary']}。")
    elif support["textbook_note"]:
        pieces.append(f"教材注释可参照：{support['textbook_note']}。")
    return " ".join(piece for piece in pieces if piece)


def format_content_explanation(
    headword: str,
    correct_gloss: str,
    support: dict[str, str],
    source_label: str,
) -> str:
    pieces = [f"“{headword}”在这里应解释为“{correct_gloss}”。"]
    if support["dict_summary"]:
        pieces.append(f"辞典可对到：{support['dict_summary']}。")
    if support["textbook_note"]:
        pieces.append(f"教材注释可参照：{support['textbook_note']}。")
    pieces.append(f"依据题源：{source_label}。")
    return " ".join(piece for piece in pieces if piece)


def build_content_option_analysis(
    headword: str,
    correct_gloss: str,
    option_text: str,
    is_correct: bool,
    support: dict[str, str],
) -> str:
    if is_correct:
        return format_content_explanation(headword, correct_gloss, support, "本题标准答案")
    support_text = f"；辞典和题源都更支持“{correct_gloss}”" if support["dict_summary"] else f"；题源应落实为“{correct_gloss}”"
    return f"若释为“{option_text}”，则与本句语境不合{support_text}。"


def build_function_option_analysis(
    stem: str,
    option_label: str,
    option_text: str,
    correct_label: str,
    explanation: str,
) -> str:
    if option_label == correct_label:
        return explanation
    if "不同" in stem:
        return f"本项不符合题干要求的“不同”判断，标准答案不是 {option_label}。"
    return f"本项不符合题干要求的“相同”判断，标准答案不是 {option_label}。"


def find_passage(qdoc_text: str, excerpt: str) -> str:
    return truncate_excerpt(qdoc_text, 240)


def looks_like_sentence_context(text: str, headword: str) -> bool:
    cleaned = clean_text(text)
    if not cleaned or len(cleaned) < 6:
        return False
    if cleaned.startswith(("阅读下面", "文言文阅读", "(一)文言文阅读", "二、本大题", "下列对", "根据文意")):
        return False
    if "阅读下面的文言文,完成" in cleaned[:40]:
        return False
    if re.match(r"^\d+\s*[\.．、]", cleaned) and any(marker in cleaned for marker in ("下列", "根据文意", "完成下面")):
        return False
    if headword and headword not in cleaned:
        return False
    if re.search(r"[\u4e00-\u9fff]{1,3}\s*[:：]\s*(?![“\"])", cleaned):
        return False
    if any(marker in cleaned for marker in (*GLOSS_BLOCK_MARKERS, "义同")):
        return False
    if re.search(r"^[12]\s*[“\"'‘]", cleaned):
        return False
    if re.search(r"[“\"'‘][^”\"'’]{1,8}[”\"'’]\s*[,:：，]", cleaned):
        return False
    return True


def looks_like_passage_context(text: str, headword: str) -> bool:
    cleaned = clean_text(text)
    if not cleaned or len(cleaned) < 6:
        return False
    if headword and headword not in cleaned:
        return False
    if cleaned.startswith(("阅读下面", "下列对", "根据文意", "将下面的句子译为")):
        return False
    if re.match(r"^\d+\s*[\.．、]", cleaned) and any(marker in cleaned for marker in ("下列对", "根据文意", "完成下面")):
        return False
    if any(marker in cleaned for marker in ("参考答案", "答案示例")):
        return False
    return True


def extract_marked_compare_sentences(raw_excerpt: str, headword: str) -> list[str]:
    raw = str(raw_excerpt or "")
    if not raw or not headword:
        return []
    marked_pattern = re.escape(f"*{headword}*")
    matches = list(re.finditer(marked_pattern, raw))
    if len(matches) < 2:
        if "/" in raw:
            return [clean_text(part.replace("*", "")) for part in raw.split("/") if clean_text(part.replace("*", ""))]
        return []
    sentences: list[str] = []
    sentence_markers = "。！？；/\n"
    for index, match in enumerate(matches[:2]):
        prev_end = matches[index - 1].end() if index > 0 else 0
        next_start = matches[index + 1].start() if index + 1 < len(matches) else -1

        previous_punctuation = max((raw.rfind(marker, prev_end, match.start()) for marker in sentence_markers), default=-1)
        if previous_punctuation >= prev_end:
            left = previous_punctuation + 1
        else:
            previous_space = raw.rfind(" ", prev_end, match.start())
            left = previous_space + 1 if previous_space >= prev_end else 0

        next_punctuation_candidates = [raw.find(marker, match.end()) for marker in sentence_markers if raw.find(marker, match.end()) != -1]
        next_punctuation = min(next_punctuation_candidates) if next_punctuation_candidates else -1
        right = next_punctuation + 1 if next_punctuation >= 0 else len(raw)
        if next_start >= 0 and (next_punctuation < 0 or next_start < next_punctuation):
            between = raw[match.end():next_start]
            if not any(marker in between for marker in sentence_markers):
                next_space = raw.find(" ", match.end(), next_start)
                right = next_space if next_space != -1 else next_start

        sentence = clean_text(raw[left:right].replace("*", ""))
        if sentence and sentence not in sentences:
            sentences.append(sentence)
    return sentences


def extract_function_option_sentences(entries: list[dict[str, Any]]) -> list[str]:
    if not entries:
        return []
    headword = str(entries[0].get("headword") or "")
    raw_candidates = []
    for item in entries:
        raw_excerpt = str(item.get("excerpt") or "")
        if raw_excerpt and raw_excerpt not in raw_candidates:
            raw_candidates.append(raw_excerpt)
    for raw_excerpt in raw_candidates:
        marked = extract_marked_compare_sentences(raw_excerpt, headword)
        if len(marked) >= 2:
            return [truncate_excerpt(marked[0], 80), truncate_excerpt(marked[1], 80)]

    sentences = [strip_inline_gloss(str(item.get("excerpt") or ""), headword) for item in entries]
    if (len(sentences) == 1 or len(set(sentences)) == 1) and sentences:
        if "/" in sentences[0]:
            sentences = [part.strip() for part in sentences[0].split("/") if part.strip()]
        else:
            sentences = split_sentences(sentences[0])
    cleaned = [truncate_excerpt(clean_text(sentence), 80) for sentence in sentences if clean_text(sentence)]
    return cleaned[:2]


def find_sentence_context(qdoc_text: str, excerpt: str, headword: str) -> str:
    source_text = extract_source_passage(qdoc_text)
    sanitized = strip_inline_gloss(excerpt, headword)
    translation_probe = extract_translation_probe(excerpt)
    if looks_like_sentence_context(sanitized, headword) and not translation_probe:
        return truncate_excerpt(sanitized, 120)
    candidates = [
        clean_text(sentence)
        for sentence in split_sentences(source_text)
        if headword and headword in sentence
    ]
    candidates = [
        sentence
        for sentence in candidates
        if not any(marker in sentence for marker in ("阅读下面", "下列", "参考答案", "答案示例", "完成下面", "本大题共"))
    ]
    probe_tokens = [
        token
        for token in re.split(r"[，,。！？；:：\s]+", sanitized)
        if token and token != headword
    ]
    probe_ngrams = cjk_ngrams(translation_probe or sanitized, 2)
    best_sentence = ""
    best_score = -1
    for sentence in candidates:
        score = sum(1 for token in probe_tokens[:4] if token in sentence)
        if translation_probe:
            score += len(probe_ngrams & cjk_ngrams(sentence, 2))
        if sanitized and sanitized[:4] and sanitized[:4] in sentence:
            score += 2
        if score > best_score:
            best_sentence = sentence
            best_score = score
    if best_sentence:
        return truncate_excerpt(best_sentence, 120)
    return truncate_excerpt(sanitized, 120)


def find_passage(qdoc_text: str, excerpt: str, headword: str) -> str:
    source_text = extract_source_passage(qdoc_text)
    probe = clean_text(excerpt).split("/")[0].strip()
    paragraphs = [
        clean_text(paragraph)
        for paragraph in re.split(r"\n{2,}", source_text)
        if clean_text(paragraph)
    ]
    passage_candidates = [paragraph for paragraph in paragraphs if looks_like_passage_context(paragraph, headword)]
    if probe:
        for paragraph in passage_candidates:
            if probe[:8] and probe[:8] in paragraph:
                return truncate_around_headword(paragraph, headword, 240)

    probe_tokens = [
        token
        for token in re.split(r"[，,。！？；:：\s]+", probe)
        if token and token != headword
    ]
    best_paragraph = ""
    best_score = -1
    for paragraph in passage_candidates:
        score = 0
        if headword and headword in paragraph:
            score += 2
        score += sum(1 for token in probe_tokens[:4] if token in paragraph)
        if score > best_score:
            best_paragraph = paragraph
            best_score = score
    if best_paragraph:
        return truncate_around_headword(best_paragraph, headword, 240)

    nearby_sentences = [sentence for sentence in split_sentences(source_text) if looks_like_sentence_context(sentence, headword)]
    if nearby_sentences:
        return truncate_around_headword(" ".join(nearby_sentences[:3]), headword, 240)
    return truncate_around_headword(probe or clean_text(excerpt), headword, 240)


def build_function_question_bank(
    function_terms: list[dict[str, Any]],
    question_docs: dict[str, dict[str, Any]],
    function_records: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    support_by_term_id = {record["term_id"]: record for record in function_records}
    banks: dict[str, list[dict[str, Any]]] = {
        "xuci_pair_compare": [],
        "function_gloss": [],
        "function_profile": [],
    }
    answer_keys: dict[str, dict[str, Any]] = {}
    grouped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for term in function_terms:
        term_id = f"function::{term['headword']}"
        for occurrence in term.get("occurrences", []):
            if occurrence.get("scope") != "beijing":
                continue
            subtype = str(occurrence.get("question_subtype") or "")
            if subtype not in {"xuci_compare_same", "xuci_compare_diff"}:
                continue
            question_number = int(occurrence.get("question_number") or 0)
            paper_key = str(occurrence.get("paper_key") or "")
            option_label = str(occurrence.get("option_label") or "").strip().upper()
            key = (paper_key, question_number, subtype)
            group = grouped.setdefault(
                key,
                {
                    "question_type": "xuci_pair_compare",
                    "kind": "function_word",
                    "paper_key": paper_key,
                    "year": occurrence.get("year"),
                    "paper": occurrence.get("paper"),
                    "question_number": question_number,
                    "question_subtype": subtype,
                    "options": defaultdict(list),
                    "term_ids": set(),
                },
            )
            group["options"][option_label].append({**occurrence, "term_id": term_id})
            group["term_ids"].add(term_id)

    for group in grouped.values():
        qdoc = question_docs.get(group["paper_key"], {})
        answer_label = answer_label_for_question(str(qdoc.get("answer") or ""), int(group["question_number"]))
        if answer_label not in {"A", "B", "C", "D"}:
            continue
        options = []
        complete = True
        for label in ("A", "B", "C", "D"):
            entries = group["options"].get(label, [])
            if not entries:
                complete = False
                break
            ordered = sorted(entries, key=lambda item: int(item.get("pair_index") or 0))
            sentences = extract_function_option_sentences(ordered)
            if len(sentences) < 2:
                complete = False
                break
            options.append(
                {
                    "label": label,
                    "term_id": ordered[0]["term_id"],
                    "headword": ordered[0]["headword"],
                    "sentences": [truncate_excerpt(sentences[0], 80), truncate_excerpt(sentences[1], 80)],
                    "usage_profile": FUNCTION_WORD_PROFILES.get(ordered[0]["headword"], []),
                }
            )
        if not complete:
            continue
        stem = "下列各组句子中，加点虚词的意义和用法相同的一项是" if group["question_subtype"] == "xuci_compare_same" else "下列各组句子中，加点虚词的意义和用法不同的一项是"
        challenge_id = f"xuci-{stable_slug(group['paper_key'])}-q{group['question_number']}"
        banks["xuci_pair_compare"].append(
            {
                "challenge_id": challenge_id,
                "question_type": "xuci_pair_compare",
                "kind": "function_word",
                "term_id": options[0]["term_id"],
                "term_ids": sorted(group["term_ids"]),
                "priority_level": CONTENT_PRIORITY_CORE,
                "paper_key": group["paper_key"],
                "year": group["year"],
                "paper": group["paper"],
                "question_number": group["question_number"],
                "stem": stem,
                "options": options,
            }
        )
        correct_option = next((option for option in options if option["label"] == answer_label), options[0])
        support = first_support_snippet(support_by_term_id.get(str(correct_option["term_id"]) or "", {}))
        explanation = f"依据 {group['year']} 年 {group['paper']} 第 {group['question_number']} 题，正确答案为 {answer_label}。"
        if support["dict_summary"]:
            explanation += f" 可参照辞典：{support['dict_summary']}。"
        answer_keys[challenge_id] = {
            "challenge_id": challenge_id,
            "kind": "function_word",
            "question_type": "xuci_pair_compare",
            "term_id": correct_option["term_id"],
            "term_ids": sorted(group["term_ids"]),
            "priority_level": CONTENT_PRIORITY_CORE,
            "source_type": "gaokao_beijing_pair_compare",
            "source_ref": {
                "year": group["year"],
                "paper": group["paper"],
                "paper_key": group["paper_key"],
                "question_number": group["question_number"],
            },
            "correct_label": answer_label,
            "correct_text": correct_option["headword"],
            "explanation": explanation,
            "dict_support": support_by_term_id.get(str(correct_option["term_id"]) or "", {}).get("dict_refs", [])[:2],
            "textbook_support": support_by_term_id.get(str(correct_option["term_id"]) or "", {}).get("textbook_refs", [])[:2],
            "option_analyses": [
                {
                    "label": option["label"],
                    "text": option["headword"],
                    "is_correct": option["label"] == answer_label,
                    "analysis": build_function_option_analysis(stem, option["label"], option["headword"], answer_label, explanation),
                }
                for option in options
            ],
        }

    gloss_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for term in function_terms:
        for occurrence in term.get("occurrences", []):
            if str(occurrence.get("question_subtype") or "") != "xuci_explanation":
                continue
            paper_key = str(occurrence.get("paper_key") or "")
            question_number = int(occurrence.get("question_number") or 0)
            if not paper_key or not question_number:
                continue
            gloss_groups[(paper_key, question_number)].append({**occurrence, "headword": term["headword"], "term_id": f"function::{term['headword']}"})

    for (paper_key, question_number), entries in gloss_groups.items():
        labels = {str(entry.get("option_label") or "").strip().upper() for entry in entries if str(entry.get("option_label") or "").strip()}
        if labels != {"A", "B", "C", "D"}:
            continue
        qdoc = question_docs.get(paper_key, {})
        answer_label = answer_label_for_question(str(qdoc.get("answer") or ""), question_number)
        if answer_label not in {"A", "B", "C", "D"}:
            continue
        ordered_entries = sorted(entries, key=lambda item: str(item.get("option_label") or ""))
        term_id = str(ordered_entries[0]["term_id"])
        headword = str(ordered_entries[0]["headword"])
        challenge_id = f"function-gloss-{stable_slug(paper_key)}-{question_number}"
        options = []
        for entry in ordered_entries:
            excerpt = strip_inline_gloss(str(entry.get("excerpt") or ""), headword)
            gloss = clean_gloss(headword, str(entry.get("gloss") or ""), str(entry.get("excerpt") or ""))
            if not excerpt or not gloss:
                options = []
                break
            options.append(
                {
                    "label": str(entry.get("option_label") or "").strip().upper(),
                    "text": f"{truncate_excerpt(excerpt, 52)}：{gloss}",
                }
            )
        if len(options) != 4:
            continue
        support_record = support_by_term_id.get(term_id, {})
        support = first_support_snippet(support_record)
        explanation = f"依据 {qdoc.get('year')} 年 {qdoc.get('paper')} 第 {question_number} 题，正确答案为 {answer_label}。"
        if support["dict_summary"]:
            explanation += f" 相关辞典可参照：{support['dict_summary']}。"
        banks["function_gloss"].append(
            {
                "challenge_id": challenge_id,
                "question_type": "function_gloss",
                "kind": "function_word",
                "term_id": term_id,
                "term_ids": [term_id],
                "priority_level": CONTENT_PRIORITY_CORE,
                "paper_key": paper_key,
                "year": qdoc.get("year"),
                "paper": qdoc.get("paper"),
                "question_number": question_number,
                "stem": f"下列对句中“{headword}”的解释，最恰当的一项是",
                "options": options,
            }
        )
        answer_keys[challenge_id] = {
            "challenge_id": challenge_id,
            "kind": "function_word",
            "question_type": "function_gloss",
            "term_id": term_id,
            "term_ids": [term_id],
            "priority_level": CONTENT_PRIORITY_CORE,
            "source_type": "gaokao_beijing_direct_choice",
            "source_ref": {
                "year": qdoc.get("year"),
                "paper": qdoc.get("paper"),
                "paper_key": paper_key,
                "question_number": question_number,
            },
            "correct_label": answer_label,
            "correct_text": next((option["text"] for option in options if option["label"] == answer_label), ""),
            "explanation": explanation,
            "dict_support": support_record.get("dict_refs", [])[:2],
            "textbook_support": support_record.get("textbook_refs", [])[:2],
            "option_analyses": [
                {
                    "label": option["label"],
                    "text": option["text"],
                    "is_correct": option["label"] == answer_label,
                    "analysis": build_function_option_analysis(
                        f"下列对句中“{headword}”的解释，最恰当的一项是",
                        option["label"],
                        option["text"],
                        answer_label,
                        explanation,
                    ),
                }
                for option in options
            ],
        }

    all_profile_texts = unique_clean_strings(
        [
            format_usage_profile(profile)
            for record in function_records
            for profile in record.get("usage_relations", [])
            if format_usage_profile(profile)
        ]
    )
    for record in function_records:
        profiles = [profile for profile in record.get("usage_relations", []) if format_usage_profile(profile)]
        if not profiles:
            continue
        term_id = str(record["term_id"])
        headword = str(record["headword"])
        seed = f"profile:{term_id}"
        profile_index = stable_number(seed, len(profiles))
        correct_profile = profiles[profile_index]
        correct_text = format_usage_profile(correct_profile)
        distractors = [item for item in all_profile_texts if item != correct_text]
        picked = stable_pick(distractors, seed, 3)
        if len(picked) < 3:
            continue
        option_texts = stable_pick([correct_text, *picked], seed + ":options", 4)
        option_labels = ["A", "B", "C", "D"]
        correct_label = option_labels[option_texts.index(correct_text)]
        support = first_support_snippet(record)
        challenge_id = f"function-profile-{stable_slug(term_id)}"
        example_sentence = clean_text(str(record.get("textbook_refs", [{}])[0].get("sentence") or ""))
        if not example_sentence:
            raw_term = next((item for item in function_terms if f"function::{item['headword']}" == term_id), None)
            if raw_term and raw_term.get("occurrences"):
                raw_occurrence = raw_term["occurrences"][0]
                example_sentence = strip_inline_gloss(str(raw_occurrence.get("excerpt") or ""), headword)
        explanation = format_function_explanation(headword, correct_profile, support)
        banks["function_profile"].append(
            {
                "challenge_id": challenge_id,
                "question_type": "function_profile",
                "kind": "function_word",
                "term_id": term_id,
                "term_ids": [term_id],
                "priority_level": str(record.get("priority_level") or FUNCTION_PRIORITY_SUPPORT),
                "stem": f"关于虚词“{headword}”的常见意义和用法概括，最稳妥的一项是",
                "sentence": truncate_excerpt(example_sentence, 96) if example_sentence else "",
                "options": [{"label": label, "text": text} for label, text in zip(option_labels, option_texts)],
            }
        )
        answer_keys[challenge_id] = {
            "challenge_id": challenge_id,
            "kind": "function_word",
            "question_type": "function_profile",
            "term_id": term_id,
            "term_ids": [term_id],
            "priority_level": str(record.get("priority_level") or FUNCTION_PRIORITY_SUPPORT),
            "source_type": "derived_profile",
            "source_ref": {
                "year": record.get("year_range", [None, None])[1],
                "paper": "",
                "paper_key": "",
                "question_number": None,
            },
            "correct_label": correct_label,
            "correct_text": correct_text,
            "explanation": explanation,
            "dict_support": record.get("dict_refs", [])[:2],
            "textbook_support": record.get("textbook_refs", [])[:2],
            "option_analyses": [
                {
                    "label": label,
                    "text": text,
                    "is_correct": label == correct_label,
                    "analysis": explanation if label == correct_label else f"这项概括不是“{headword}”在高考中最稳妥的常见考法。",
                }
                for label, text in zip(option_labels, option_texts)
            ],
        }

    for question_type in list(banks):
        banks[question_type] = sorted(
            banks[question_type],
            key=lambda item: (
                0 if str(item.get("priority_level") or "") == CONTENT_PRIORITY_CORE else 1,
                int(item.get("year") or 0),
                str(item.get("challenge_id") or ""),
            ),
        )
    return banks, answer_keys


def build_content_question_bank(
    content_terms: list[dict[str, Any]],
    question_docs: dict[str, dict[str, Any]],
    content_records: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    all_glosses = sorted(
        {
            gloss
            for term in content_terms
            for gloss in select_clean_glosses(str(term.get("headword") or ""), list(term.get("occurrences", [])))
        }
    )
    record_by_term_id = {record["term_id"]: record for record in content_records}
    bank: dict[str, list[dict[str, Any]]] = {
        "content_gloss": [],
        "sentence_meaning": [],
        "passage_meaning": [],
    }
    answer_keys: dict[str, dict[str, Any]] = {}
    option_labels = ["A", "B", "C", "D"]

    for term in content_terms:
        term_id = f"content::{term['headword']}"
        term_record = record_by_term_id.get(term_id, {})
        support = first_support_snippet(term_record)
        raw_headword = str(term["headword"])
        same_term_glosses = select_clean_glosses(raw_headword, list(term.get("occurrences", [])))
        selected_occurrences: dict[tuple[str, str, int, str], dict[str, Any]] = {}
        passage_candidates: dict[tuple[str, str, int, str], dict[str, Any]] = {}
        for index, occurrence in enumerate(term.get("occurrences", [])):
            gloss = clean_gloss(raw_headword, str(occurrence.get("gloss") or ""), str(occurrence.get("excerpt") or ""))
            paper_key = str(occurrence.get("paper_key") or "")
            qdoc = question_docs.get(paper_key, {})
            headword = normalize_occurrence_headword(raw_headword, str(occurrence.get("excerpt") or ""))
            excerpt = find_sentence_context(str(qdoc.get("text") or ""), str(occurrence.get("excerpt") or ""), headword)
            if not looks_like_clean_gloss(gloss) or not excerpt or not looks_like_sentence_context(excerpt, headword):
                continue
            context_key = (
                term_id,
                paper_key,
                int(occurrence.get("question_number") or 0),
                truncate_excerpt(excerpt, 120),
            )
            candidate = {
                "occurrence": occurrence,
                "paper_key": paper_key,
                "qdoc": qdoc,
                "headword": headword,
                "excerpt": truncate_excerpt(excerpt, 120),
                "gloss": gloss,
                "score": score_clean_gloss(str(occurrence.get("gloss") or ""), gloss),
                "index": index,
            }
            existing = selected_occurrences.get(context_key)
            if existing and (
                existing["score"] > candidate["score"]
                or (
                    existing["score"] == candidate["score"]
                    and len(str(existing["gloss"])) >= len(str(candidate["gloss"]))
                )
            ):
                continue
            selected_occurrences[context_key] = candidate

        ordered_occurrences = sorted(
            selected_occurrences.values(),
            key=lambda item: (
                int(item["occurrence"].get("year") or 0),
                str(item["paper_key"]),
                int(item["occurrence"].get("question_number") or 0),
                int(item["index"]),
            ),
        )
        for candidate in ordered_occurrences:
            occurrence = candidate["occurrence"]
            paper_key = candidate["paper_key"]
            qdoc = candidate["qdoc"]
            headword = candidate["headword"]
            excerpt = candidate["excerpt"]
            gloss = candidate["gloss"]
            seed = f"{term_id}:{paper_key}:{occurrence.get('question_number')}:{hash_text(excerpt)[:10]}"
            distractors = choose_gloss_distractors(same_term_glosses, all_glosses, gloss, seed)
            if len(distractors) < 3:
                continue
            gloss_options = stable_pick([gloss, *distractors], seed + ":gloss", 4)
            correct_label = option_labels[gloss_options.index(gloss)]
            source_label = f"{occurrence.get('year')} 年 {occurrence.get('paper')} 第 {occurrence.get('question_number')} 题"

            base_meta = {
                "term_id": term_id,
                "headword": headword,
                "priority_level": str(term_record.get("priority_level") or CONTENT_PRIORITY_SECONDARY),
                "paper_key": paper_key,
                "year": occurrence.get("year"),
                "paper": occurrence.get("paper"),
                "question_number": occurrence.get("question_number"),
                "evidence_excerpt": truncate_excerpt(excerpt, 120),
            }

            gloss_challenge_id = f"content-{stable_slug(seed)}"
            bank["content_gloss"].append(
                {
                    "challenge_id": gloss_challenge_id,
                    "question_type": "content_gloss",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"下列对句中“{headword}”的解释，最恰当的一项是",
                    "sentence": truncate_excerpt(excerpt, 120),
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, gloss_options)],
                }
            )
            answer_keys[gloss_challenge_id] = {
                "challenge_id": gloss_challenge_id,
                "kind": "content_word",
                "question_type": "content_gloss",
                "term_id": term_id,
                "term_ids": [term_id],
                "priority_level": str(term_record.get("priority_level") or CONTENT_PRIORITY_SECONDARY),
                "source_type": "gaokao_term",
                "source_ref": {
                    "year": occurrence.get("year"),
                    "paper": occurrence.get("paper"),
                    "paper_key": paper_key,
                    "question_number": occurrence.get("question_number"),
                },
                "correct_label": correct_label,
                "correct_text": gloss,
                "explanation": format_content_explanation(headword, gloss, support, source_label),
                "dict_support": term_record.get("dict_refs", [])[:2],
                "textbook_support": term_record.get("textbook_refs", [])[:2],
                "option_analyses": [
                    {
                        "label": label,
                        "text": option,
                        "is_correct": label == correct_label,
                        "analysis": build_content_option_analysis(headword, gloss, option, label == correct_label, support),
                    }
                    for label, option in zip(option_labels, gloss_options)
                ],
            }

            meaning_options = [
                f"把“{headword}”理解为“{option}”，整句意思才会随之落定。"
                for option in gloss_options
            ]
            sentence_challenge_id = f"sentence-{stable_slug(seed)}"
            bank["sentence_meaning"].append(
                {
                    "challenge_id": sentence_challenge_id,
                    "question_type": "sentence_meaning",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"结合语境，哪一项最能说明句中“{headword}”对句意的影响？",
                    "sentence": truncate_excerpt(excerpt, 120),
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, meaning_options)],
                }
            )
            answer_keys[sentence_challenge_id] = {
                "challenge_id": sentence_challenge_id,
                "kind": "content_word",
                "question_type": "sentence_meaning",
                "term_id": term_id,
                "term_ids": [term_id],
                "priority_level": str(term_record.get("priority_level") or CONTENT_PRIORITY_SECONDARY),
                "source_type": "gaokao_term",
                "source_ref": {
                    "year": occurrence.get("year"),
                    "paper": occurrence.get("paper"),
                    "paper_key": paper_key,
                    "question_number": occurrence.get("question_number"),
                },
                "correct_label": correct_label,
                "correct_text": meaning_options[gloss_options.index(gloss)],
                "explanation": format_content_explanation(headword, gloss, support, source_label),
                "dict_support": term_record.get("dict_refs", [])[:2],
                "textbook_support": term_record.get("textbook_refs", [])[:2],
                "option_analyses": [
                    {
                        "label": label,
                        "text": option,
                        "is_correct": label == correct_label,
                        "analysis": build_content_option_analysis(headword, gloss, gloss_option, label == correct_label, support),
                    }
                    for label, option, gloss_option in zip(option_labels, meaning_options, gloss_options)
                ],
            }

            passage = find_passage(str(qdoc.get("text") or ""), excerpt, headword)
            if passage and passage.count(headword) <= 1:
                passage_key = (
                    term_id,
                    paper_key,
                    int(occurrence.get("question_number") or 0),
                    passage,
                )
                existing_passage = passage_candidates.get(passage_key)
                passage_candidate = {
                    "score": candidate["score"],
                    "seed": seed,
                    "base_meta": base_meta,
                    "headword": headword,
                    "gloss": gloss,
                    "passage": passage,
                    "gloss_options": gloss_options,
                        "correct_label": correct_label,
                }
                if existing_passage and (
                    existing_passage["score"] > passage_candidate["score"]
                    or (
                        existing_passage["score"] == passage_candidate["score"]
                        and len(str(existing_passage["gloss"])) >= len(str(passage_candidate["gloss"]))
                    )
                ):
                    pass
                else:
                    passage_candidates[passage_key] = passage_candidate
        for passage_candidate in passage_candidates.values():
            headword = str(passage_candidate["headword"])
            gloss = str(passage_candidate["gloss"])
            gloss_options = list(passage_candidate["gloss_options"])
            correct_label = str(passage_candidate["correct_label"])
            passage_options = [
                f"整段里“{headword}”若理解为“{option}”，文意判断才会随之成立。"
                for option in gloss_options
            ]
            passage_challenge_id = f"passage-{stable_slug(str(passage_candidate['seed']))}"
            bank["passage_meaning"].append(
                {
                    "challenge_id": passage_challenge_id,
                    "question_type": "passage_meaning",
                    "kind": "content_word",
                    **passage_candidate["base_meta"],
                    "stem": f"结合整段语境，哪一项对“{headword}”的理解最稳妥？",
                    "passage": passage_candidate["passage"],
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, passage_options)],
                }
            )
            answer_keys[passage_challenge_id] = {
                "challenge_id": passage_challenge_id,
                "kind": "content_word",
                "question_type": "passage_meaning",
                "term_id": str(passage_candidate["base_meta"]["term_id"]),
                "term_ids": [str(passage_candidate["base_meta"]["term_id"])],
                "priority_level": str(passage_candidate["base_meta"]["priority_level"]),
                "source_type": "gaokao_passage",
                "source_ref": {
                    "year": passage_candidate["base_meta"]["year"],
                    "paper": passage_candidate["base_meta"]["paper"],
                    "paper_key": passage_candidate["base_meta"]["paper_key"],
                    "question_number": passage_candidate["base_meta"]["question_number"],
                },
                "correct_label": correct_label,
                "correct_text": passage_options[gloss_options.index(gloss)],
                "explanation": format_content_explanation(headword, gloss, support, f"{passage_candidate['base_meta']['year']} 年 {passage_candidate['base_meta']['paper']}"),
                "dict_support": term_record.get("dict_refs", [])[:2],
                "textbook_support": term_record.get("textbook_refs", [])[:2],
                "option_analyses": [
                    {
                        "label": label,
                        "text": option,
                        "is_correct": label == correct_label,
                        "analysis": build_content_option_analysis(headword, gloss, gloss_option, label == correct_label, support),
                    }
                    for label, option, gloss_option in zip(option_labels, passage_options, gloss_options)
                ],
            }
    for question_type in list(bank):
        bank[question_type] = sorted(
            bank[question_type],
            key=lambda item: (
                0 if str(item.get("priority_level") or "") == CONTENT_PRIORITY_CORE else 1,
                int(item.get("year") or 0),
                str(item.get("challenge_id") or ""),
            ),
        )
    return bank, answer_keys


def query_revised_links(headwords: list[str]) -> dict[str, list[dict[str, Any]]]:
    refs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    temp_ctx: tempfile.TemporaryDirectory[str] | None = None
    conn: sqlite3.Connection | None = None
    try:
        conn, temp_ctx = open_sqlite_readonly(MOE_REVISED_PATH)
        conn.row_factory = sqlite3.Row
        for headword in headwords:
            rows = conn.execute(
                """
                SELECT id, headword, content_text, raw_json
                FROM entries
                WHERE headword = ? OR headword_norm = ?
                ORDER BY LENGTH(headword) ASC, id ASC
                LIMIT 3
                """,
                (headword, headword),
            ).fetchall()
            for row in rows:
                refs[headword].append(
                    {
                        "entry_id": row["id"],
                        "headword": row["headword"],
                        "summary": truncate_excerpt(row["content_text"], 180),
                        "source": "moe_revised",
                    }
                )
    finally:
        if conn is not None:
            conn.close()
        if temp_ctx is not None:
            temp_ctx.cleanup()
    return refs


def query_idiom_links(headwords: list[str]) -> dict[str, list[dict[str, Any]]]:
    refs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    temp_ctx: tempfile.TemporaryDirectory[str] | None = None
    conn: sqlite3.Connection | None = None
    try:
        conn, temp_ctx = open_sqlite_readonly(MOE_IDIOMS_PATH)
        conn.row_factory = sqlite3.Row
        for headword in headwords:
            limit = 2 if len(headword) > 1 else 3
            rows = conn.execute(
                """
                SELECT id, headword, content_text
                FROM entries
                WHERE instr(headword, ?) > 0
                ORDER BY LENGTH(headword) ASC, id ASC
                LIMIT ?
                """,
                (headword, limit),
            ).fetchall()
            for row in rows:
                refs[headword].append(
                    {
                        "entry_id": row["id"],
                        "headword": row["headword"],
                        "summary": truncate_excerpt(row["content_text"], 140),
                        "source": "moe_idioms",
                    }
                )
    finally:
        if conn is not None:
            conn.close()
        if temp_ctx is not None:
            temp_ctx.cleanup()
    return refs


def build_term_records(
    raw_terms: list[dict[str, Any]],
    kind: str,
    textbook_refs: dict[str, list[dict[str, Any]]],
    revised_links: dict[str, list[dict[str, Any]]],
    idiom_links: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for term in raw_terms:
        headword = str(term["headword"])
        term_id = f"{'function' if kind == 'function_word' else 'content'}::{headword}"
        bases = []
        for occurrence in term.get("occurrences", []):
            bases.extend(infer_basis_records(occurrence, headword))
        bases = sorted(
            bases,
            key=lambda item: (
                int(item.get("exam_year") or 0),
                int(item.get("question_number") or 0),
                str(item.get("basis_type") or ""),
            ),
        )
        term_textbook_refs = textbook_refs.get(term_id, [])
        term_dict_refs = revised_links.get(headword, [])
        term_idiom_refs = idiom_links.get(headword, [])
        manual_review = not (term_textbook_refs or term_dict_refs or term_idiom_refs)
        cleaned_sample_glosses = select_clean_glosses(headword, list(term.get("occurrences", [])))
        usage_relations: list[dict[str, Any]]
        if kind == "function_word":
            usage_relations = FUNCTION_WORD_PROFILES.get(headword, [])
            priority_level = priority_level_for_function_term(term_textbook_refs, int(term.get("total_occurrences") or 0))
        else:
            gloss_counter = Counter(
                clean_gloss(headword, str(item.get("gloss") or ""), str(item.get("excerpt") or ""))
                for item in term.get("occurrences", [])
                if looks_like_clean_gloss(clean_gloss(headword, str(item.get("gloss") or ""), str(item.get("excerpt") or "")))
            )
            usage_relations = [
                {"semantic_value": gloss, "evidence_count": count}
                for gloss, count in gloss_counter.most_common(6)
            ]
            priority_level = priority_level_for_content_term(term_textbook_refs, int(term.get("total_occurrences") or 0))
        records.append(
            {
                "term_id": term_id,
                "kind": kind,
                "headword": headword,
                "display_headword": term.get("display_headword") or headword,
                "must_master": int(term.get("beijing_occurrences") or 0) > 0,
                "must_master_basis": bases,
                "beijing_frequency": int(term.get("beijing_occurrences") or 0),
                "national_frequency": int(term.get("national_occurrences") or 0),
                "year_range": [
                    min(term.get("years") or [0]) if term.get("years") else None,
                    max(term.get("years") or [0]) if term.get("years") else None,
                ],
                "question_type_counts": term.get("question_type_counts") or {},
                "frequencies": {
                    "total": int(term.get("total_occurrences") or 0),
                    "beijing": int(term.get("beijing_occurrences") or 0),
                    "national": int(term.get("national_occurrences") or 0),
                },
                "usage_relations": usage_relations,
                "sample_glosses": cleaned_sample_glosses[:6],
                "textbook_refs": term_textbook_refs,
                "dict_refs": term_dict_refs,
                "idiom_refs": term_idiom_refs,
                "priority_level": priority_level,
                "needs_manual_review": manual_review,
            }
        )
    return records


def build_exam_question_docs(
    question_docs: dict[str, dict[str, Any]],
    function_terms: list[dict[str, Any]],
    content_terms: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_paper: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for kind, terms in (("function_word", function_terms), ("content_word", content_terms)):
        for term in terms:
            term_id = f"{'function' if kind == 'function_word' else 'content'}::{term['headword']}"
            for occurrence in term.get("occurrences", []):
                by_paper[str(occurrence.get("paper_key") or "")].append(
                    {
                        "term_id": term_id,
                        "headword": term["headword"],
                        "kind": kind,
                        "question_number": occurrence.get("question_number"),
                        "question_subtype": occurrence.get("question_subtype"),
                        "option_label": occurrence.get("option_label"),
                        "excerpt": truncate_excerpt(str(occurrence.get("excerpt") or ""), 120),
                        "gloss": truncate_excerpt(str(occurrence.get("gloss") or ""), 60),
                    }
                )
    docs: list[dict[str, Any]] = []
    for paper_key, qdoc in sorted(question_docs.items(), key=lambda item: item[1].get("year") or 0):
        scope = "beijing" if "北京" in str(qdoc.get("paper") or qdoc.get("category") or "") else "national"
        docs.append(
            {
                "paper_key": paper_key,
                "scope": scope,
                "year": qdoc.get("year"),
                "paper": qdoc.get("paper"),
                "question_number": None,
                "question_subtype": "mixed",
                "text": truncate_excerpt(str(qdoc.get("text") or ""), 3200),
                "answer": truncate_excerpt(str(qdoc.get("answer") or ""), 600),
                "term_occurrences": by_paper.get(paper_key, []),
            }
        )
    return docs


def merge_asset_payloads(kind: str, payloads: list[Any]) -> Any:
    if kind == "list":
        merged: list[Any] = []
        for payload in payloads:
            merged.extend(payload)
        return merged
    merged: dict[str, Any] = {}
    for payload in payloads:
        merged.update(payload)
    return merged


def shard_payload(payload: Any, max_bytes: int) -> list[Any]:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) <= max_bytes:
        return [payload]

    if isinstance(payload, list):
        shards: list[list[Any]] = []
        current: list[Any] = []
        for item in payload:
            tentative = current + [item]
            encoded = json.dumps(tentative, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if current and len(encoded) > max_bytes:
                shards.append(current)
                current = [item]
            else:
                current = tentative
        if current:
            shards.append(current)
        return shards

    if isinstance(payload, dict):
        shards_dicts: list[dict[str, Any]] = []
        current_dict: dict[str, Any] = {}
        for key, value in payload.items():
            tentative = {**current_dict, key: value}
            encoded = json.dumps(tentative, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if current_dict and len(encoded) > max_bytes:
                shards_dicts.append(current_dict)
                current_dict = {key: value}
            else:
                current_dict = tentative
        if current_dict:
            shards_dicts.append(current_dict)
        return shards_dicts

    raise ValueError("Unsupported payload for sharding")


def write_runtime_asset(name: str, payload: Any, kind: str, manifest: dict[str, Any]) -> None:
    shards = shard_payload(payload, ASSET_MAX_BYTES)
    manifest["assets"][name] = {"kind": kind, "shards": []}
    for index, shard in enumerate(shards):
        file_name = f"{name}.json" if len(shards) == 1 else f"{name}.part{index + 1}.json"
        encoded = json.dumps(shard, ensure_ascii=False, indent=2).encode("utf-8")
        manifest["assets"][name]["shards"].append(
            {
                "file_name": file_name,
                "size_bytes": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
            }
        )
        for output_dir in (RUNTIME_MIRROR_DIR, PUBLIC_RUNTIME_DIR):
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / file_name).write_bytes(encoded)


def write_manifest(manifest: dict[str, Any]) -> None:
    encoded = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    for output_dir in (RUNTIME_MIRROR_DIR, PUBLIC_RUNTIME_DIR):
        (output_dir / "manifest.json").write_bytes(encoded)


def write_private_answer_keys(answer_keys: dict[str, Any]) -> None:
    encoded = json.dumps(answer_keys, ensure_ascii=False, indent=2).encode("utf-8")
    PRIVATE_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    (PRIVATE_RUNTIME_DIR / "answer_keys.json").write_bytes(encoded)
    (GENERATED_DIR / "answer_keys.json").write_bytes(encoded)


def clear_old_runtime_files() -> None:
    for output_dir in (RUNTIME_MIRROR_DIR, PUBLIC_RUNTIME_DIR):
        output_dir.mkdir(parents=True, exist_ok=True)
        for path in output_dir.glob("*.json"):
            path.unlink()


def main() -> int:
    report = collect_source_report()
    ensure_sources_or_raise(report)

    xuci = load_json(XUCI_PATH)
    shici = load_json(SHICI_PATH)
    manifest = load_json(MANIFEST_PATH)
    junior_md = JUNIOR_MD_PATH.read_text(encoding="utf-8")
    senior_md = SENIOR_MD_PATH.read_text(encoding="utf-8")
    question_docs = merge_question_docs(xuci, shici)
    templates = load_question_templates()

    function_raw_terms = list(xuci.get("terms", []))
    content_raw_terms = merge_content_terms(list(shici.get("terms", [])), question_docs)
    all_terms = [
        {"term_id": f"function::{item['headword']}", "headword": item["headword"]}
        for item in function_raw_terms
    ] + [
        {"term_id": f"content::{item['headword']}", "headword": item["headword"]}
        for item in content_raw_terms
    ]

    sections = build_classical_sections(manifest, junior_md, senior_md)
    textbook_refs = build_textbook_refs(all_terms, sections)
    revised_links = query_revised_links([item["headword"] for item in all_terms])
    idiom_links = query_idiom_links([item["headword"] for item in all_terms])

    function_records = build_term_records(
        function_raw_terms,
        "function_word",
        textbook_refs,
        revised_links,
        idiom_links,
    )
    content_records = build_term_records(
        content_raw_terms,
        "content_word",
        textbook_refs,
        revised_links,
        idiom_links,
    )

    function_bank, function_answer_keys = build_function_question_bank(function_raw_terms, question_docs, function_records)
    content_bank, content_answer_keys = build_content_question_bank(content_raw_terms, question_docs, content_records)
    exam_question_docs = build_exam_question_docs(question_docs, function_raw_terms, content_raw_terms)
    answer_keys = {**function_answer_keys, **content_answer_keys}

    dict_links = {
        record["term_id"]: {
            "headword": record["headword"],
            "kind": record["kind"],
            "revised_sense_links": record["dict_refs"],
            "idiom_links": record["idiom_refs"],
        }
        for record in [*function_records, *content_records]
    }

    textbook_examples = {
        key: value
        for key, value in textbook_refs.items()
        if value
    }

    exam_questions = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "question_docs": exam_question_docs,
        "challenge_bank": {
            "xuci_pair_compare": function_bank["xuci_pair_compare"],
            "function_gloss": function_bank["function_gloss"],
            "function_profile": function_bank["function_profile"],
            "content_gloss": content_bank["content_gloss"],
            "sentence_meaning": content_bank["sentence_meaning"],
            "passage_meaning": content_bank["passage_meaning"],
        },
        "question_templates": templates,
    }

    clear_old_runtime_files()
    write_private_answer_keys(answer_keys)
    manifest_payload = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "asset_max_bytes": ASSET_MAX_BYTES,
        "assets": {},
        "stats": {
            "terms_function": len(function_records),
            "terms_content": len(content_records),
            "question_docs": len(exam_question_docs),
            "textbook_term_refs": len(textbook_examples),
            "dict_term_refs": len(dict_links),
            "answer_keys": len(answer_keys),
        },
        "source_root": str(SOURCE_ROOT),
    }
    write_runtime_asset("terms_function", function_records, "list", manifest_payload)
    write_runtime_asset("terms_content", content_records, "list", manifest_payload)
    write_runtime_asset("exam_questions", exam_questions, "object", manifest_payload)
    write_runtime_asset("textbook_examples", textbook_examples, "object", manifest_payload)
    write_runtime_asset("dict_links", dict_links, "object", manifest_payload)
    write_manifest(manifest_payload)

    print(
        json.dumps(
            {
                "ok": True,
                "built_at": manifest_payload["built_at"],
                "stats": manifest_payload["stats"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
