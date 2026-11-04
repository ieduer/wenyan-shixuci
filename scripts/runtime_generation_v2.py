#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from check_sources import (
    MANIFEST_PATH,
    MOE_IDIOMS_PATH,
    MOE_REVISED_PATH,
    REPO_ROOT,
    SOURCE_ROOT,
    SHICI_PATH,
    XUCI_PATH,
    collect_source_report,
    ensure_sources_or_raise,
    normalize_title,
    open_sqlite_readonly,
)


ASSET_MAX_BYTES = int(os.environ.get("ASSET_MAX_BYTES", str(5 * 1024 * 1024)))
RUNTIME_MIRROR_DIR = REPO_ROOT / "data" / "runtime"
PUBLIC_RUNTIME_DIR = REPO_ROOT / "public" / "runtime"
PRIVATE_RUNTIME_DIR = REPO_ROOT / "data" / "runtime_private"
GENERATED_DIR = REPO_ROOT / "src" / "generated"
DOCS_DIR = REPO_ROOT / "docs"
QUESTION_TEMPLATES_DIR = REPO_ROOT / "question_templates"
TEXTBOOK_ARTICLE_MASTER_PATH = PRIVATE_RUNTIME_DIR / "textbook_article_master_table.json"
TEXTBOOK_NOTE_MASTER_PATH = PRIVATE_RUNTIME_DIR / "textbook_note_master_table.json"
TEXTBOOK_PROOFREAD_TABLE_PATH = PRIVATE_RUNTIME_DIR / "textbook_note_proofread_table.json"
TEXTBOOK_PROOFREAD_TABLE_CSV_PATH = PRIVATE_RUNTIME_DIR / "textbook_note_proofread_table.csv"
TEXTBOOK_PROOFREAD_MD_PATH = DOCS_DIR / "TEXTBOOK_NOTE_PROOFREADING.md"
TEXTBOOK_ARTICLE_PACKET_DIR = PRIVATE_RUNTIME_DIR / "textbook_article_packets"
TEXTBOOK_ARTICLE_PACKET_MD_DIR = DOCS_DIR / "TEXTBOOK_ARTICLES"
VERSION_MANIFEST_PATH = SOURCE_ROOT / "platform" / "backend" / "textbook_version_manifest.json.pre_chuzhong"
MINERU_OUTPUT_ROOT = SOURCE_ROOT / "data" / "mineru_output"
ANSWER_OVERRIDE_PATH = REPO_ROOT / "data" / "manual" / "beijing_exam_answer_overrides.json"
SOLUTION_NOTE_PATH = REPO_ROOT / "data" / "manual" / "beijing_exam_solution_notes.json"
OPTION_OVERRIDE_PATH = REPO_ROOT / "data" / "manual" / "beijing_exam_option_overrides.json"
XUCI_DETAILS_PATH = SOURCE_ROOT / "data" / "index" / "dict_exam_xuci_details.json"

QUESTION_TYPE_TO_BASIS = {
    "xuci_compare_same": "direct_choice",
    "xuci_compare_diff": "direct_choice",
    "xuci_explanation": "direct_choice",
    "shici_explanation": "direct_choice",
    "sentence_meaning": "sentence_meaning",
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

LOW_VALUE_SINGLE_CHAR_DICT_HEADWORDS = {
    "一",
    "二",
    "三",
    "四",
    "五",
    "上",
    "下",
    "大",
    "小",
    "中",
    "之",
    "其",
    "而",
    "于",
    "以",
    "为",
    "也",
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

TEXTBOOK_CLASSICAL_EXCLUDE_TITLES = {
    "目录",
    "预习",
    "阅读提示",
    "学习提示",
    "读读写写",
    "思考探究积累",
    "思考探究",
    "积累拓展",
    "单元学习任务",
    "单元研习任务",
    "写作",
    "口语交际",
    "综合性学习",
    "人民教育出版社",
}

CLASSICAL_MARKERS_STRONG = (
    "曰",
    "矣",
    "焉",
    "哉",
    "兮",
    "寡人",
    "若夫",
    "者也",
    "何以",
    "是故",
    "君子",
    "小人",
    "吾",
    "汝",
    "尔",
)

CLASSICAL_MARKERS_LIGHT = (
    "乃",
    "则",
    "岂",
    "孰",
    "奚",
    "故",
    "夫",
    "其子",
    "为之",
)

QUESTION_SPLIT_RE = re.compile(r"(?:^|\n)\s*(?:[（(]\s*)?(\d+)\s*(?:[)）]|[\.．、])\s*", re.M)
OPTION_SPLIT_RE = re.compile(r"([A-D])[\.．]\s*")
ITEM_SPLIT_RE = re.compile(
    r"([①②③④⑤⑥⑦⑧⑨⑩]|\d+)\s*(.*?)(?=(?:[①②③④⑤⑥⑦⑧⑨⑩]|\d+)\s*[A-D\u4e00-\u9fff(（]|$)",
    re.S,
)
STAR_TOKEN_RE = re.compile(r"\*([^*]{1,8})\*")
DOT_TOKEN_RE = re.compile(r"([\u4e00-\u9fff]{1,8})[．·•.]")
QUOTED_HEADWORD_RE = re.compile(r"[“\"]([\u4e00-\u9fff]{1,8})[”\"](?:字|词)?的解释")
GLOSS_RE = re.compile(r"([\u4e00-\u9fff]{1,8})\s*[：:；;]\s*(.+)")
XUCI_SUBTYPE_SAME_RE = re.compile(r"意义和用法.{0,6}(都相同|相同)")
XUCI_SUBTYPE_DIFF_RE = re.compile(r"意义和用法.{0,6}(不同|不相同)")
EXPLANATION_RE = re.compile(r"加点词(?:语)?(?:的|语的)解释|加点词语的解说")
NOTE_ENTRY_RE = re.compile(
    r"(?:\$\s*\\?textcircled\s*\{[^}]+\}\$|[①②③④⑤⑥⑦⑧⑨⑩])\s*[〔\[]([^〕\]】]+)[〕\]】]\s*(.*?)(?=(?:\n\s*(?:\$\s*\\?textcircled\s*\{[^}]+\}\$|[①②③④⑤⑥⑦⑧⑨⑩])\s*[〔\[]|\n#\s+|\Z))",
    re.S,
)
OPTION_LINE_RE = re.compile(r"^\s*([A-D])\s*(?:[\.．、:：]|(?=[\u4e00-\u9fff*“\"'‘’]))?\s*(.*)$")
OPTION_TOKEN_RE = re.compile(r"(?:(?<=^)|(?<=[\s\n]))([A-D])\s*(?:[\.．、:：]|(?=[\u4e00-\u9fff*“\"'‘’]))")
SUBQUESTION_LINE_RE = re.compile(r"^\s*(?:[（(])?([1-9]\d?)(?:[）)]|[\.．、])?\s*(.+?)\s*$")
NOTE_MARKER_RE = re.compile(r"(?<!\n)(\$\s*\\?textcircled\s*\{[^}]+\}\$)")
NOTE_LINE_RE = re.compile(r"^\s*\$\s*\\?textcircled\s*\{[^}]+\}\$\s*(.*)$")
NOTE_LABEL_RE = re.compile(r"^[〔［\[]([^〕］\]]+)[〕］\]]\s*(.*)$|^[【]([^】]+)[】]\s*(.*)$")
QUESTION_PREFIX_RE = re.compile(r"^\s*(?:[（(]\s*)?\d+\s*(?:[)）]|[\.．、])\s*")
IMAGE_LINE_RE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
WHITESPACE_RE = re.compile(r"\s+")
LESSON_HEADING_RE = re.compile(r"^\s*#\s*(?:\d+\s*)?(.+?)\s*$")
SECTION_BOUNDARY_RE = re.compile(r"^\s*#\s*(?:\d+\s+|第[一二三四五六七八九十百]+单元|古诗词诵读|词语积累与词语解释)")
BODY_HINT_RE = re.compile(r"[①②③④⑤⑥⑦⑧⑨⑩@]|[\u4e00-\u9fff]{4,}")

COMMON_XUCI_HEADWORDS = {
    "之",
    "其",
    "而",
    "以",
    "于",
    "乃",
    "则",
    "者",
    "也",
    "焉",
    "乎",
    "所",
    "与",
    "为",
    "且",
    "若",
    "因",
    "由",
    "抑",
    "或",
    "夫",
    "盖",
    "故",
    "诚",
    "既",
    "耳",
    "矣",
    "已",
    "哉",
    "欤",
    "遂",
    "即",
    "虽",
    "但",
    "何",
    "胡",
    "安",
    "孰",
    "奚",
    "斯",
    "兹",
    "然",
    "尔",
}

TEXTBOOK_FUNCTION_GRAMMAR_RE = re.compile(
    r"(助词|介词|连词|代词|副词|语气|语气词|结构助词|句中停顿|祈使语气|判断语气|宾语前置|提宾|被动)"
)
TEXTBOOK_FUNCTION_VALUE_RE = re.compile(r"(何|怎么|为什么|还是|竟|于是|就|趁着|假如|如果|将近|大概)")
TEXTBOOK_FUNCTION_BAD_HINT_RE = re.compile(
    r"(这里指|古代|人名|地名|匈奴|创作|性情|江面|媒人|不久后|备办|除去以前|武帝|流去的|广阔的江面)"
)
TEXTBOOK_FUNCTION_VALUE_PREFIXES = (
    "为什么呢",
    "为什么",
    "怎么",
    "还是",
    "于是",
    "如果",
    "那么",
    "就",
    "趁着（机会）",
    "趁着",
    "难道",
)
NUMBER_COMBO_OPTION_RE = re.compile(r"^\d{2,3}$")
FUNCTION_PROFILE_DISTRACTOR_FALLBACK = [
    "连词，表示并列或承接",
    "介词，表示凭借或把",
    "助词，提示停顿或判断",
    "代词，代人或代事物",
    "副词，表示反问或推测",
    "语气词，表示感叹或疑问",
]
FUNCTION_PROFILE_BAD_RE = re.compile(r"(其谁|其孰|用千|小旬|旬之前|前或代词后|一分句|…|[A-Za-z])")
TEXTBOOK_EXTRA_FUNCTION_HEADWORDS = {"来", "将", "言", "载"}
TEXTBOOK_FUNCTION_HEADWORD_ALLOW = COMMON_XUCI_HEADWORDS | TEXTBOOK_EXTRA_FUNCTION_HEADWORDS
DICT_SENSE_NUMBERED_RE = re.compile(r"(?:\[[^\]]+\]\s*)?\d+\.\s*([^\[\]]+?)(?=(?:\s*(?:\[[^\]]+\]\s*)?\d+\.)|\s*\[[^\]]+\]|$)")
DICT_SENSE_TAGGED_RE = re.compile(r"\[([^\]]+)\]\s*([^。\[\]]+)")
DICT_META_NOISE_RE = re.compile(r"(部首外筆畫數|總筆畫數|部首字|部首外笔画数|总笔画数)")

FUNCTION_USAGE_POS = {"代词", "副词", "连词", "助词", "介词", "语气词", "动词"}
CHINESE_CHAR_RE = re.compile(r"[\u4e00-\u9fff]")
CONTENT_GLOSS_BAD_HINT_RE = re.compile(
    r"(指.*?(王|帝|公|侯|君|将军|大夫|人|地名|县|州|国)|作者于|公元|前\d+|后\d+|年（?\d+|在今|版）|选自《)"
)
TEXTBOOK_CONTENT_TARGET_BAD_RE = re.compile(
    r"(地名|古地名|在今|今[^。；;，,]{0,16}(?:东|西|南|北|县|市|省|一带)|山名|水名|渡口|都城|国名|郡名|县令|州治|源出|流入长江的地方|一带的人|人名|官名|职官名|職官名|年号|帝号|谥号|匈奴的一个亲王|生卒年不详|请教过|问过礼|，字[\u4e00-\u9fff]{1,4}|(?:春秋|战国|两汉|汉代|唐代|宋代|明代|清代)[^。；;，,]{0,18}(?:人|乐官|大夫|将军|国君|诗人|词人)|部首|复姓|複姓|^姓(?:的支系)?(?:。)?$|^名，姓(?:。)?$)"
)
TEXTBOOK_NON_MEANINGFUL_HEADWORD_RE = re.compile(r"^[一二三四五六七八九十百千万两廿卅\d]+(?:年|月|日|夜|鼓)?$")
TEXTBOOK_CONTENT_EXACT_SKIP = {"知方", "吾与", "湯武", "汤武"}
CONTENT_DICT_GLOSS_BAD_RE = re.compile(
    r"^(?:(?:名|动|動|副|形|介|连|連|助|代)，)?(?:姓(?:的支系)?|名，姓|二一四部首之一|複姓|复姓|職官名|职官名|地名簡稱|地名简称|官名|官职|古代官名|古代地名|古代国名|人名用字|人名)$"
)
DIRECT_OPTION_GLOSS_BAD_PAREN_RE = re.compile(
    r"[（(][^）)]*(?:中午|下載|下载|教材|高考|学习|學習|創作|创作|作者于|在今|公元|年号|年號|链接|鏈接)[^）)]*[）)]"
)
DIRECT_OPTION_GLOSS_BAD_FRAGMENT_RE = re.compile(
    r"(还中午|僅供個人學習使用|仅供个人学习使用|教材|高考|下載|下载|链接|鏈接)"
)
NOTE_LEADING_LABEL_RE = re.compile(r"^[﹝〔［【\[]([^﹞〕］】\]]{1,48})[﹞〕］】\]]\s*(.*)$")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_text(value: str) -> str:
    text = str(value or "")
    text = text.replace("_x000D_", " ")
    text = text.replace("*", "")
    text = text.replace("\u3000", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def clean_text_keep_newlines(value: str) -> str:
    text = str(value or "").replace("_x000D_", " ")
    text = text.replace("\u3000", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_gloss(headword: str, gloss: str, excerpt: str = "") -> str:
    raw_gloss = clean_text(gloss)
    if not raw_gloss:
        return ""
    working = raw_gloss
    working = re.sub(r"^(意思是|这里是|这里指|这里借指)\s*[:：,，]\s*", "", working)
    if headword:
        working = re.sub(rf"^{re.escape(headword)}\s*[:：,，]\s*", "", working)
    same_as_match = re.search(r"义同\s*[“\"'‘]?([\u4e00-\u9fff]{1,12})", raw_gloss)
    if same_as_match:
        return same_as_match.group(1)
    translated_match = re.search(r"译为\s*[“\"'‘]?([\u4e00-\u9fff]{1,16})", raw_gloss)
    if translated_match:
        return translated_match.group(1).split("、", 1)[0]
    working = re.split(r"[。；;]\s*\d+\s*", working, maxsplit=1)[0]
    for marker in GLOSS_BLOCK_MARKERS:
        working = working.split(marker, 1)[0]
    working = re.sub(r"[“”\"'‘’]", "", working)
    working = working.strip("，,；;、。:： ")
    if "、" in working:
        working = working.split("、", 1)[0]
    if len(working) > 26 and any(sep in working for sep in ("，", ",", "；", ";", "。")):
        working = re.split(r"[，,；;。]", working, maxsplit=1)[0]
    if "。" in working:
        clauses = [clean_text(part) for part in working.split("。") if clean_text(part)]
        if len(clauses) >= 2 and len(clauses[-1]) <= 2 and len(clauses[0]) >= 2:
            working = clauses[0]
    return clean_text(working)


def clean_direct_option_gloss(headword: str, gloss: str) -> str:
    cleaned = clean_text(gloss)
    if not cleaned:
        return ""
    needs_compaction = bool(
        len(cleaned) > 28
        or CONTENT_GLOSS_BAD_HINT_RE.search(cleaned)
        or DIRECT_OPTION_GLOSS_BAD_PAREN_RE.search(cleaned)
        or DIRECT_OPTION_GLOSS_BAD_FRAGMENT_RE.search(cleaned)
    )
    if not needs_compaction:
        return cleaned

    stripped = DIRECT_OPTION_GLOSS_BAD_PAREN_RE.sub("", cleaned)
    stripped = DIRECT_OPTION_GLOSS_BAD_FRAGMENT_RE.sub("", stripped)
    stripped = clean_text(stripped).strip("，,；;。:： ")
    stripped = re.split(r"[。；;]", stripped, maxsplit=1)[0].strip("，,；;。:： ")
    if stripped and looks_like_clean_gloss(stripped):
        return stripped

    concise = clean_gloss(headword, stripped or cleaned)
    if concise and looks_like_clean_gloss(concise):
        return concise

    fallback = re.sub(r"[（(][^）)]*[）)]", "", stripped or cleaned)
    fallback = clean_text(fallback).strip("，,；;。:： ")
    fallback = re.split(r"[，,；;。]", fallback, maxsplit=1)[0].strip("，,；;。:： ")
    if fallback and looks_like_clean_gloss(fallback):
        return fallback
    return cleaned


def looks_like_clean_gloss(gloss: str) -> bool:
    cleaned = clean_text(gloss)
    if not cleaned or len(cleaned) > 28:
        return False
    if cleaned in BANNED_GLOSS_CANDIDATES:
        return False
    if any(marker in cleaned for marker in GLOSS_BLOCK_MARKERS):
        return False
    if re.search(r"\d", cleaned):
        return False
    return True


def normalize_signature_part(value: Any) -> str:
    return re.sub(r"\s+", "", clean_text(str(value or ""))).replace("“", "").replace("”", "").replace("「", "").replace("」", "")


def challenge_display_signature(item: dict[str, Any]) -> str:
    options = "|".join(
        f"{clean_text(str(option.get('label') or ''))}:{normalize_signature_part(option.get('sentence') or '')}:{normalize_signature_part(option.get('text') or '')}"
        for option in list(item.get("options") or [])
    )
    return "||".join(
        [
            clean_text(str(item.get("kind") or "")),
            normalize_signature_part(item.get("source_kind") or ""),
            normalize_signature_part(item.get("source_label") or ""),
            normalize_signature_part(item.get("stem") or ""),
            normalize_signature_part(item.get("sentence") or ""),
            normalize_signature_part(item.get("passage") or ""),
            options,
        ]
    )


def dedupe_challenge_bank(
    challenge_bank: dict[str, list[dict[str, Any]]],
    answer_keys: dict[str, dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    deduped: dict[str, list[dict[str, Any]]] = {}
    seen_signatures: dict[str, set[str]] = defaultdict(set)
    removed_ids: set[str] = set()
    for question_type in QUESTION_TYPES:
        kept: list[dict[str, Any]] = []
        for item in list(challenge_bank.get(question_type) or []):
            kind = clean_text(str(item.get("kind") or "content_word"))
            signature = challenge_display_signature(item)
            if not signature:
                kept.append(item)
                continue
            if signature in seen_signatures[kind]:
                removed_ids.add(clean_text(str(item.get("challenge_id") or "")))
                continue
            seen_signatures[kind].add(signature)
            kept.append(item)
        deduped[question_type] = kept
    filtered_answer_keys = {
        key: value for key, value in answer_keys.items() if clean_text(str(key)) not in removed_ids
    }
    return deduped, filtered_answer_keys


def normalize_exam_headword(headword: str, excerpt: str = "") -> str:
    candidate = clean_text(headword)
    if not candidate:
        return ""
    if len(candidate) <= 4:
        return candidate
    left = clean_text(str(excerpt or ""))
    if "：" in left:
        left = left.split("：", 1)[0]
    elif ":" in left:
        left = left.split(":", 1)[0]
    if left:
        for size in range(1, min(4, len(candidate)) + 1):
            suffix = candidate[-size:]
            if left.endswith(suffix) and suffix in left[: -size or None]:
                return suffix
        for size in range(1, min(4, len(left)) + 1):
            suffix = left[-size:]
            if left.endswith(suffix) and suffix in left[: -size or None]:
                return suffix
    if candidate[-1:] in COMMON_XUCI_HEADWORDS:
        return candidate[-1:]
    return candidate[-2:] if len(candidate) >= 2 else candidate


def normalize_occurrence_gloss(headword: str, occurrence: dict[str, Any]) -> str:
    return clean_gloss(
        normalize_exam_headword(headword, str(occurrence.get("excerpt") or "")),
        str(occurrence.get("gloss") or ""),
        str(occurrence.get("excerpt") or ""),
    )


def normalize_exam_occurrence(headword: str, occurrence: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(occurrence)
    normalized["headword"] = normalize_exam_headword(headword, str(occurrence.get("excerpt") or ""))
    normalized["gloss"] = normalize_occurrence_gloss(headword, occurrence)
    return normalized


def merge_content_terms(terms: list[dict[str, Any]], _question_docs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for term in terms:
        raw_headword = str(term.get("headword") or "").strip()
        if not raw_headword:
            continue
        occurrences = [
            normalize_exam_occurrence(raw_headword, occurrence)
            for occurrence in list(term.get("occurrences") or [])
            if occurrence.get("scope") == "beijing"
        ]
        if not occurrences:
            continue
        headword = normalize_exam_headword(raw_headword, str(occurrences[0].get("excerpt") or ""))
        if not headword:
            continue
        bucket = merged.setdefault(
            headword,
            {
                "headword": headword,
                "display_headword": headword,
                "occurrences": [],
                "years": [],
                "total_occurrences": 0,
                "beijing_occurrences": 0,
                "national_occurrences": 0,
                "question_type_counts": {},
                "sample_glosses": [],
            },
        )
        bucket["occurrences"].extend(occurrences)
    for headword, bucket in merged.items():
        years = sorted({int(item.get("year")) for item in bucket["occurrences"] if isinstance(item.get("year"), int)})
        counts = Counter(str(item.get("question_subtype") or "") for item in bucket["occurrences"] if str(item.get("question_subtype") or ""))
        sample_glosses = unique_clean_strings(
            [
                clean_gloss(headword, str(item.get("gloss") or ""), str(item.get("excerpt") or ""))
                for item in bucket["occurrences"]
            ]
        )
        bucket["years"] = years
        bucket["total_occurrences"] = len(bucket["occurrences"])
        bucket["beijing_occurrences"] = sum(1 for item in bucket["occurrences"] if item.get("scope") == "beijing")
        bucket["national_occurrences"] = sum(1 for item in bucket["occurrences"] if item.get("scope") == "national")
        bucket["question_type_counts"] = dict(counts)
        bucket["sample_glosses"] = sample_glosses[:8]
    return sorted(merged.values(), key=lambda item: (-int(item["beijing_occurrences"]), item["headword"]))


def merge_function_terms(terms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for term in terms:
        raw_headword = str(term.get("headword") or "").strip()
        if not raw_headword:
            continue
        occurrences = [
            normalize_exam_occurrence(raw_headword, occurrence)
            for occurrence in list(term.get("occurrences") or [])
            if occurrence.get("scope") == "beijing"
        ]
        if not occurrences:
            continue
        headword = normalize_exam_headword(raw_headword, str(occurrences[0].get("excerpt") or ""))
        if not headword:
            continue
        bucket = merged.setdefault(
            headword,
            {
                "headword": headword,
                "display_headword": headword,
                "occurrences": [],
                "years": [],
                "total_occurrences": 0,
                "beijing_occurrences": 0,
                "national_occurrences": 0,
                "question_type_counts": {},
                "sample_glosses": [],
            },
        )
        bucket["occurrences"].extend(occurrences)
    for headword, bucket in merged.items():
        years = sorted({int(item.get("year")) for item in bucket["occurrences"] if isinstance(item.get("year"), int)})
        counts = Counter(str(item.get("question_subtype") or "") for item in bucket["occurrences"] if str(item.get("question_subtype") or ""))
        sample_glosses = unique_clean_strings([str(item.get("gloss") or "") for item in bucket["occurrences"]])
        bucket["years"] = years
        bucket["total_occurrences"] = len(bucket["occurrences"])
        bucket["beijing_occurrences"] = len(bucket["occurrences"])
        bucket["national_occurrences"] = 0
        bucket["question_type_counts"] = dict(counts)
        bucket["sample_glosses"] = sample_glosses[:8]
    return sorted(merged.values(), key=lambda item: (-int(item["beijing_occurrences"]), item["headword"]))


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


def stable_shuffle(items: list[str], seed: str) -> list[str]:
    pool = [item for item in items if item]
    keyed = []
    for index, item in enumerate(pool):
        digest = hashlib.sha1(f"{seed}:{index}:{item}".encode("utf-8")).hexdigest()
        keyed.append((digest, index, item))
    keyed.sort(key=lambda row: (row[0], row[1]))
    return [row[2] for row in keyed]


def stable_pick(items: list[str], seed: str, count: int) -> list[str]:
    pool = [item for item in items if item]
    if len(pool) <= count:
        return pool
    start = stable_number(seed, len(pool))
    picked: list[str] = []
    for offset in range(len(pool)):
        candidate = pool[(start + offset) % len(pool)]
        if candidate in picked:
            continue
        picked.append(candidate)
        if len(picked) >= count:
            break
    return picked


def unique_clean_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(clean_text(value) for value in values if clean_text(value)))


def truncate_excerpt(text: str, limit: int = 200) -> str:
    cleaned = clean_text(text)
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 1] + "…"


def load_function_detail_terms() -> dict[str, Any]:
    if not XUCI_DETAILS_PATH.exists():
        return {}
    payload = load_json(XUCI_DETAILS_PATH)
    if not isinstance(payload, dict):
        return {}
    terms = payload.get("terms", {})
    return terms if isinstance(terms, dict) else {}


def normalize_function_profile_text(value: str) -> str:
    text = clean_text(value)
    text = re.sub(r"^[一二三四五六七八九十]+、", "", text)
    text = text.replace("，可译为", "，表示")
    text = text.replace("。可译为", "，表示")
    text = text.replace("可译为", "表示")
    text = text.replace("“", "").replace("”", "")
    text = text.replace('"', "").replace("'", "")
    text = text.replace("‘", "").replace("’", "")
    text = text.replace("\\", "")
    text = re.sub(r"\(\(.*", "", text)
    text = re.sub(r"[+]+", "", text)
    text = re.sub(r"\s+", "", text)
    return text.strip("，。；; ")


def canonical_function_semantic(usage: str, value: str) -> tuple[str, str]:
    text = normalize_function_profile_text(value)
    relation = relation_from_profile_text(text)
    if usage == "代词":
        if "其中" in text or "其一" in text:
            return "表示其中之一", relation
        if any(token in text for token in ("这", "那", "指示", "近指")):
            return "表示指示", relation
        if any(token in text for token in ("领属", "第三人称", "它的", "我的", "你的")):
            return "表示领属或代称", relation
        if "取消小句的独立性" in text or "作小句的主语" in text:
            return "作小句主语，取消句子独立性", relation
        if "其他" in text or "其馀" in text or "其余" in text:
            return "表示其余、其他", relation
    if usage == "副词":
        if any(token in text for token in ("祈请", "祈使")):
            return "表示祈使语气", "语气"
        if any(token in text for token in ("测度", "也许", "大概", "或许", "恐怕")):
            return "表示揣测", relation or "语气"
        if any(token in text for token in ("反诘", "难道", "岂")):
            return "表示反诘", relation or "语气"
        if any(token in text for token in ("将要", "就要", "即将")):
            return "表示将要", relation
        if any(token in text for token in ("程度", "非常", "确实", "果然", "真正")):
            return "表示强调或程度", relation
    if usage == "连词":
        if "假设" in text or "如果" in text:
            return "表示假设", "假设"
        if "让步" in text or "尚且" in text:
            return "表示让步", "让步"
        if "选择" in text or "还是" in text:
            return "表示选择", "选择"
        if "承接" in text or "论断" in text or "就" in text:
            return "表示承接或论断", relation or "承接"
        if "修饰语与名词之间" in text or "的" in text:
            return "连接修饰语与中心语", relation or "修饰"
    if usage == "助词":
        if "疑问" in text or "语气" in text or "句末" in text:
            return "表示语气或疑问", relation or "语气"
        if "附加成分与中心成分之间" in text or "之" in text:
            return "连接定语与中心语", relation or "修饰"
        if "动词性成分之后" in text or "名词性结构" in text:
            return "构成名词性结构", relation
        if "停顿" in text or "提示" in text:
            return "提示停顿或判断", relation or "语气"
    if usage == "介词":
        if any(token in text for token in ("凭借", "依靠", "因为", "由于", "替", "给", "向", "对")):
            return "表示凭借、对象或原因", relation
    if usage == "语气词":
        if any(token in text for token in ("疑问", "感叹", "语气")):
            return "表示疑问或感叹语气", relation or "语气"
    concise = semantic_value_from_profile_text(text)
    concise = re.sub(r"[。；;].*", "", concise)
    concise = truncate_excerpt(concise, 18)
    return concise, relation


def canonicalize_function_profile(usage: str, value: str) -> dict[str, str] | None:
    semantic, relation = canonical_function_semantic(usage, value)
    semantic = clean_text(semantic)
    if not semantic:
        return None
    display = f"{usage}，{semantic}"
    return {
        "part_of_speech": usage,
        "semantic_value": semantic,
        "syntactic_function": "",
        "relation": relation,
        "display": display,
    }


def function_profile_display_ok(value: str) -> bool:
    cleaned = normalize_function_profile_text(value)
    if not cleaned:
        return False
    if len(cleaned) < 6 or len(cleaned) > 18:
        return False
    if FUNCTION_PROFILE_BAD_RE.search(cleaned):
        return False
    return any(cleaned.startswith(f"{usage}，") for usage in FUNCTION_USAGE_POS)


def relation_from_profile_text(value: str) -> str:
    text = clean_text(value)
    for relation in ("并列", "承接", "递进", "转折", "因果", "目的", "条件", "假设", "让步", "修饰", "选择", "判断", "被动", "提宾", "语气"):
        if relation in text:
            return relation
    return ""


def semantic_value_from_profile_text(value: str) -> str:
    text = clean_text(value)
    match = re.search(r"表示([^，。；;]+)", text)
    if match:
        return clean_text(match.group(1))
    match = re.search(r"可译为([^，。；;]+)", text)
    if match:
        return clean_text(match.group(1))
    return text


def build_function_usage_catalog(detail_terms: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    catalog: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw_headword, detail in detail_terms.items():
        headword = clean_text(raw_headword)
        if not headword:
            continue
        xuci_dict = detail.get("xuci_dict") if isinstance(detail, dict) else None
        sections = xuci_dict.get("sections") if isinstance(xuci_dict, dict) else []
        profiles: list[dict[str, Any]] = []
        for section in sections or []:
            usage = clean_text(str(section.get("usage") or ""))
            if usage not in FUNCTION_USAGE_POS:
                continue
            senses = section.get("senses") if isinstance(section, dict) else []
            if senses:
                for sense in senses:
                    label = str(sense.get("label") or "")
                    summary = str(sense.get("summary") or "")
                    combined = " ".join(item for item in [label, summary] if clean_text(item))
                    if not clean_text(combined):
                        continue
                    canonical = canonicalize_function_profile(usage, combined)
                    if not canonical:
                        continue
                    profiles.append({**canonical, "source": "dict_exam_xuci_details"})
            else:
                summary = str(section.get("summary") or "")
                canonical = canonicalize_function_profile(usage, summary)
                if canonical:
                    profiles.append({**canonical, "source": "dict_exam_xuci_details"})
        deduped: list[dict[str, Any]] = []
        seen_displays: set[str] = set()
        for profile in profiles:
            display = clean_text(str(profile.get("display") or ""))
            if not display or display in seen_displays or not function_profile_display_ok(display):
                continue
            seen_displays.add(display)
            deduped.append(profile)
        catalog[headword] = deduped
    return {key: value for key, value in catalog.items() if value}


def filter_valid_content_glosses(values: list[str]) -> list[str]:
    cleaned_values = unique_clean_strings(values)
    results: list[str] = []
    for value in cleaned_values:
        if not value or len(value) > 120:
            continue
        if CONTENT_GLOSS_BAD_HINT_RE.search(value):
            continue
        if CONTENT_DICT_GLOSS_BAD_RE.search(value):
            continue
        if value in BANNED_GLOSS_CANDIDATES:
            continue
        if re.search(r"[……]|\.{2,}", value):
            continue
        if re.search(r"\d{3,}", value):
            continue
        results.append(value)
    return results


def textbook_content_ref_style(ref: dict[str, Any]) -> str:
    headword = clean_text(str(ref.get("headword") or ""))
    target = (
        normalize_label_headword(str(ref.get("label_text") or ""))
        or normalize_label_headword(leading_note_annotation(str(ref.get("note_block") or "")))
        or normalize_label_headword(headword)
    )
    if len(target) > 2:
        return "phrase"
    return "word"


def textbook_answer_shape(ref: dict[str, Any], kind: str, answer_text: str) -> str:
    if kind == "function_word":
        return "function"
    style = textbook_content_ref_style(ref)
    cleaned = clean_text(answer_text)
    if re.search(r"(心里|内心|痛快|高兴|快乐|忧愁|担心|害怕|惭愧|怜悯|吝惜|舍不得|不忍)", cleaned):
        return "state"
    if re.search(r"(地区|地方|地带|中原|都城|京城|城邑|境内|所在地|黄河|长江|江南|江北|河东|河西|山东|山西)", cleaned):
        return "place"
    if re.search(r"^(地名|国名)", cleaned):
        return "place"
    if re.search(r"^(古代|古人|古时|人名|官名|爵名|年号|星宿|一种|某种)", cleaned):
        return "term"
    if re.search(r"(地区|地方|地带|官职|官名|人名|国名|族名|衣服|牲畜|祭品|钟|鼓|车|马|牛|羊|猪)", cleaned):
        return "term"
    if re.search(r"^(用|把|使|让|给|向|对|为|替|被|从|到|达到|希望|愿意|听任|效法|侍奉|敬爱|率领|治理|安抚|保全|驱使|朝见|攻打|征伐)", cleaned):
        return "action"
    if re.search(r"(这里|此处|比喻|形容|指的是|意思是)", cleaned):
        return "explain"
    if style == "phrase":
        return "phrase"
    return "gloss"


def derive_textbook_dict_headwords(ref: dict[str, Any], fallback_headword: str = "") -> list[str]:
    candidates: list[str] = []
    raw_values = [
        fallback_headword,
        str(ref.get("headword") or ""),
        str(ref.get("label_text") or ""),
    ]
    for raw in raw_values:
        normalized = normalize_label_headword(raw)
        if not normalized:
            continue
        if len(normalized) <= 4:
            candidates.append(normalized)
        fragments: list[str] = []
        current = ""
        for char in normalized:
            if char in COMMON_XUCI_HEADWORDS and len(normalized) > 1:
                if current:
                    fragments.append(current)
                    current = ""
                continue
            current += char
        if current:
            fragments.append(current)
        for fragment in fragments:
            if 1 <= len(fragment) <= 4:
                candidates.append(fragment)
                continue
            for size in range(min(4, len(fragment)), 1, -1):
                candidates.append(fragment[:size])
                candidates.append(fragment[-size:])
                for start in range(0, len(fragment) - size + 1):
                    candidates.append(fragment[start : start + size])
        if len(normalized) > 1:
            candidates.extend(char for char in normalized if char not in COMMON_XUCI_HEADWORDS)
            candidates.extend(char for char in normalized if char in COMMON_XUCI_HEADWORDS)
    return unique_clean_strings(candidates)


def clean_dict_gloss_candidate(value: str) -> str:
    text = clean_text(value)
    if not text or DICT_META_NOISE_RE.search(text):
        return ""
    text = re.sub(r"《[^》]+》[^。；;]*", "", text)
    text = re.sub(r"^[如若]\s*[:：]\s*", "", text)
    text = re.split(r"[。；;（(]", text, maxsplit=1)[0]
    text = text.strip("，,；;、。:： ")
    if "如" in text and "：" in text:
        text = text.split("：", 1)[0]
    return clean_text(text)


def extract_revised_sense_candidates(links: list[dict[str, Any]]) -> list[str]:
    candidates: list[str] = []
    for link in links:
        summary = clean_text(str(link.get("summary") or ""))
        if not summary:
            continue
        numbered = [clean_dict_gloss_candidate(match.group(1)) for match in DICT_SENSE_NUMBERED_RE.finditer(summary)]
        if numbered:
            candidates.extend(item for item in numbered if item)
            continue
        for match in DICT_SENSE_TAGGED_RE.finditer(summary):
            pos = clean_text(str(match.group(1) or ""))
            sense = clean_dict_gloss_candidate(str(match.group(2) or ""))
            if not sense:
                continue
            if pos:
                candidates.append(f"{pos}，{sense}")
            else:
                candidates.append(sense)
    return filter_valid_content_glosses(candidates)


def select_textbook_dict_links(
    headword: str,
    refs: list[dict[str, Any]],
    revised_links: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_entry_ids: set[str] = set()

    def append_links(candidate_headword: str) -> None:
        for link in revised_links.get(candidate_headword, []):
            entry_id = str(link.get("entry_id") or "")
            if not entry_id or entry_id in seen_entry_ids:
                continue
            seen_entry_ids.add(entry_id)
            selected.append(link)

    append_links(headword)
    if selected:
        return selected[:4]

    candidate_headwords: list[str] = []
    for ref in refs:
        candidate_headwords.extend(ref.get("dict_headwords") or derive_textbook_dict_headwords(ref, headword))

    def candidate_priority(candidate_headword: str) -> tuple[int, int, int, int]:
        candidate = clean_text(str(candidate_headword))
        cleaned_headword = clean_text(headword)
        return (
            1 if candidate == cleaned_headword else 0,
            len(candidate),
            1 if cleaned_headword.endswith(candidate) else 0,
            0 if len(candidate) == 1 and candidate in LOW_VALUE_SINGLE_CHAR_DICT_HEADWORDS else 1,
        )

    for candidate in sorted(unique_clean_strings(candidate_headwords), key=candidate_priority, reverse=True):
        if candidate == clean_text(headword):
            continue
        if len(clean_text(headword)) > 1 and len(candidate) == 1 and candidate in LOW_VALUE_SINGLE_CHAR_DICT_HEADWORDS:
            continue
        append_links(candidate)
        if len(selected) >= 4:
            break
    return selected[:4]


def build_distractor_variants(pool: list[str], seed: str, max_variants: int) -> list[list[str]]:
    cleaned_pool = unique_clean_strings(pool)
    if len(cleaned_pool) < 3:
        return []
    variants: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for attempt in range(max_variants * 8):
        picks = stable_shuffle(cleaned_pool, f"{seed}:variant:{attempt}")[:3]
        if len(picks) < 3:
            continue
        key = tuple(sorted(picks))
        if key in seen:
            continue
        seen.add(key)
        variants.append(picks)
        if len(variants) >= max_variants:
            break
    return variants


def textbook_phrase_gloss_ok(value: str) -> bool:
    cleaned = clean_text(value)
    if len(cleaned) < 6 or len(cleaned) > 120:
        return False
    if CONTENT_GLOSS_BAD_HINT_RE.search(cleaned):
        return False
    return True


def textbook_distractor_text_ok(value: str, kind: str) -> bool:
    cleaned = clean_text(value)
    if not cleaned:
        return False
    if cleaned in BANNED_GLOSS_CANDIDATES:
        return False
    if "仅供个人学习使用" in cleaned or "人民教育出版社" in cleaned:
        return False
    if re.search(r"[……]|\.{2,}", cleaned):
        return False
    if kind == "function_word":
        if len(cleaned) > 40:
            return False
        if TEXTBOOK_FUNCTION_BAD_HINT_RE.search(cleaned):
            return False
        if FUNCTION_PROFILE_BAD_RE.search(cleaned):
            return False
        return True
    if len(cleaned) > 90:
        return False
    if CONTENT_GLOSS_BAD_HINT_RE.search(cleaned):
        return False
    if CONTENT_DICT_GLOSS_BAD_RE.search(cleaned):
        return False
    if re.search(r"\d{3,}", cleaned):
        return False
    return True


def textbook_distractor_answer_text(ref: dict[str, Any], kind: str, headword: str) -> str:
    if kind == "function_word":
        return normalize_function_profile_text(
            str(ref.get("answer_text") or ref.get("gloss") or ref.get("note_block") or "")
        )
    raw_answer = str(
        ref.get("answer_text")
        or build_textbook_answer_text(
            str(ref.get("label_text") or ""),
            str(ref.get("note_block") or ""),
            str(ref.get("gloss") or ""),
            headword,
        )
    )
    return clean_textbook_answer_text(raw_answer, headword, str(ref.get("label_text") or ""))


def build_textbook_distractor_catalog(textbook_refs: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for term_id, refs in sorted(textbook_refs.items()):
        kind = "function_word" if str(term_id).startswith("function::") else "content_word"
        fallback_headword = str(term_id).split("::", 1)[1] if "::" in str(term_id) else str(term_id)
        for ref in refs:
            headword = clean_text(str(ref.get("headword") or fallback_headword))
            label_text = str(ref.get("label_text") or "")
            note_text = str(ref.get("note_block") or "")
            gloss = str(ref.get("gloss") or "")
            sentence = best_textbook_focus_sentence(
                str(ref.get("sentence") or ""),
                list(ref.get("context_window") or []),
                headword,
                label_text,
            )
            answer_text = textbook_distractor_answer_text(ref, kind, headword)
            if not answer_text:
                continue
            if not textbook_ref_is_reliable(kind, headword, label_text, note_text, gloss or answer_text, sentence):
                continue
            if not textbook_distractor_text_ok(answer_text, kind):
                continue
            book_key = clean_text(str(ref.get("book_key") or ""))
            title = clean_text(str(ref.get("title") or ""))
            catalog.append(
                {
                    "text": answer_text,
                    "kind": kind,
                    "term_id": term_id,
                    "headword": headword,
                    "ref_id": clean_text(str(ref.get("ref_id") or "")),
                    "book_key": book_key,
                    "title": title,
                    "article_id": stable_slug(f"{book_key}:{title}") if book_key or title else "",
                    "school_stage": clean_text(str(ref.get("school_stage") or "")),
                    "style": textbook_content_ref_style(ref) if kind == "content_word" else "function",
                    "answer_shape": textbook_answer_shape(ref, kind, answer_text),
                    "note_order": int(ref.get("note_order") or 0),
                }
            )
    return catalog


def text_too_close_for_distractor(candidate: str, correct: str) -> bool:
    left = clean_text(candidate)
    right = clean_text(correct)
    if not left or not right:
        return True
    if left == right:
        return True
    left_compact = compact_hanzi_text(left)
    right_compact = compact_hanzi_text(right)
    if not left_compact or not right_compact:
        return False
    shorter, longer = sorted([left_compact, right_compact], key=len)
    return len(shorter) >= 3 and shorter in longer


def build_textbook_note_distractors(
    ref: dict[str, Any],
    term_id: str,
    kind: str,
    correct_text: str,
    catalog: list[dict[str, Any]],
    seed: str,
) -> list[dict[str, str]]:
    ref_id = clean_text(str(ref.get("ref_id") or ""))
    book_key = clean_text(str(ref.get("book_key") or ""))
    title = clean_text(str(ref.get("title") or ""))
    school_stage = clean_text(str(ref.get("school_stage") or ""))
    article_id = stable_slug(f"{book_key}:{title}") if book_key or title else ""
    style = textbook_content_ref_style(ref) if kind == "content_word" else "function"
    answer_shape = textbook_answer_shape(ref, kind, correct_text)

    scored: list[tuple[int, str, dict[str, Any]]] = []
    for candidate in catalog:
        if str(candidate.get("kind") or "") != kind:
            continue
        if clean_text(str(candidate.get("ref_id") or "")) == ref_id:
            continue
        text = clean_text(str(candidate.get("text") or ""))
        if text_too_close_for_distractor(text, correct_text):
            continue
        score = 0
        if clean_text(str(candidate.get("term_id") or "")) == clean_text(term_id):
            score += 110
        if clean_text(str(candidate.get("article_id") or "")) == article_id:
            score += 90
        if clean_text(str(candidate.get("book_key") or "")) == book_key:
            score += 45
        if clean_text(str(candidate.get("school_stage") or "")) == school_stage:
            score += 20
        if clean_text(str(candidate.get("style") or "")) == style:
            score += 80
        else:
            score -= 60
        candidate_shape = clean_text(str(candidate.get("answer_shape") or ""))
        if candidate_shape == answer_shape:
            score += 90
        elif answer_shape in {"state", "action", "place", "term"} or candidate_shape in {"state", "action", "place", "term"}:
            score -= 85
        else:
            score -= 35
        score -= min(80, abs(len(text) - len(clean_text(correct_text))) * 2)
        tie_break = hashlib.sha1(
            f"{seed}:{candidate.get('ref_id')}:{candidate.get('text')}".encode("utf-8")
        ).hexdigest()
        scored.append((score, tie_break, candidate))

    chosen: list[dict[str, str]] = []
    seen_texts: set[str] = set()
    for _score, _tie_break, candidate in sorted(scored, key=lambda item: (-item[0], item[1])):
        text = clean_text(str(candidate.get("text") or ""))
        if not text or text in seen_texts:
            continue
        seen_texts.add(text)
        chosen.append(
            {
                "text": text,
                "origin": "textbook_note_distractor",
                "source_ref_id": clean_text(str(candidate.get("ref_id") or "")),
                "source_title": clean_text(str(candidate.get("title") or "")),
                "source_headword": clean_text(str(candidate.get("headword") or "")),
            }
        )
        if len(chosen) >= 3:
            break
    return chosen


def group_textbook_refs_by_source(textbook_refs: dict[str, list[dict[str, Any]]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for refs in textbook_refs.values():
        for ref in refs:
            key = (str(ref.get("book_key") or ""), str(ref.get("title") or ""))
            grouped[key].append(ref)
    return grouped


def build_content_distractor_pool(
    ref: dict[str, Any],
    record: dict[str, Any],
) -> list[str]:
    current_gloss = clean_text(str(ref.get("answer_text") or ref.get("gloss") or ref.get("note_block") or ""))
    pool = extract_revised_sense_candidates(list(record.get("dict_refs") or []))
    filtered = [item for item in filter_valid_content_glosses(pool) if item != current_gloss]
    if textbook_content_ref_style(ref) == "phrase":
        return [item for item in filtered if textbook_phrase_gloss_ok(item)]
    return filtered


def build_function_distractor_pool(
    headword: str,
    correct_gloss: str,
    usage_catalog: dict[str, list[dict[str, Any]]],
    record: dict[str, Any],
) -> list[str]:
    pool = [str(profile.get("display") or "") for profile in usage_catalog.get(headword, [])]
    pool.extend(str(item) for item in list(record.get("sample_glosses") or []))
    pool.extend(FUNCTION_PROFILE_DISTRACTOR_FALLBACK)
    cleaned = unique_clean_strings(normalize_function_profile_text(item) for item in pool if item)
    return [item for item in cleaned if item and item != normalize_function_profile_text(correct_gloss)]


def build_headword_frequency_records(
    terms_function: list[dict[str, Any]],
    terms_content: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for term in [*terms_content, *terms_function]:
        records.append(
            {
                "term_id": term["term_id"],
                "headword": term["headword"],
                "kind": term["kind"],
                "beijing_frequency": term.get("beijing_frequency", 0),
                "national_frequency": term.get("national_frequency", 0),
                "textbook_ref_count": len(term.get("textbook_refs", [])),
                "question_type_counts": term.get("question_type_counts", {}),
                "sample_glosses": term.get("sample_glosses", []),
            }
        )
    return sorted(records, key=lambda item: (-int(item.get("beijing_frequency") or 0), str(item.get("term_id") or "")))


def longest_match_segment(text: str, vocabulary: set[str], max_length: int = 8) -> list[str]:
    compact = "".join(CHINESE_CHAR_RE.findall(clean_text(text)))
    if not compact:
        return []
    tokens: list[str] = []
    index = 0
    while index < len(compact):
        matched = ""
        for length in range(min(max_length, len(compact) - index), 0, -1):
            candidate = compact[index : index + length]
            if candidate in vocabulary:
                matched = candidate
                break
        if not matched:
            matched = compact[index]
        tokens.append(matched)
        index += len(matched)
    return tokens


def build_segmentation_vocabulary(
    terms_function: list[dict[str, Any]],
    terms_content: list[dict[str, Any]],
    textbook_refs: dict[str, list[dict[str, Any]]],
) -> set[str]:
    vocabulary: set[str] = set(COMMON_XUCI_HEADWORDS)
    for term in [*terms_content, *terms_function]:
        headword = clean_text(str(term.get("headword") or ""))
        if 1 <= len(headword) <= 8:
            vocabulary.add(headword)
    for refs in textbook_refs.values():
        for ref in refs:
            for value in (str(ref.get("headword") or ""), str(ref.get("label_text") or "")):
                token = normalize_label_headword(value)
                if 1 <= len(token) <= 8:
                    vocabulary.add(token)
    return {item for item in vocabulary if item}


def build_corpus_frequency_table(passages: list[dict[str, Any]], vocabulary: set[str]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    sources: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for passage in passages:
        tokens = longest_match_segment(str(passage.get("text") or ""), vocabulary)
        counter.update(tokens)
        for token in dict.fromkeys(tokens):
            if len(sources[token]) >= 5:
                continue
            sources[token].append(
                {
                    "title": passage.get("title"),
                    "source": passage.get("source"),
                    "year": passage.get("year"),
                    "book_key": passage.get("book_key"),
                }
            )
    return [
        {
            "token": token,
            "frequency": frequency,
            "examples": sources.get(token, []),
        }
        for token, frequency in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def load_question_templates() -> dict[str, Any]:
    templates: dict[str, Any] = {}
    for path in sorted(QUESTION_TEMPLATES_DIR.glob("*.json")):
        templates[path.stem] = load_json(path)
    return templates


def _normalize_text(text: str) -> str:
    return clean_text_keep_newlines(text)


def _strip_question_prefix(block: str) -> str:
    return QUESTION_PREFIX_RE.sub("", _normalize_text(block), count=1).strip()


def _iter_question_blocks(text: str) -> list[tuple[int, str]]:
    normalized = _normalize_text(text)
    matches = list(QUESTION_SPLIT_RE.finditer(normalized))
    blocks: list[tuple[int, str]] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        number = int(match.group(1))
        blocks.append((number, normalized[start:end].strip()))
    return blocks


def _block_lines(block: str) -> list[str]:
    return [line.strip().strip("*") for line in _normalize_text(block).splitlines() if clean_text(line)]


def _extract_prompt_and_body_lines(block: str) -> tuple[str, list[str]]:
    prompt_lines: list[str] = []
    body_lines: list[str] = []
    started = False
    for line in _block_lines(block):
        if not started and (OPTION_LINE_RE.match(line) or SUBQUESTION_LINE_RE.match(line)):
            started = True
        if started:
            body_lines.append(line)
        else:
            prompt_lines.append(line)
    return clean_text(" ".join(prompt_lines)), body_lines


def _split_options(block: str) -> list[tuple[str, str]]:
    normalized = _normalize_text(block)
    matches = list(OPTION_TOKEN_RE.finditer(normalized))
    if not matches:
        return []
    options: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        label = clean_text(match.group(1))
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        option_text = re.sub(r"\s+", " ", clean_text_keep_newlines(normalized[start:end])).strip()
        if label and option_text:
            options.append((label, option_text))
    return options


def _split_items(block: str) -> list[tuple[int, str]]:
    items: list[tuple[int, str]] = []
    current_number: int | None = None
    current_lines: list[str] = []
    _prompt, body_lines = _extract_prompt_and_body_lines(block)
    for raw_line in body_lines:
        sub_match = SUBQUESTION_LINE_RE.match(raw_line)
        if sub_match and not OPTION_LINE_RE.match(raw_line):
            content = clean_text(sub_match.group(2))
            if "分" in content and len(content) <= 6:
                continue
            if current_number is not None and current_lines:
                items.append((current_number, clean_text_keep_newlines("\n".join(current_lines)).strip()))
            current_number = int(sub_match.group(1))
            current_lines = [content]
            continue
        if current_number is not None:
            current_lines.append(raw_line)
    if current_number is not None and current_lines:
        items.append((current_number, clean_text_keep_newlines("\n".join(current_lines)).strip()))
    return items


def _extract_emphasis_tokens(text: str) -> list[str]:
    tokens = [clean_text(token) for token in STAR_TOKEN_RE.findall(text) if clean_text(token)]
    if tokens:
        return tokens
    return [clean_text(token) for token in DOT_TOKEN_RE.findall(text) if clean_text(token)]


def _extract_gloss_from_unit(unit_text: str, explicit_headword: str | None = None) -> tuple[str, str] | None:
    normalized = truncate_excerpt(unit_text, 400)
    if explicit_headword:
        explicit_pattern = re.compile(rf"{re.escape(explicit_headword)}\s*[：:]\s*(.+)")
        match = explicit_pattern.search(normalized)
        if match:
            gloss = match.group(1).strip()
            gloss = re.split(r"\s+[A-D][\.．]\s+", gloss)[0].strip()
            return explicit_headword, gloss
    match = GLOSS_RE.search(normalized)
    if not match:
        return None
    headword = clean_text(match.group(1))
    gloss = clean_text(match.group(2))
    gloss = re.split(r"\s+[A-D][\.．]\s+", gloss)[0].strip()
    return headword, gloss


def _split_sentence_and_headword(left_text: str, hinted_headword: str = "") -> tuple[str, str]:
    left = clean_text(left_text)
    hinted = clean_text(hinted_headword)
    if hinted and left.endswith(hinted):
        sentence = clean_text(left[: -len(hinted)])
        return sentence or left, hinted
    for size in range(min(4, len(left)), 0, -1):
        candidate = left[-size:]
        if candidate and candidate in left[: -size or None]:
            sentence = clean_text(left[: -size])
            return sentence or left, candidate
    return left, hinted or left[-1:]


def option_sentence_and_gloss(
    option_text: str,
    explicit_headword: str | None = None,
    hinted_headword: str | None = None,
    hinted_gloss: str | None = None,
) -> dict[str, str]:
    normalized = clean_text(option_text)
    hinted = clean_text(hinted_headword or "")
    if explicit_headword and not any(separator in normalized for separator in ("：", ":", "；", ";")):
        return {"headword": explicit_headword, "sentence": "", "gloss": normalized}
    if "：" in normalized:
        left, right = normalized.split("：", 1)
    elif ":" in normalized:
        left, right = normalized.split(":", 1)
    elif "；" in normalized:
        left, right = normalized.split("；", 1)
    elif ";" in normalized:
        left, right = normalized.split(";", 1)
    else:
        if hinted and hinted_gloss:
            sentence, headword = _split_sentence_and_headword(normalized, hinted)
            return {
                "headword": headword or hinted,
                "sentence": sentence,
                "gloss": clean_text(hinted_gloss),
            }
        parsed = _extract_gloss_from_unit(option_text, explicit_headword=explicit_headword)
        if not parsed:
            return {"headword": hinted or explicit_headword or "", "sentence": "", "gloss": normalized}
        headword, gloss = parsed
        return {"headword": headword, "sentence": "", "gloss": gloss}
    sentence, headword = _split_sentence_and_headword(left, hinted or explicit_headword or "")
    return {"headword": headword, "sentence": sentence, "gloss": clean_text(right)}


def _detect_block_subtype(block: str) -> str | None:
    if XUCI_SUBTYPE_SAME_RE.search(block):
        return "xuci_compare_same"
    if XUCI_SUBTYPE_DIFF_RE.search(block):
        return "xuci_compare_diff"
    if EXPLANATION_RE.search(block) or QUOTED_HEADWORD_RE.search(block):
        return "gloss_explanation"
    return None


def _answer_label_from_text(answer_text: str, question_number: int) -> str:
    normalized = clean_text_keep_newlines(answer_text or "")
    patterns = [
        rf"(?<!\d){question_number}(?!\d)\s*[\.．、]\s*(?:[（(]\s*\d+\s*分\s*[)）]\s*)?([A-D])\b",
        rf"(?<!\d){question_number}(?!\d)\s*(?:[（(]\s*\d+\s*分\s*[)）]\s*)\s*([A-D])\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            return match.group(1)
    return ""


def load_manual_answer_overrides() -> dict[str, dict[str, str]]:
    if not ANSWER_OVERRIDE_PATH.exists():
        return {}
    payload = load_json(ANSWER_OVERRIDE_PATH)
    return payload if isinstance(payload, dict) else {}


def load_solution_notes() -> dict[str, dict[str, str]]:
    if not SOLUTION_NOTE_PATH.exists():
        return {}
    payload = load_json(SOLUTION_NOTE_PATH)
    return payload if isinstance(payload, dict) else {}


def load_manual_option_overrides() -> dict[str, dict[str, str]]:
    if not OPTION_OVERRIDE_PATH.exists():
        return {}
    payload = load_json(OPTION_OVERRIDE_PATH)
    return payload if isinstance(payload, dict) else {}


def exam_option_key(paper_key: str, question_number: int, option_label: str, sub_index: int | None = None) -> str:
    key = f"{paper_key}#{question_number}"
    if sub_index:
        key += f"#{sub_index}"
    return f"{key}#{option_label}"


def answer_label_for_source(
    overrides: dict[str, dict[str, str]],
    paper_key: str,
    question_number: int,
    answer_text: str,
    sub_index: int | None = None,
) -> str:
    key = f"{paper_key}#{question_number}" + (f"#{sub_index}" if sub_index else "")
    override = overrides.get(key, {})
    override_label = clean_text(str(override.get("label") or ""))
    if override_label in {"A", "B", "C", "D"}:
        return override_label
    return _answer_label_from_text(answer_text, question_number)


def build_manual_dict_support(solution_note: dict[str, Any]) -> list[dict[str, Any]]:
    headword = clean_text(str(solution_note.get("dict_headword") or ""))
    excerpt = clean_text(str(solution_note.get("dict_excerpt") or ""))
    if not excerpt:
        return []
    return [
        {
            "entry_id": f"manual:{headword or 'dict'}",
            "headword": headword or clean_text(str(solution_note.get("actual_gloss") or "")),
            "summary": excerpt,
            "source": "manual_moe_revised",
        }
    ]


def apply_manual_term_source_corrections(
    terms: list[dict[str, Any]],
    option_overrides: dict[str, dict[str, str]],
    solution_notes: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    corrected_terms: list[dict[str, Any]] = []
    for raw_term in terms:
        term = dict(raw_term)
        occurrences: list[dict[str, Any]] = []
        override_heads: list[str] = []
        for raw_occurrence in list(raw_term.get("occurrences") or []):
            occurrence = dict(raw_occurrence)
            paper_key = clean_text(str(occurrence.get("paper_key") or ""))
            question_number = int(occurrence.get("question_number") or 0)
            option_label = clean_text(str(occurrence.get("option_label") or ""))
            if paper_key and question_number and option_label in {"A", "B", "C", "D"}:
                manual_key = exam_option_key(paper_key, question_number, option_label)
                option_override = option_overrides.get(manual_key, {})
                solution_note = solution_notes.get(manual_key, {})
                if option_override.get("headword"):
                    occurrence["headword"] = clean_text(str(option_override.get("headword") or ""))
                    override_heads.append(clean_text(str(option_override.get("headword") or "")))
                if option_override.get("excerpt"):
                    occurrence["excerpt"] = clean_text(str(option_override.get("excerpt") or ""))
                elif option_override.get("sentence"):
                    sentence = clean_text(str(option_override.get("sentence") or ""))
                    gloss = clean_direct_option_gloss(
                        clean_text(str(option_override.get("headword") or occurrence.get("headword") or "")),
                        str(option_override.get("gloss") or occurrence.get("gloss") or ""),
                    )
                    headword = clean_text(str(option_override.get("headword") or occurrence.get("headword") or ""))
                    if sentence and headword and gloss:
                        occurrence["excerpt"] = f"{sentence} {headword}:{gloss}"
                if option_override.get("gloss"):
                    occurrence["display_gloss"] = clean_direct_option_gloss(
                        clean_text(str(option_override.get("headword") or occurrence.get("headword") or "")),
                        str(option_override.get("gloss") or ""),
                    )
                if solution_note.get("actual_gloss"):
                    occurrence["gloss"] = clean_direct_option_gloss(
                        clean_text(str(option_override.get("headword") or occurrence.get("headword") or "")),
                        str(solution_note.get("actual_gloss") or ""),
                    )
                elif option_override.get("gloss"):
                    occurrence["gloss"] = clean_direct_option_gloss(
                        clean_text(str(option_override.get("headword") or occurrence.get("headword") or "")),
                        str(option_override.get("gloss") or ""),
                    )
            occurrences.append(occurrence)
        if occurrences:
            term["occurrences"] = occurrences
        override_heads = unique_clean_strings(override_heads)
        if len(override_heads) == 1:
            term["headword"] = override_heads[0]
            term["display_headword"] = override_heads[0]
        corrected_terms.append(term)
    return corrected_terms


def extract_source_passage(qdoc_text: str) -> str:
    raw = str(qdoc_text or "")
    if not raw:
        return ""
    for pattern in (
        r"\n\s*[\(（]\s*1\s*[\)）]",
        r"\n\s*1\s*[\.．、]",
        r"\n\s*第[一二三四五六七八九十]+题",
        r"\s(?=\d{1,2}\s*[\.．、]\s*(?:对下列|下列|把文中|将文中|根据文意|下列各组|下列句子))",
    ):
        match = re.search(pattern, raw)
        if match:
            return raw[: match.start()]
    return raw


def normalize_context_source(text: str) -> str:
    normalized = clean_text_keep_newlines(text)
    normalized = IMAGE_LINE_RE.sub("", normalized)
    kept_lines: list[str] = []
    for raw_line in normalized.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if line.startswith("![]("):
            continue
        kept_lines.append(line)
    return "\n".join(kept_lines)


def split_context_units(text: str) -> list[str]:
    normalized = normalize_context_source(text)
    lines = []
    for raw_line in normalized.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = re.split(r"(?<=[。！？；!?])", line)
        for part in parts:
            value = clean_text(part)
            if value:
                lines.append(value)
    return lines


def split_context_units_with_offsets(text: str) -> list[dict[str, Any]]:
    normalized = normalize_context_source(text)
    units: list[dict[str, Any]] = []
    for match in re.finditer(r"[^\n。！？；!?]+[。！？；!?]?", normalized):
        value = clean_text(match.group(0))
        if not value:
            continue
        units.append(
            {
                "text": value,
                "start": match.start(),
                "end": match.end(),
            }
        )
    return units


def context_window_from_position(source_text: str, position: int) -> list[str]:
    units = split_context_units_with_offsets(source_text)
    if not units:
        return []
    target_index = 0
    for index, unit in enumerate(units):
        if int(unit["start"]) <= position < int(unit["end"]):
            target_index = index
            break
        if position >= int(unit["end"]):
            target_index = index
    start = max(0, target_index - 3)
    end = min(len(units), target_index + 4)
    return [str(unit["text"]) for unit in units[start:end]]


def best_context_window(source_text: str, probe_text: str, headword: str = "") -> list[str]:
    units = split_context_units(source_text)
    if not units:
        cleaned_probe = clean_text(probe_text)
        return [cleaned_probe] if cleaned_probe else []
    cleaned_probe = clean_text(probe_text)
    probe_tokens = [token for token in re.split(r"[，,。！？；:：\s]+", cleaned_probe) if token]
    best_index = 0
    best_score = -1
    for index, unit in enumerate(units):
        score = 0
        if cleaned_probe and cleaned_probe[:10] in unit:
            score += 8
        if headword and headword in unit:
            score += 4
        score += sum(1 for token in probe_tokens[:6] if token and token in unit)
        if score > best_score:
            best_index = index
            best_score = score
    start = max(0, best_index - 3)
    end = min(len(units), best_index + 4)
    return units[start:end]


def best_textbook_focus_sentence(sentence: str, context_window: list[str], headword: str, label_text: str) -> str:
    candidates = [clean_text(sentence)] + [clean_text(item) for item in context_window if clean_text(item)]
    variants = textbook_target_variants(headword, label_text)
    for variant in variants:
        compact_variant = compact_hanzi_text(variant)
        if not compact_variant:
            continue
        for candidate in candidates:
            if not candidate:
                continue
            for unit in split_context_units(candidate):
                if compact_variant in compact_hanzi_text(unit):
                    return clean_text(unit)
            if compact_variant in compact_hanzi_text(candidate):
                return candidate
    return clean_text(sentence or (candidates[0] if candidates else ""))


def locate_progressive_probe(body_text: str, label_text: str, headword: str, start_index: int = 0) -> tuple[str, int]:
    candidates = unique_clean_strings(
        [
            clean_text(label_text),
            normalize_label_headword(label_text),
            clean_text(headword),
        ]
    )
    search_body = normalize_context_source(body_text)
    for candidate in sorted(candidates, key=len, reverse=True):
        if not candidate:
            continue
        position = search_body.find(candidate, start_index)
        if position >= 0:
            return candidate, position
    for candidate in sorted(candidates, key=len, reverse=True):
        if not candidate:
            continue
        position = search_body.find(candidate)
        if position >= 0:
            return candidate, position
    return clean_text(label_text) or clean_text(headword), -1


def _compact_query(value: str) -> str:
    return re.sub(r"\s+", "", clean_text(value))


def extract_marked_compare_sentences(raw_excerpt: str, headword: str) -> list[str]:
    raw = str(raw_excerpt or "")
    if not raw:
        return []
    plain = clean_text(raw.replace("*", ""))
    if headword:
        marked_pattern = re.escape(f"*{headword}*")
        marked_matches = list(re.finditer(marked_pattern, raw))
        if len(marked_matches) >= 2:
            split_parts = re.split(marked_pattern, raw, maxsplit=2)
            if len(split_parts) == 3:
                prefix, middle, suffix = split_parts
                separator_candidates = [
                    index for index, char in enumerate(middle) if char in {" ", "/", "／"}
                ]
                if separator_candidates:
                    middle_split = max(separator_candidates)
                    left = clean_text((prefix + headword + middle[:middle_split]).replace("*", ""))
                    right = clean_text((middle[middle_split:] + headword + suffix).replace("*", ""))
                    if left and right:
                        return [left, right]
                boundary_candidates = [
                    index
                    for index, char in enumerate(middle)
                    if char in {"。", "！", "？", "；", "，", ",", "、"}
                ]
                if boundary_candidates:
                    midpoint = len(middle) / 2
                    middle_split = min(boundary_candidates, key=lambda index: abs(index - midpoint))
                    left = clean_text((prefix + headword + middle[:middle_split]).replace("*", ""))
                    right = clean_text((middle[middle_split:] + headword + suffix).replace("*", ""))
                    if left and right:
                        return [left, right]
        occurrences = [match.start() for match in re.finditer(re.escape(headword), plain)]
        if len(occurrences) >= 2:
            lower_bound = occurrences[0] + len(headword)
            upper_bound = occurrences[1]
            boundary_candidates = [
                index
                for index, char in enumerate(plain)
                if lower_bound <= index < upper_bound and char in {" ", "/", "／"}
            ]
            boundary_candidates.extend(
                index + 1
                for index, char in enumerate(plain)
                if lower_bound <= index < upper_bound and char in {"。", "！", "？", "；", "，", ",", "、"}
            )
            if boundary_candidates:
                midpoint = (lower_bound + upper_bound) / 2
                split_at = min(boundary_candidates, key=lambda index: abs(index - midpoint))
            else:
                split_at = upper_bound
            left = clean_text(plain[:split_at])
            right = clean_text(plain[split_at:])
            if left and right:
                return [left, right]
    if "/" in raw:
        parts = [clean_text(part.replace("*", "")) for part in raw.split("/") if clean_text(part.replace("*", ""))]
        if len(parts) >= 2:
            return parts[:2]
    units = split_context_units(plain)
    return units[:2]


def question_prompt_text(block_body: str) -> str:
    prompt, _body = _extract_prompt_and_body_lines(block_body)
    return prompt


def split_title_parts(value: str) -> list[str]:
    return [part.strip() for part in re.split(r"/|／", str(value or "")) if part.strip()]


def title_part_variants(title_part: str) -> list[str]:
    title = str(title_part or "").strip()
    variants = [title]
    variants.extend(split_title_parts(title))
    variants.extend([item.replace("《", "").replace("》", "").strip() for item in variants])
    return unique_clean_strings(variants)


def locate_section_by_title(text: str, title: str, all_titles: set[str], window: int = 20000) -> str:
    normalized_targets = [normalize_title(item) for item in title_part_variants(title)]
    heading_matches = list(re.finditer(r"(?m)^#\s*(.+?)\s*$", text))
    for index, match in enumerate(heading_matches):
        line = clean_text(match.group(1))
        normalized_line = normalize_title(line)
        if not any(target and target in normalized_line for target in normalized_targets):
            continue
        start = match.start()
        end = min(len(text), start + window)
        for later in heading_matches[index + 1 :]:
            later_line = clean_text(later.group(1))
            normalized_later = normalize_title(later_line)
            if normalized_later in all_titles and not any(target and target == normalized_later for target in normalized_targets):
                end = later.start()
                break
            if SECTION_BOUNDARY_RE.match(f"# {later_line}"):
                end = later.start()
                break
        return text[start:end]
    normalized_text = normalize_title(text)
    for target in normalized_targets:
        pos = normalized_text.find(target)
        if pos >= 0:
            return text[max(0, pos - 500) : pos + window]
    return ""


def resolve_language_book_meta() -> dict[str, dict[str, str]]:
    payload = load_json(VERSION_MANIFEST_PATH)
    by_book_key = payload.get("by_book_key", {}) if isinstance(payload, dict) else {}
    meta: dict[str, dict[str, str]] = {}
    for book_key, info in by_book_key.items():
        if "_语文_" not in str(book_key):
            continue
        meta[str(book_key)] = {
            "book_key": str(book_key),
            "title": str(info.get("title") or ""),
            "display_title": str(info.get("display_title") or info.get("title") or ""),
        }
    return meta


def resolve_language_book_paths(book_meta: dict[str, dict[str, str]]) -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for book_key in sorted(book_meta):
        direct_dir = MINERU_OUTPUT_ROOT / book_key
        md_candidates: list[Path] = []
        if direct_dir.exists():
            md_candidates.extend(sorted(direct_dir.glob("*.md")))
        if not md_candidates:
            globbed = list(MINERU_OUTPUT_ROOT.glob(f"{book_key}*/**/*.md"))
            md_candidates.extend(sorted(path for path in globbed if path.name.endswith(".md")))
        if md_candidates:
            resolved[book_key] = md_candidates[0]
    return resolved


def extract_passage_heading(section_text: str) -> str:
    for raw_line in section_text.splitlines()[:6]:
        line = clean_text(raw_line.strip("*# "))
        if not line:
            continue
        if len(line) > 28:
            continue
        if re.fullmatch(r"[0-9一二三四五六七八九十百千（）()·.、 ]+", line):
            continue
        if any(mark in line for mark in ("。", "？", "！", "；", "：", ":")):
            continue
        return line
    return ""


def classical_marker_score(text: str) -> int:
    score = 0
    for marker in CLASSICAL_MARKERS_STRONG:
        score += min(4, text.count(marker)) * 2
    for marker in CLASSICAL_MARKERS_LIGHT:
        score += min(4, text.count(marker))
    if "《" in text and "》" in text:
        score += 2
    return score


def looks_like_poem(text: str) -> bool:
    lines = [clean_text(line) for line in clean_text_keep_newlines(text).splitlines() if clean_text(line)]
    if len(lines) < 4:
        return False
    sample = lines[:16]
    short_lines = []
    for line in sample:
        normalized = re.sub(r"[（(].*?[)）]", "", line).strip("· ")
        if 2 <= len(normalized) <= 18 and not normalized.endswith("。"):
            short_lines.append(normalized)
    return len(short_lines) >= 4 and len(short_lines) >= max(4, int(len(sample) * 0.4))


def parse_author_from_section(section_text: str) -> tuple[str, str]:
    lines = [clean_text(line.strip("*# ")) for line in clean_text_keep_newlines(section_text).splitlines()]
    filtered = [line for line in lines[1:8] if line and "〔" not in line and not line.startswith("![](")]
    for line in filtered:
        if len(line) <= 18 and not any(token in line for token in ("人民教育出版社", "预习", "学习提示", "阅读提示")):
            if "（" in line and "）" in line:
                author = re.split(r"[（(]", line, maxsplit=1)[0].strip()
                dynasty = ""
                if "唐代" in line:
                    dynasty = "唐"
                elif "宋代" in line:
                    dynasty = "宋"
                elif "汉" in line:
                    dynasty = "汉"
                elif "东汉" in line:
                    dynasty = "东汉"
                return author, dynasty
            if re.fullmatch(r"[\u4e00-\u9fff·]{1,12}", line):
                return line, ""
    return "", ""


def normalize_textbook_section(section_text: str) -> str:
    normalized = clean_text_keep_newlines(section_text)
    normalized = NOTE_MARKER_RE.sub(r"\n\1", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized


def looks_like_body_line(line: str) -> bool:
    cleaned = clean_text(line)
    if not cleaned:
        return False
    if cleaned in TEXTBOOK_CLASSICAL_EXCLUDE_TITLES:
        return False
    if cleaned.startswith(("仅供个人学习使用", "人民教育出版社")):
        return False
    if cleaned.startswith(("你还记得", "熟读课文", "回顾一下", "尝试用自己的话", "学习活动", "本单元", "资料", "另外")):
        return False
    if IMAGE_LINE_RE.search(cleaned):
        return False
    if BODY_HINT_RE.search(cleaned) and classical_marker_score(cleaned) >= 1:
        return True
    return looks_like_poem(cleaned) or classical_marker_score(cleaned) >= 2


def split_section_body_and_notes(section_text: str) -> tuple[str, list[str]]:
    lines = normalize_textbook_section(section_text).splitlines()
    body_lines: list[str] = []
    note_lines: list[str] = []
    in_body = False
    in_notes = False
    for raw_line in lines[1:]:
        line = raw_line.strip()
        if not line or line.startswith("![]("):
            continue
        if line.startswith("# "):
            heading = clean_text(line.strip("# "))
            if heading in {"思考探究", "积累拓展", "单元学习任务", "单元研习任务", "写作", "口语交际", "综合性学习"}:
                break
            if heading in {"预习", "阅读提示", "学习提示"}:
                continue
            if not in_body:
                continue
            break
        if line.startswith(("仅供个人学习使用", "人民教育出版社")):
            continue
        if NOTE_LINE_RE.match(line):
            in_notes = True
            in_body = True
            note_lines.append(line)
            continue
        if in_notes:
            note_lines.append(line)
            continue
        if not in_body:
            if looks_like_body_line(line):
                in_body = True
                body_lines.append(line)
            continue
        body_lines.append(line)
    return "\n".join(body_lines).strip(), note_lines


def parse_note_entries(note_lines: list[str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    current_lines: list[str] = []
    for line in note_lines:
        if NOTE_LINE_RE.match(line):
            if current_lines:
                entries.append({"raw": clean_text_keep_newlines("\n".join(current_lines))})
            current_lines = [NOTE_LINE_RE.sub(r"\1", line).strip()]
        elif current_lines:
            current_lines.append(line)
    if current_lines:
        entries.append({"raw": clean_text_keep_newlines("\n".join(current_lines))})
    parsed: list[dict[str, str]] = []
    for entry in entries:
        raw = clean_text_keep_newlines(entry["raw"])
        if not raw or raw.startswith("选自") or raw.startswith("说明"):
            continue
        first_line, *rest_lines = raw.splitlines()
        label_match = NOTE_LABEL_RE.match(first_line)
        label_text = ""
        body = ""
        if label_match:
            label_text = clean_text(label_match.group(1) or label_match.group(3) or "")
            body = clean_text_keep_newlines((label_match.group(2) or label_match.group(4) or "") + ("\n" + "\n".join(rest_lines) if rest_lines else ""))
        else:
            continue
        if not label_text or not body:
            continue
        parsed.append({"label_text": label_text, "note_text": clean_text(body)})
    return parsed


def extract_note_headword(label_text: str, note_text: str = "") -> str:
    label = clean_text(label_text)
    label = re.sub(r"[（(].*?[)）]", "", label)
    label = re.sub(r"《.*?》", "", label)
    label = "".join(re.findall(r"[\u4e00-\u9fff]+", label))
    body = clean_text(note_text)
    stripped_body = strip_leading_note_annotation(note_text) or body
    function_like_note = bool(
        TEXTBOOK_FUNCTION_GRAMMAR_RE.search(body)
        or TEXTBOOK_FUNCTION_VALUE_RE.search(body)
        or TEXTBOOK_FUNCTION_GRAMMAR_RE.search(stripped_body)
        or TEXTBOOK_FUNCTION_VALUE_RE.search(stripped_body)
    )
    quoted_targets = [
        clean_text(match.group(1))
        for match in re.finditer(r"[“\"]([\u4e00-\u9fff]{1,3})[”\"]", stripped_body)
        if clean_text(match.group(1))
    ]
    for candidate in quoted_targets:
        if candidate in label and candidate in TEXTBOOK_FUNCTION_HEADWORD_ALLOW:
            return candidate
    body_token_match = re.search(r"^([\u4e00-\u9fff]{1,4})[，,:：]", body)
    if body_token_match:
        candidate = body_token_match.group(1)
        if candidate in label or candidate in TEXTBOOK_FUNCTION_HEADWORD_ALLOW:
            return candidate
    stripped_body_token_match = re.search(r"^([\u4e00-\u9fff]{1,4})[，,:：]", stripped_body)
    if stripped_body_token_match:
        candidate = stripped_body_token_match.group(1)
        if candidate in label or candidate in TEXTBOOK_FUNCTION_HEADWORD_ALLOW:
            return candidate
    clause_matches = re.finditer(r"(?:^|[。；;，,？！!?])\s*([\u4e00-\u9fff]{1,2})\s*[，,:：]\s*([^。；;]{1,28})", body)
    for match in clause_matches:
        candidate = clean_text(match.group(1))
        detail = clean_text(match.group(2))
        if not candidate:
            continue
        if len(candidate) == 1 and TEXTBOOK_FUNCTION_GRAMMAR_RE.search(detail):
            return candidate
        if candidate in TEXTBOOK_FUNCTION_HEADWORD_ALLOW and (
            TEXTBOOK_FUNCTION_GRAMMAR_RE.search(detail) or TEXTBOOK_FUNCTION_VALUE_RE.search(detail)
        ):
            return candidate
    if label in TEXTBOOK_FUNCTION_HEADWORD_ALLOW:
        return label
    if function_like_note and label:
        if len(label) > 1 and label[-1] in TEXTBOOK_FUNCTION_HEADWORD_ALLOW:
            if TEXTBOOK_FUNCTION_GRAMMAR_RE.search(body):
                return label[-1]
            if TEXTBOOK_FUNCTION_VALUE_RE.search(body) and label[0] not in COMMON_XUCI_HEADWORDS:
                return label[-1]
        function_chars = [char for char in label if char in TEXTBOOK_FUNCTION_HEADWORD_ALLOW]
        if len(dict.fromkeys(function_chars)) == 1:
            return function_chars[0]
    if len(label) <= 4:
        return label
    if body:
        body_hint_match = re.search(r"([\u4e00-\u9fff]{1,4})[，,:：].{0,18}(?:用作后缀|助词|代词|介词|连词|语气)", body)
        if body_hint_match:
            return body_hint_match.group(1)
        for size in range(1, min(4, len(label)) + 1):
            candidate = label[-size:]
            if candidate and re.search(rf"{re.escape(candidate)}[，,:：]", body):
                return candidate
    return label[:8]


def extract_textbook_note_explicit_targets(note_text: str) -> list[str]:
    stripped = strip_leading_note_annotation(note_text) or clean_text(note_text)
    if not stripped:
        return []
    candidates: list[str] = []
    candidates.extend(
        clean_text(match.group(1))
        for match in re.finditer(r"[“\"]([\u4e00-\u9fff]{1,3})[”\"]", stripped)
        if clean_text(match.group(1))
    )
    candidates.extend(
        clean_text(match.group(1))
        for match in re.finditer(r"(?:^|[。；;，,？！!?])\s*([\u4e00-\u9fff]{1,3})\s*[，,:：]", stripped)
        if clean_text(match.group(1))
    )
    return unique_clean_strings(candidates)


def textbook_primary_target(headword: str, label_text: str, note_text: str) -> str:
    normalized_headword = normalize_label_headword(headword)
    normalized_label = normalize_label_headword(label_text)
    leading_label = normalize_label_headword(leading_note_annotation(note_text))
    if normalized_headword and normalized_label and normalized_headword in normalized_label and len(normalized_headword) < len(normalized_label):
        return normalized_headword
    if leading_label and normalized_label and leading_label in normalized_label and len(leading_label) < len(normalized_label):
        return leading_label
    return normalized_label or normalized_headword


def resolve_textbook_bank_headword(row: dict[str, Any], initial_headword: str, term_kind: str) -> str:
    label = normalize_label_headword(str(row.get("label_text") or ""))
    note_text = clean_text(str(row.get("note_text") or ""))
    stripped_note = strip_leading_note_annotation(note_text) or note_text
    sentence = compact_hanzi_text(
        clean_text(
            str(
                row.get("source_sentence")
                or row.get("text_source_sentence")
                or row.get("layout_source_sentence")
                or ""
            )
        )
    )
    explicit_targets = extract_textbook_note_explicit_targets(note_text)
    dict_targets = unique_clean_strings(str(item) for item in list(row.get("dict_headwords") or []) if clean_text(str(item)))
    normalized_initial = normalize_label_headword(initial_headword)

    def sentence_has(candidate: str) -> bool:
        compact_candidate = compact_hanzi_text(candidate)
        return bool(compact_candidate and compact_candidate in sentence)

    if term_kind == "function_word":
        scored: list[tuple[int, str]] = []
        for candidate in unique_clean_strings([*explicit_targets, normalized_initial, label, *dict_targets]):
            normalized_candidate = normalize_label_headword(candidate)
            if not normalized_candidate or len(normalized_candidate) > 2:
                continue
            if normalized_candidate not in TEXTBOOK_FUNCTION_HEADWORD_ALLOW:
                continue
            if not sentence_has(normalized_candidate):
                continue
            score = 0
            if normalized_candidate in explicit_targets:
                score += 60
            if re.search(rf"[“\"]{re.escape(normalized_candidate)}[”\"]", stripped_note):
                score += 30
            if re.search(rf"(?:^|[。；;，,？！!?])\s*{re.escape(normalized_candidate)}\s*[，,:：]", stripped_note):
                score += 30
            if normalized_candidate == normalized_initial:
                score += 18
            if normalized_candidate in label:
                score += 12
            score += 8 - len(normalized_candidate)
            scored.append((score, normalized_candidate))
        if scored:
            return sorted(scored, key=lambda item: (-item[0], len(item[1]), item[1]))[0][1]
        return ""

    if len(label) <= 2 and sentence_has(label):
        return label

    scored_content: list[tuple[int, str]] = []
    label_suffixes = [label[-size:] for size in (2, 1) if len(label) >= size]
    if "之" in label:
        prefix = label.split("之", 1)[0]
        if 1 <= len(prefix) <= 2:
            label_suffixes.insert(0, prefix)
    if label.startswith("如") and len(label) >= 3:
        for size in (2, 1):
            candidate = label[1 : 1 + size]
            if len(candidate) == size:
                label_suffixes.insert(0, candidate)
    for explicit in explicit_targets:
        normalized_explicit = normalize_label_headword(explicit)
        if len(normalized_explicit) == 1 and label.endswith(normalized_explicit) and len(label) >= 2:
            label_suffixes.insert(0, label[-2:])
    candidate_pool = unique_clean_strings(
        [normalized_initial, *explicit_targets, *dict_targets, *label_suffixes]
    )
    for candidate in candidate_pool:
        normalized_candidate = normalize_label_headword(candidate)
        if not normalized_candidate or len(normalized_candidate) > 2:
            continue
        if normalized_candidate in COMMON_XUCI_HEADWORDS and normalized_candidate not in explicit_targets:
            continue
        if not sentence_has(normalized_candidate):
            continue
        score = 0
        if normalized_candidate in explicit_targets:
            score += 70
        if re.search(rf"(?:^|[。；;，,？！!?])\s*{re.escape(normalized_candidate)}\s*[，,:：]", stripped_note):
            score += 40
        if normalized_candidate in dict_targets:
            score += 28
        if label.endswith(normalized_candidate):
            score += 18
        if label.startswith(normalized_candidate):
            score += 10
        if normalized_candidate in stripped_note:
            score += 8
        score += len(normalized_candidate) * 6
        scored_content.append((score, normalized_candidate))
    if scored_content:
        return sorted(scored_content, key=lambda item: (-item[0], -len(item[1]), item[1]))[0][1]
    if len(normalized_initial) <= 2 and sentence_has(normalized_initial):
        return normalized_initial
    return ""


def summarize_note_gloss(headword: str, note_text: str) -> str:
    original_cleaned = clean_text(note_text)
    cleaned = strip_leading_note_annotation(note_text)
    if not cleaned:
        cleaned = original_cleaned
    if not cleaned:
        return ""
    if cleaned.startswith("选自《") or "仅供个人学习使用" in cleaned:
        return ""
    if headword:
        inline_match = re.search(rf"(?:^|[。；;，,？！!?])\s*{re.escape(headword)}\s*[，,:：]\s*([^。；;]+)", cleaned)
        if inline_match:
            candidate = clean_text(inline_match.group(1))
            if looks_like_clean_gloss(candidate) or TEXTBOOK_FUNCTION_GRAMMAR_RE.search(candidate):
                return truncate_excerpt(candidate, 28)
    for marker in ("意思是", "这里是", "这里指", "这里借指", "即", "指"):
        if marker in cleaned and len(cleaned.split(marker, 1)[1]) <= 22:
            candidate = clean_text(cleaned.split(marker, 1)[1])
            if looks_like_clean_gloss(candidate):
                return candidate
    if re.search(r"^[\u4e00-\u9fff]{1,4}[，,:：]", cleaned):
        candidate = clean_text(re.split(r"[，,:：]", cleaned, maxsplit=1)[1])
        candidate = re.split(r"[。；;]", candidate, maxsplit=1)[0]
        if looks_like_clean_gloss(candidate):
            return candidate
    candidate = clean_gloss(headword, cleaned)
    if looks_like_clean_gloss(candidate):
        return candidate
    first_clause = re.split(r"[。；;，,]", cleaned, maxsplit=1)[0]
    return truncate_excerpt(first_clause, 28)


def summarize_textbook_function_gloss_candidate(value: str) -> str:
    cleaned = normalize_function_profile_text(value)
    if not cleaned:
        return ""
    if TEXTBOOK_FUNCTION_GRAMMAR_RE.search(cleaned) and cleaned.startswith(
        ("助词", "语气助词", "语助词", "副词", "连词", "介词", "代词", "句中助词", "表示", "用于", "用在", "表")
    ):
        return truncate_excerpt(cleaned, 18)
    if cleaned in {"无实义", "句中助词", "语助词", "语气助词", "表被动", "表示被动"}:
        return cleaned
    if len(cleaned) <= 12 and not TEXTBOOK_FUNCTION_BAD_HINT_RE.search(cleaned):
        if not re.search(r"[。；;]", cleaned):
            return cleaned
    for prefix in TEXTBOOK_FUNCTION_VALUE_PREFIXES:
        if not cleaned.startswith(prefix):
            continue
        tail = cleaned[len(prefix) :]
        if tail.startswith(("、", "，")) and len(cleaned) <= 12:
            return cleaned
        return prefix
    return ""


def summarize_textbook_function_gloss(headword: str, note_text: str, gloss: str) -> str:
    sources = [
        clean_text(gloss),
        strip_leading_note_annotation(note_text),
        clean_text(note_text),
    ]
    for source in sources:
        if not source:
            continue
        if headword:
            inline_match = re.search(rf"(?:^|[。；;，,？！!?])\s*{re.escape(headword)}\s*[，,:：]\s*([^。；;]+)", source)
            if inline_match:
                summary = summarize_textbook_function_gloss_candidate(inline_match.group(1))
                if summary:
                    return summary
        summary = summarize_textbook_function_gloss_candidate(source)
        if summary:
            return summary
    return ""


def build_textbook_answer_text(label_text: str, note_text: str, gloss: str, headword: str = "") -> str:
    cleaned_note = clean_text(note_text)
    stripped_note = strip_leading_note_annotation(note_text) or cleaned_note
    cleaned_gloss = clean_text(gloss)
    normalized_label = textbook_primary_target(headword, label_text, note_text)
    if not cleaned_note:
        return cleaned_gloss
    compact_note = "".join(
        part
        for part in [clean_text(item) for item in re.split(r"(?<=[。；;])", stripped_note)][:2]
        if part
    )
    if len(normalized_label) >= 4:
        return truncate_excerpt(compact_note or stripped_note or cleaned_note, 120)
    if cleaned_gloss in {"意思是", "这里是", "这里指", "指"} or len(cleaned_gloss) <= 2:
        return truncate_excerpt(compact_note or stripped_note or cleaned_note, 120)
    return cleaned_gloss


def clean_textbook_answer_text(answer_text: str, headword: str = "", label_text: str = "") -> str:
    cleaned = clean_text(answer_text)
    if not cleaned:
        return ""
    cleaned = re.sub(r"([。！？；，、,.!?;])\1+", r"\1", cleaned)
    stripped = strip_leading_note_annotation(cleaned)
    if stripped and stripped != cleaned:
        return re.sub(r"([。！？；，、,.!?;])\1+", r"\1", stripped)
    for target in textbook_target_variants(headword, label_text):
        target_compact = compact_hanzi_text(target)
        if not target_compact:
            continue
        pattern = rf"^[﹝〔［【\[]\s*{re.escape(target_compact)}(?:[（(][^）)]*[）)])?\s*"
        candidate = re.sub(pattern, "", cleaned, count=1)
        if candidate != cleaned and clean_text(candidate):
            return re.sub(r"([。！？；，、,.!?;])\1+", r"\1", clean_text(candidate))
    return cleaned


def normalize_label_headword(label_text: str) -> str:
    label = clean_text(label_text)
    label = re.sub(r"[（(].*?[)）]", "", label)
    label = re.sub(r"《.*?》", "", label)
    return "".join(re.findall(r"[\u4e00-\u9fff]+", label))


def split_leading_note_annotation(note_text: str) -> tuple[str, str]:
    cleaned = clean_text(note_text)
    if not cleaned:
        return "", ""
    match = NOTE_LEADING_LABEL_RE.match(cleaned)
    if not match:
        return "", cleaned
    return clean_text(match.group(1)), clean_text(match.group(2))


def leading_note_annotation(note_text: str) -> str:
    return split_leading_note_annotation(note_text)[0]


def strip_leading_note_annotation(note_text: str) -> str:
    return split_leading_note_annotation(note_text)[1]


def compact_hanzi_text(value: str) -> str:
    return "".join(CHINESE_CHAR_RE.findall(clean_text(value)))


def textbook_target_variants(headword: str, label_text: str) -> list[str]:
    return sorted(
        unique_clean_strings([normalize_label_headword(label_text), clean_text(headword)]),
        key=len,
        reverse=True,
    )


def textbook_target_in_text(text: str, headword: str, label_text: str) -> bool:
    compact_text = compact_hanzi_text(text)
    if not compact_text:
        return False
    for variant in textbook_target_variants(headword, label_text):
        compact_variant = compact_hanzi_text(variant)
        if compact_variant and compact_variant in compact_text:
            return True
    return False


def truncate_excerpt_around_target(text: str, headword: str, label_text: str, limit: int = 160) -> str:
    cleaned = clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    for variant in textbook_target_variants(headword, label_text):
        position = cleaned.find(variant)
        if position < 0:
            continue
        start = max(0, position - max(0, (limit - len(variant)) // 2))
        end = min(len(cleaned), start + limit)
        start = max(0, end - limit)
        prefix = "…" if start > 0 else ""
        suffix = "…" if end < len(cleaned) else ""
        return prefix + cleaned[start:end].strip() + suffix
    return truncate_excerpt(cleaned, limit)


def textbook_content_target_allowed(headword: str, label_text: str, note_text: str, gloss: str, answer_text: str = "") -> bool:
    normalized_label = textbook_primary_target(headword, label_text, note_text)
    compact_label = compact_hanzi_text(normalized_label)
    compact_raw_label = compact_hanzi_text(label_text)
    compact_headword = compact_hanzi_text(headword)
    combined = " ".join(
        clean_text(item)
        for item in (label_text, note_text, gloss, answer_text)
        if clean_text(item)
    )
    if not compact_label:
        return False
    if (
        normalized_label in TEXTBOOK_CONTENT_EXACT_SKIP
        or compact_label in TEXTBOOK_CONTENT_EXACT_SKIP
        or compact_raw_label in TEXTBOOK_CONTENT_EXACT_SKIP
        or compact_headword in TEXTBOOK_CONTENT_EXACT_SKIP
    ):
        return False
    if TEXTBOOK_NON_MEANINGFUL_HEADWORD_RE.fullmatch(compact_label):
        return False
    if TEXTBOOK_CONTENT_TARGET_BAD_RE.search(combined):
        return False
    return True


def textbook_content_phrase_like(headword: str, label_text: str, note_text: str) -> bool:
    raw_label = normalize_label_headword(label_text)
    if raw_label and len(raw_label) > 2:
        return True
    normalized_label = raw_label or normalize_label_headword(headword)
    if not normalized_label:
        return True
    if len(normalized_label) > 2:
        return True
    note_target = normalize_label_headword(leading_note_annotation(note_text))
    if note_target and note_target != normalized_label:
        if normalized_label in note_target or len(note_target) > len(normalized_label):
            return True
    return False


TEXTBOOK_SPLIT_TARGET_STOPWORDS = {
    "一说",
    "意思",
    "即",
    "是",
    "指",
    "为",
    "作",
}


def extract_textbook_split_targets(label_text: str, note_text: str, sentence: str) -> list[dict[str, str]]:
    sentence_compact = compact_hanzi_text(sentence)
    label_compact = compact_hanzi_text(label_text)
    if len(label_compact) <= 2:
        return []
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r"(?:^|[。；;])\s*([\u4e00-\u9fff]{1,2})[，,]\s*([^。；;]{1,48})", clean_text(note_text)):
        headword = normalize_label_headword(match.group(1))
        if not headword or headword in TEXTBOOK_SPLIT_TARGET_STOPWORDS:
            continue
        if headword not in sentence_compact:
            continue
        gloss = clean_text(match.group(2))
        gloss = re.sub(r"^(?:即|就是|是|指|作|为)\s*", "", gloss)
        gloss = gloss.strip(" ：:，,。；;")
        if not gloss or len(gloss) > 30:
            continue
        if CONTENT_GLOSS_BAD_HINT_RE.search(gloss) or CONTENT_DICT_GLOSS_BAD_RE.search(gloss):
            continue
        key = f"{headword}:{gloss}"
        if key in seen:
            continue
        seen.add(key)
        results.append(
            {
                "headword": headword,
                "label_text": headword,
                "gloss": gloss,
                "note_text": f"{headword}，{gloss}。",
            }
        )
    return results


def infer_textbook_term_kind(headword: str, label_text: str, note_text: str, gloss: str) -> str:
    cleaned_note = clean_text(note_text)
    cleaned_gloss = clean_text(gloss)
    has_grammar_hint = bool(
        TEXTBOOK_FUNCTION_GRAMMAR_RE.search(cleaned_note) or TEXTBOOK_FUNCTION_GRAMMAR_RE.search(cleaned_gloss)
    )
    has_value_hint = bool(
        TEXTBOOK_FUNCTION_VALUE_RE.search(cleaned_note) or TEXTBOOK_FUNCTION_VALUE_RE.search(cleaned_gloss)
    )
    if headword in TEXTBOOK_FUNCTION_HEADWORD_ALLOW and (has_grammar_hint or has_value_hint):
        return "function_word"
    if len(headword) == 1 and has_grammar_hint:
        return "function_word"
    return "content_word"


def textbook_ref_is_reliable(term_kind: str, headword: str, label_text: str, note_text: str, gloss: str, sentence: str) -> bool:
    cleaned_note = clean_text(note_text)
    cleaned_gloss = clean_text(gloss)
    cleaned_sentence = clean_text(sentence)
    normalized_label = normalize_label_headword(label_text)
    explicit_targets = extract_textbook_note_explicit_targets(note_text)
    if not headword or not cleaned_gloss or not cleaned_sentence:
        return False
    if not textbook_target_in_text(cleaned_sentence, headword, label_text):
        return False
    if "仅供个人学习使用" in cleaned_note or "人民教育出版社" in cleaned_note:
        return False
    if term_kind == "function_word":
        if TEXTBOOK_FUNCTION_BAD_HINT_RE.search(cleaned_note) or TEXTBOOK_FUNCTION_BAD_HINT_RE.search(cleaned_gloss):
            return False
        if len(cleaned_gloss) > 18:
            return False
        if (
            len(cleaned_note) > 64
            and not TEXTBOOK_FUNCTION_GRAMMAR_RE.search(cleaned_note)
            and clean_text(headword) not in explicit_targets
        ):
            return False
        if not (
            TEXTBOOK_FUNCTION_GRAMMAR_RE.search(cleaned_note)
            or TEXTBOOK_FUNCTION_GRAMMAR_RE.search(cleaned_gloss)
            or TEXTBOOK_FUNCTION_VALUE_RE.search(cleaned_note)
            or TEXTBOOK_FUNCTION_VALUE_RE.search(cleaned_gloss)
            or clean_text(headword) in explicit_targets
        ):
            return False
    else:
        if textbook_content_phrase_like(headword, label_text, note_text):
            return False
        if headword in COMMON_XUCI_HEADWORDS and normalized_label == headword and not re.match(rf"^{re.escape(headword)}[，,:：]", cleaned_note):
            return False
        if len(cleaned_gloss) > 30:
            return False
        if not textbook_content_target_allowed(headword, label_text, note_text, gloss, cleaned_gloss):
            return False
    if normalized_label and len(normalized_label) > 12:
        return False
    return True


def gloss_tokens(value: str) -> set[str]:
    cleaned = clean_text(value)
    if not cleaned:
        return set()
    pieces = re.split(r"[，,、；;。：:\s（）()“”\"'‘’]+", cleaned)
    return {piece for piece in pieces if len(piece) >= 2}


def textbook_support_matches(headword: str, target_gloss: str, ref: dict[str, Any]) -> bool:
    if clean_text(str(ref.get("headword") or "")) != clean_text(headword):
        return False
    reference_gloss = clean_text(str(ref.get("gloss") or ref.get("note_block") or ""))
    if not reference_gloss:
        return False
    target_cleaned = clean_text(target_gloss)
    if not target_cleaned:
        return False
    if target_cleaned in reference_gloss or reference_gloss in target_cleaned:
        return True
    return bool(gloss_tokens(target_cleaned) & gloss_tokens(reference_gloss))


def filter_matching_textbook_support(headword: str, target_gloss: str, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [ref for ref in refs if textbook_support_matches(headword, target_gloss, ref)]


def looks_like_number_combo_options(direct_options: list[tuple[str, str]]) -> bool:
    texts = [clean_text(text) for _label, text in direct_options]
    return len(texts) == 4 and all(NUMBER_COMBO_OPTION_RE.fullmatch(text or "") for text in texts)


def build_textbook_sections() -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    book_meta = resolve_language_book_meta()
    book_paths = resolve_language_book_paths(book_meta)
    manifest = load_json(MANIFEST_PATH)
    sections: list[dict[str, Any]] = []
    refs_by_term: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_refs: dict[str, set[tuple[str, str, str, str]]] = defaultdict(set)

    for book_key, md_path in sorted(book_paths.items()):
        raw_text = md_path.read_text(encoding="utf-8")
        school_stage = "高中" if book_key.startswith("高中_") else "初中"
        book_title = book_meta.get(book_key, {}).get("display_title") or book_meta.get(book_key, {}).get("title") or book_key
        manifest_items = [item for item in manifest.get(book_key, []) if str(item.get("title") or "").strip()]
        all_title_targets = {
            normalize_title(part)
            for item in manifest_items
            for part in title_part_variants(str(item.get("title") or ""))
            if normalize_title(part)
        }
        if not manifest_items:
            continue
        for item in manifest_items:
            title = str(item.get("title") or "").strip()
            for title_part in title_part_variants(title):
                section_text = locate_section_by_title(raw_text, title_part, all_title_targets)
                if not section_text:
                    continue
                body_text, note_lines = split_section_body_and_notes(section_text)
                if not body_text:
                    continue
                parsed_notes = parse_note_entries(note_lines)
                if not parsed_notes:
                    continue
                note_entries = []
                for note in parsed_notes:
                    headword = extract_note_headword(note["label_text"], note["note_text"])
                    gloss = summarize_note_gloss(headword, note["note_text"])
                    if not headword or not gloss:
                        continue
                    term_kind = infer_textbook_term_kind(
                        headword,
                        note["label_text"],
                        note["note_text"],
                        gloss,
                    )
                    if term_kind == "function_word":
                        gloss = summarize_textbook_function_gloss(headword, note["note_text"], gloss)
                        if not gloss:
                            continue
                    note_entries.append(
                        {
                            "headword": headword,
                            "label_text": note["label_text"],
                            "note_text": note["note_text"],
                            "gloss": gloss,
                        }
                    )
                if not note_entries:
                    continue
                author, dynasty = parse_author_from_section(section_text)
                section_record = {
                    "book_key": book_key,
                    "book_title": book_title,
                    "school_stage": school_stage,
                    "title": title_part,
                    "kind": str(item.get("kind") or ""),
                    "author": author,
                    "dynasty": dynasty,
                    "page_start": item.get("page_start"),
                    "page_end": item.get("page_end"),
                    "body_text": body_text,
                    "note_entries": note_entries,
                }
                sections.append(section_record)
                body_cursor = 0
                for note_index, note in enumerate(note_entries, start=1):
                    term_kind = infer_textbook_term_kind(
                        note["headword"],
                        note["label_text"],
                        note["note_text"],
                        note["gloss"],
                    )
                    term_id = f"{'function' if term_kind == 'function_word' else 'content'}::{note['headword']}"
                    probe_text, probe_position = locate_progressive_probe(
                        body_text,
                        note["label_text"],
                        note["headword"],
                        body_cursor,
                    )
                    if probe_position >= 0:
                        body_cursor = probe_position + len(probe_text)
                    context_window = (
                        context_window_from_position(body_text, probe_position)
                        if probe_position >= 0
                        else best_context_window(body_text, probe_text, note["headword"])
                    )
                    sentence = ""
                    if context_window:
                        if probe_position >= 0:
                            exact_window = split_context_units_with_offsets(body_text)
                            sentence = next(
                                (
                                    str(unit["text"])
                                    for unit in exact_window
                                    if int(unit["start"]) <= probe_position < int(unit["end"])
                                ),
                                "",
                            )
                        if not sentence:
                            sentence = next(
                                (
                                    unit
                                    for unit in context_window
                                    if probe_text in unit or note["headword"] in unit or note["label_text"] in unit
                                ),
                                context_window[min(len(context_window) // 2, len(context_window) - 1)],
                            )
                    if not textbook_ref_is_reliable(
                        term_kind,
                        note["headword"],
                        note["label_text"],
                        note["note_text"],
                        note["gloss"],
                        sentence,
                    ):
                        continue
                    ref_dedupe_key = (
                        normalize_title(title_part),
                        clean_text(sentence),
                        clean_text(note["gloss"]),
                        clean_text(note["headword"]),
                    )
                    if ref_dedupe_key in seen_refs[term_id]:
                        continue
                    seen_refs[term_id].add(ref_dedupe_key)
                    display_sentence = best_textbook_focus_sentence(
                        sentence,
                        context_window,
                        note["headword"],
                        note["label_text"],
                    )
                    display_context = [
                        truncate_excerpt_around_target(item_text, note["headword"], note["label_text"], 160)
                        for item_text in context_window[:7]
                    ]
                    if display_sentence and display_sentence not in display_context:
                        display_context.insert(min(len(display_context), 3), truncate_excerpt_around_target(display_sentence, note["headword"], note["label_text"], 160))
                        display_context = display_context[:7]
                    refs_by_term[term_id].append(
                        {
                            "ref_id": f"{term_id}:{stable_slug(book_key)}:{stable_slug(title_part)}:{note_index}",
                            "school_stage": school_stage,
                            "book_key": book_key,
                            "title": title_part,
                            "kind": str(item.get("kind") or ""),
                            "page_start": item.get("page_start"),
                            "page_end": item.get("page_end"),
                            "sentence": truncate_excerpt_around_target(display_sentence or sentence, note["headword"], note["label_text"], 160),
                            "context_window": display_context,
                            "note_block": note["note_text"],
                            "author": author,
                            "dynasty": dynasty,
                            "book_title": book_title,
                            "headword": note["headword"],
                            "label_text": note["label_text"],
                            "gloss": note["gloss"],
                            "answer_text": (
                                note["gloss"]
                                if term_kind == "function_word"
                                else build_textbook_answer_text(note["label_text"], note["note_text"], note["gloss"], note["headword"])
                            ),
                            "dict_headwords": derive_textbook_dict_headwords(
                                {"headword": note["headword"], "label_text": note["label_text"]},
                                note["headword"],
                            ),
                        }
                    )
                continue
    return sections, {key: value[:20] for key, value in refs_by_term.items()}


def textbook_school_stage(book_key: str) -> str:
    return "高中" if str(book_key).startswith("高中_") else "初中"


def load_textbook_source_master_tables() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    missing = [str(path) for path in (TEXTBOOK_ARTICLE_MASTER_PATH, TEXTBOOK_NOTE_MASTER_PATH) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing textbook source master tables: "
            + ", ".join(missing)
            + ". Run scripts/build_textbook_source_tables.py before runtime_generation_v2.py."
        )
    article_rows = load_json(TEXTBOOK_ARTICLE_MASTER_PATH)
    note_rows = load_json(TEXTBOOK_NOTE_MASTER_PATH)
    if not isinstance(article_rows, list) or not isinstance(note_rows, list):
        raise ValueError("Textbook source master tables must be JSON arrays.")
    return article_rows, note_rows


def load_textbook_sections_from_master_tables(
    article_rows: list[dict[str, Any]] | None = None,
    note_rows: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    if article_rows is None or note_rows is None:
        article_rows, note_rows = load_textbook_source_master_tables()
    sections: list[dict[str, Any]] = []
    article_meta: dict[tuple[str, str], dict[str, Any]] = {}

    for row in article_rows:
        book_key = clean_text(str(row.get("book_key") or ""))
        title = clean_text(str(row.get("title") or ""))
        if not book_key or not title:
            continue
        section = {
            "book_key": book_key,
            "book_title": clean_text(str(row.get("book_title") or "")),
            "school_stage": textbook_school_stage(book_key),
            "title": title,
            "kind": clean_text(str(row.get("kind") or "")),
            "author": clean_text(str(row.get("author") or "")),
            "dynasty": clean_text(str(row.get("dynasty") or "")),
            "page_start": row.get("page_start_print"),
            "page_end": row.get("page_end_print"),
            "body_text": clean_text_keep_newlines(str(row.get("full_text") or row.get("ocr_full_text") or "")),
            "body_source_mode": clean_text(str(row.get("body_source_mode") or "")),
            "body_source_title": clean_text(str(row.get("body_source_title") or "")),
            "body_source_path": clean_text(str(row.get("body_source_path") or "")),
            "manifest_title": clean_text(str(row.get("manifest_title") or "")),
            "note_entries": [],
        }
        sections.append(section)
        article_meta[(book_key, title)] = section

    refs_by_term: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_refs: dict[str, set[tuple[str, str, str, str]]] = defaultdict(set)
    for row in note_rows:
        if str(row.get("match_status") or "") != "matched":
            continue
        if clean_text(str(row.get("annotation_scope") or "body")) != "body":
            continue
        book_key = clean_text(str(row.get("book_key") or ""))
        title = clean_text(str(row.get("title") or ""))
        raw_headword = clean_text(str(row.get("headword") or ""))
        label_text = clean_text(str(row.get("label_text") or ""))
        note_text = clean_text(str(row.get("note_text") or ""))
        raw_gloss = clean_text(str(row.get("gloss") or ""))
        initial_headword = extract_note_headword(label_text, note_text) or raw_headword
        initial_gloss = raw_gloss or summarize_note_gloss(initial_headword, note_text)
        initial_kind = infer_textbook_term_kind(initial_headword, label_text, note_text, initial_gloss)
        headword = resolve_textbook_bank_headword(row, initial_headword, initial_kind)
        if not headword:
            continue
        sentence = clean_text(str(row.get("source_sentence") or ""))
        gloss = raw_gloss or summarize_note_gloss(headword, note_text)
        term_kind = infer_textbook_term_kind(headword, label_text, note_text, gloss)
        candidates = [
            {
                "headword": headword,
                "label_text": label_text,
                "note_text": note_text,
                "gloss": gloss,
                "term_kind": term_kind,
                "split_index": 0,
                "answer_text": clean_text(str(row.get("answer_text") or "")),
            }
        ]
        if term_kind == "content_word" and textbook_content_phrase_like(headword, label_text, note_text):
            candidates = [
                {
                    **candidate,
                    "term_kind": "content_word",
                    "split_index": split_index,
                    "answer_text": candidate["gloss"],
                }
                for split_index, candidate in enumerate(extract_textbook_split_targets(label_text, note_text, sentence), start=1)
            ]
        for candidate in candidates:
            candidate_headword = clean_text(str(candidate.get("headword") or ""))
            candidate_label_text = clean_text(str(candidate.get("label_text") or candidate_headword))
            candidate_note_text = clean_text(str(candidate.get("note_text") or note_text))
            candidate_gloss = clean_text(str(candidate.get("gloss") or ""))
            candidate_kind = clean_text(str(candidate.get("term_kind") or term_kind))
            if candidate_kind == "function_word":
                candidate_gloss = summarize_textbook_function_gloss(candidate_headword, candidate_note_text, candidate_gloss)
                if not candidate_gloss:
                    continue
            if not (book_key and title and candidate_headword and candidate_gloss and sentence):
                continue
            if not textbook_ref_is_reliable(
                candidate_kind,
                candidate_headword,
                candidate_label_text,
                candidate_note_text,
                candidate_gloss,
                sentence,
            ):
                continue
            raw_context_window = [clean_text(str(item)) for item in list(row.get("context_window") or []) if clean_text(str(item))][:7]
            focus_sentence = best_textbook_focus_sentence(sentence, raw_context_window, candidate_headword, candidate_label_text)
            context_window = [
                truncate_excerpt_around_target(item, candidate_headword, candidate_label_text, 160)
                for item in raw_context_window
            ]
            focus_entry = truncate_excerpt_around_target(focus_sentence or sentence, candidate_headword, candidate_label_text, 160)
            if focus_entry and focus_entry not in context_window:
                focus_index = int(row.get("context_focus_index") or 0)
                if 0 <= focus_index < len(context_window):
                    context_window[focus_index] = focus_entry
                else:
                    context_window.insert(min(len(context_window), 3), focus_entry)
                    context_window = context_window[:7]
            term_id = f"{'function' if candidate_kind == 'function_word' else 'content'}::{candidate_headword}"
            ref_dedupe_key = (
                normalize_title(title),
                clean_text(focus_sentence or sentence),
                clean_text(candidate_gloss),
                clean_text(candidate_headword),
            )
            if ref_dedupe_key in seen_refs[term_id]:
                continue
            seen_refs[term_id].add(ref_dedupe_key)
            article = article_meta.get((book_key, title), {})
            split_suffix = f":s{int(candidate.get('split_index') or 0)}" if int(candidate.get("split_index") or 0) else ""
            answer_text = (
                candidate_gloss
                if candidate_kind == "function_word"
                else clean_text(
                    str(
                        candidate.get("answer_text")
                        or build_textbook_answer_text(candidate_label_text, candidate_note_text, candidate_gloss, candidate_headword)
                    )
                )
            )
            refs_by_term[term_id].append(
                {
                    "ref_id": f"{term_id}:{stable_slug(book_key)}:{stable_slug(title)}:{int(row.get('note_order') or 0)}{split_suffix}",
                    "school_stage": article.get("school_stage") or textbook_school_stage(book_key),
                    "book_key": book_key,
                    "title": title,
                    "kind": clean_text(str(row.get("kind") or article.get("kind") or "")),
                    "page_start": row.get("page_start_print") if row.get("page_start_print") is not None else article.get("page_start"),
                    "page_end": row.get("page_end_print") if row.get("page_end_print") is not None else article.get("page_end"),
                    "sentence": focus_entry,
                    "context_window": context_window,
                    "note_block": candidate_note_text,
                    "author": clean_text(str(article.get("author") or "")),
                    "dynasty": clean_text(str(article.get("dynasty") or "")),
                    "book_title": clean_text(str(row.get("book_title") or article.get("book_title") or "")),
                    "headword": candidate_headword,
                    "label_text": candidate_label_text,
                    "gloss": candidate_gloss,
                    "answer_text": answer_text,
                    "dict_headwords": unique_clean_strings(
                        [candidate_headword]
                        + [clean_text(str(item)) for item in list(row.get("dict_headwords") or []) if clean_text(str(item))]
                    ),
                    "manifest_title": clean_text(str(row.get("manifest_title") or article.get("manifest_title") or "")),
                    "note_order": row.get("note_order"),
                    "body_source_mode": clean_text(str(row.get("body_source_mode") or article.get("body_source_mode") or "")),
                    "body_source_title": clean_text(str(row.get("body_source_title") or article.get("body_source_title") or "")),
                    "body_source_path": clean_text(str(row.get("body_source_path") or article.get("body_source_path") or "")),
                    "match_status": clean_text(str(row.get("match_status") or "")),
                    "match_mode": clean_text(str(row.get("match_mode") or "")),
                    "match_confidence": row.get("match_confidence"),
                    "source_sentence": focus_entry,
                    "md_path": clean_text(str(row.get("md_path") or "")),
                    "middle_path": clean_text(str(row.get("middle_path") or "")),
                }
            )
    return sections, {key: value[:20] for key, value in refs_by_term.items()}


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
                SELECT id, headword, content_text
                FROM entries
                WHERE headword = ?
                ORDER BY id ASC
                LIMIT 3
                """,
                (headword,),
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


def infer_basis_records(raw_occurrence: dict[str, Any], headword: str) -> list[dict[str, Any]]:
    basis_type = QUESTION_TYPE_TO_BASIS.get(str(raw_occurrence.get("question_subtype") or ""), "direct_choice")
    evidence = truncate_excerpt(str(raw_occurrence.get("excerpt") or ""))
    year = raw_occurrence.get("year")
    question_number = raw_occurrence.get("question_number")
    return [
        {
            "basis_type": basis_type,
            "exam_year": year,
            "question_number": question_number,
            "evidence_sentence": evidence,
            "answer_span": truncate_excerpt(str(raw_occurrence.get("gloss") or raw_occurrence.get("option_label") or "")),
            "why_required": f"{year} 年真题中直接出现 {headword} 的考查证据。",
            "confidence": 0.92 if basis_type == "direct_choice" else 0.84,
            "needs_manual_review": False,
        }
    ]


def build_union_term_records(
    raw_exam_terms: list[dict[str, Any]],
    textbook_refs: dict[str, list[dict[str, Any]]],
    revised_links: dict[str, list[dict[str, Any]]],
    idiom_links: dict[str, list[dict[str, Any]]],
    function_usage_catalog: dict[str, list[dict[str, Any]]],
    kind: str,
) -> list[dict[str, Any]]:
    exam_index = {str(term.get("headword") or ""): term for term in raw_exam_terms}
    all_headwords = set(exam_index)
    prefix = "function::" if kind == "function_word" else "content::"
    for term_id in textbook_refs:
        if term_id.startswith(prefix):
            all_headwords.add(term_id.split("::", 1)[1])
    records: list[dict[str, Any]] = []
    for headword in sorted(all_headwords):
        exam_term = exam_index.get(headword, {})
        term_id = f"{'function' if kind == 'function_word' else 'content'}::{headword}"
        occurrences = list(exam_term.get("occurrences") or [])
        bases: list[dict[str, Any]] = []
        for occurrence in occurrences:
            bases.extend(infer_basis_records(occurrence, headword))
        term_textbook_refs = textbook_refs.get(term_id, [])
        note_glosses = unique_clean_strings([str(ref.get("gloss") or "") for ref in term_textbook_refs])
        exam_glosses = unique_clean_strings(
            [
                clean_gloss(headword, str(item.get("gloss") or ""), str(item.get("excerpt") or ""))
                for item in occurrences
            ]
        )
        sample_glosses = unique_clean_strings(exam_glosses + note_glosses)
        beijing_occurrences = sum(1 for item in occurrences if item.get("scope") == "beijing")
        national_occurrences = sum(1 for item in occurrences if item.get("scope") == "national")
        years = sorted({int(item.get("year")) for item in occurrences if isinstance(item.get("year"), int)})
        question_type_counts = Counter(str(item.get("question_subtype") or "") for item in occurrences if str(item.get("question_subtype") or ""))
        if term_textbook_refs:
            question_type_counts["textbook_note"] += len(term_textbook_refs)
        if kind == "content_word":
            priority_level = CONTENT_PRIORITY_CORE if beijing_occurrences and any(ref.get("school_stage") == "高中" for ref in term_textbook_refs) else CONTENT_PRIORITY_SECONDARY
            usage_relations = [{"semantic_value": gloss, "evidence_count": 1} for gloss in sample_glosses[:8]]
        else:
            priority_level = CONTENT_PRIORITY_CORE if beijing_occurrences and any(ref.get("school_stage") == "高中" for ref in term_textbook_refs) else FUNCTION_PRIORITY_SUPPORT
            usage_relations = function_usage_catalog.get(headword, []) or [{"semantic_value": gloss, "evidence_count": 1} for gloss in sample_glosses[:8]]
        dict_refs = select_textbook_dict_links(headword, term_textbook_refs, revised_links)
        records.append(
            {
                "term_id": term_id,
                "kind": kind,
                "headword": headword,
                "display_headword": headword,
                "must_master": beijing_occurrences > 0,
                "must_master_basis": bases,
                "beijing_frequency": beijing_occurrences,
                "national_frequency": national_occurrences,
                "year_range": [years[0], years[-1]] if years else [None, None],
                "question_type_counts": dict(question_type_counts),
                "frequencies": {
                    "total": len(occurrences),
                    "beijing": beijing_occurrences,
                    "national": national_occurrences,
                },
                "usage_relations": usage_relations,
                "sample_glosses": sample_glosses[:8],
                "textbook_refs": term_textbook_refs[:8],
                "dict_refs": dict_refs,
                "idiom_refs": idiom_links.get(headword, [])[:3],
                "priority_level": priority_level,
                "needs_manual_review": not (term_textbook_refs or dict_refs or idiom_links.get(headword)),
            }
        )
    return records


def extract_exam_article_title(source_passage: str) -> str:
    lines = [clean_text(line.strip("* ")) for line in clean_text_keep_newlines(source_passage).splitlines() if clean_text(line)]
    filtered = [
        line
        for line in lines
        if not re.search(r"阅读下面|完成\d|共\d+分|本大题|下面小题", line)
        and not re.match(r"^[一二三四五六七八九十]+、", line)
        and not re.match(r"^第[一二三四五六七八九十百\d]+[题部分大题]", line)
        and not line.startswith("![](")
    ]
    for line in reversed(filtered[-4:]):
        if re.fullmatch(r"[（(].*取材于《.+》.*[)）]", line):
            return line
    for line in filtered[:8]:
        if 1 <= len(line) <= 20 and not any(mark in line for mark in ("。", "？", "！", "；", "：", ":")):
            return line
    for line in filtered[:6]:
        sentence = re.split(r"[。！？]", line, maxsplit=1)[0]
        if classical_marker_score(sentence) >= 1 and len(sentence) >= 6:
            return truncate_excerpt(sentence, 22)
    return truncate_excerpt(filtered[0], 22) if filtered else ""


def build_exam_occurrence_lookup(
    function_terms: list[dict[str, Any]],
    content_terms: list[dict[str, Any]],
) -> dict[tuple[str, int, str], dict[str, Any]]:
    lookup: dict[tuple[str, int, str], dict[str, Any]] = {}
    for term in function_terms:
        raw_headword = str(term.get("headword") or "")
        for occurrence in list(term.get("occurrences") or []):
            if occurrence.get("scope") != "beijing":
                continue
            paper_key = str(occurrence.get("paper_key") or "")
            question_number = int(occurrence.get("question_number") or 0)
            option_label = clean_text(str(occurrence.get("option_label") or ""))
            if not paper_key or not question_number or option_label not in {"A", "B", "C", "D"}:
                continue
            normalized = normalize_exam_occurrence(raw_headword, occurrence)
            normalized["kind_hint"] = "function_word"
            key = (paper_key, question_number, option_label)
            current = lookup.get(key)
            if not current or len(str(normalized.get("headword") or "")) < len(str(current.get("headword") or "")):
                lookup[key] = normalized
    for term in content_terms:
        raw_headword = str(term.get("headword") or "")
        for occurrence in list(term.get("occurrences") or []):
            if occurrence.get("scope") != "beijing":
                continue
            paper_key = str(occurrence.get("paper_key") or "")
            question_number = int(occurrence.get("question_number") or 0)
            option_label = clean_text(str(occurrence.get("option_label") or ""))
            if not paper_key or not question_number or option_label not in {"A", "B", "C", "D"}:
                continue
            normalized = normalize_exam_occurrence(raw_headword, occurrence)
            normalized["kind_hint"] = "content_word"
            key = (paper_key, question_number, option_label)
            current = lookup.get(key)
            if not current or len(str(normalized.get("headword") or "")) < len(str(current.get("headword") or "")):
                lookup[key] = normalized
    return lookup


def build_option_analysis_for_direct(
    prompt_text: str,
    option: dict[str, Any],
    is_correct_option: bool,
    correct_label: str,
    correct_option: dict[str, Any],
    solution_note: dict[str, str],
) -> str:
    headword = str(option.get("headword") or "")
    gloss = str(option.get("gloss") or option.get("text") or "")
    sentence = clean_text(str(option.get("sentence") or ""))
    incorrect_prompt = "不正确" in prompt_text or "不同的一项" in prompt_text
    if incorrect_prompt:
        if is_correct_option:
            actual_gloss = clean_text(str(solution_note.get("actual_gloss") or ""))
            pos = clean_text(str(solution_note.get("part_of_speech") or ""))
            reason = clean_text(str(solution_note.get("reason") or ""))
            pieces = [f"本项是题目的误释项。"]
            if sentence:
                pieces.append(f"原句“{sentence}”。")
            if headword and actual_gloss:
                pieces.append(f"句中“{headword}”应作“{actual_gloss}”讲。")
            if pos:
                pieces.append(f"词性宜落实为{pos}。")
            if gloss:
                pieces.append(f"题项把它误解成“{gloss}”。")
            if reason:
                pieces.append(reason)
            return " ".join(pieces)
        if headword and gloss:
            if sentence:
                return f"本项解释成立。原句“{sentence}”里，“{headword}”在这里就是“{gloss}”的意思。"
            return f"本项解释成立。句中“{headword}”在这里就是“{gloss}”的意思。"
        return f"本项解释成立，不符合题干要求的“{clean_text(prompt_text)}”。"
    if is_correct_option:
        if headword and gloss:
            if sentence:
                return f"本项成立。原句“{sentence}”里，“{headword}”在这里应解释为“{gloss}”。"
            return f"本项成立。句中“{headword}”在这里应解释为“{gloss}”。"
        return f"本项成立，符合题干要求。"
    correct_headword = str(correct_option.get("headword") or "")
    correct_gloss = str(correct_option.get("gloss") or correct_option.get("text") or "")
    if headword and gloss and correct_headword == headword and correct_gloss:
        if sentence:
            return f"本项不当。原句“{sentence}”里，“{headword}”不作“{gloss}”讲，更稳妥的解释是“{correct_gloss}”。"
        return f"本项不当。句中“{headword}”不作“{gloss}”讲，更稳妥的解释是“{correct_gloss}”。"
    return f"本项不是正确答案，标准答案为 {correct_label}。"


def build_challenge_explanation(
    prompt_text: str,
    source_label: str,
    correct_option: dict[str, Any],
    solution_note: dict[str, str],
    dict_links: list[dict[str, Any]],
    textbook_links: list[dict[str, Any]],
) -> str:
    incorrect_prompt = "不正确" in prompt_text or "不同的一项" in prompt_text
    pieces = [f"题源：{source_label}。"]
    headword = str(correct_option.get("headword") or "")
    gloss = str(correct_option.get("gloss") or correct_option.get("text") or "")
    sentence = clean_text(str(correct_option.get("sentence") or ""))
    manual_dict_support = build_manual_dict_support(solution_note)
    if sentence:
        pieces.append(f"原句：{sentence}。")
    if incorrect_prompt and solution_note:
        actual_gloss = clean_text(str(solution_note.get("actual_gloss") or ""))
        pos = clean_text(str(solution_note.get("part_of_speech") or ""))
        reason = clean_text(str(solution_note.get("reason") or ""))
        if headword and actual_gloss:
            pieces.append(f"题目要求找出误释项；句中“{headword}”真正的意思是“{actual_gloss}”。")
        if pos:
            pieces.append(f"词性应落实为{pos}。")
        if gloss:
            pieces.append(f"题项把它误作“{gloss}”。")
        if reason:
            pieces.append(reason)
    elif headword and gloss:
        pieces.append(f"正确项所对应的释义是“{headword}＝{gloss}”。")
    dict_candidates = manual_dict_support or dict_links
    if dict_candidates:
        pieces.append(f"辞典参照：{truncate_excerpt(str(dict_candidates[0].get('summary') or ''), 90)}。")
    if textbook_links:
        support = str(textbook_links[0].get("note_block") or textbook_links[0].get("sentence") or "")
        if support:
            pieces.append(f"教材可参照：{truncate_excerpt(support, 90)}。")
    return " ".join(piece for piece in pieces if piece)


def parse_beijing_exam_bank(
    question_docs: dict[str, dict[str, Any]],
    answer_overrides: dict[str, dict[str, str]],
    solution_notes: dict[str, dict[str, str]],
    option_overrides: dict[str, dict[str, str]],
    record_by_term: dict[str, dict[str, Any]],
    occurrence_lookup: dict[tuple[str, int, str], dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    bank: dict[str, list[dict[str, Any]]] = {
        "xuci_pair_compare": [],
        "function_gloss": [],
        "function_profile": [],
        "content_gloss": [],
        "sentence_meaning": [],
        "passage_meaning": [],
    }
    answer_keys: dict[str, dict[str, Any]] = {}
    exam_docs: list[dict[str, Any]] = []

    for paper_key, qdoc in sorted(
        question_docs.items(),
        key=lambda item: (int(item[1].get("year") or 0), str(item[0])),
    ):
        if "beijing-" not in paper_key:
            continue
        year = int(qdoc.get("year") or 0)
        if year < 2002 or year > 2025:
            continue
        text = _normalize_text(str(qdoc.get("text") or ""))
        answer_text = _normalize_text(str(qdoc.get("answer") or ""))
        source_passage = extract_source_passage(text)
        article_title = extract_exam_article_title(source_passage)
        question_records = []
        for question_number, block in _iter_question_blocks(text):
            block_body = _strip_question_prefix(block)
            prompt_text = question_prompt_text(block_body)
            subtype = _detect_block_subtype(block_body)
            if not subtype:
                continue
            if subtype.startswith("xuci_compare"):
                answer_label = answer_label_for_source(answer_overrides, paper_key, question_number, answer_text)
                if answer_label not in {"A", "B", "C", "D"}:
                    continue
                options = []
                term_ids: list[str] = []
                for option_label, option_text in _split_options(block_body):
                    occurrence = occurrence_lookup.get((paper_key, question_number, option_label), {})
                    excerpt_with_marks = clean_text_keep_newlines(str(occurrence.get("excerpt") or ""))
                    tokens = _extract_emphasis_tokens(excerpt_with_marks or option_text)
                    headword = clean_text(tokens[0]) if tokens else clean_text(str(occurrence.get("headword") or ""))
                    source_for_split = excerpt_with_marks or option_text
                    sentences = extract_marked_compare_sentences(source_for_split, headword)
                    if len(sentences) < 2 or not headword:
                        options = []
                        break
                    contexts = [
                        [truncate_excerpt(item, 120) for item in best_context_window(source_passage, sentence, headword)]
                        for sentence in sentences[:2]
                    ]
                    term_id = f"function::{headword}"
                    term_ids.append(term_id)
                    options.append(
                        {
                            "label": option_label,
                            "term_id": term_id,
                            "headword": headword,
                            "sentences": [truncate_excerpt(sentence, 90) for sentence in sentences[:2]],
                            "sentence_contexts": contexts,
                        }
                    )
                if len(options) != 4:
                    continue
                challenge_id = f"xuci-{stable_slug(paper_key)}-q{question_number}"
                source_label = f"{year} 年北京卷《{article_title or str(qdoc.get('title') or '')}》第 {question_number} 题"
                bank["xuci_pair_compare"].append(
                    {
                        "challenge_id": challenge_id,
                        "question_type": "xuci_pair_compare",
                        "kind": "function_word",
                        "source_kind": "exam",
                        "term_id": options[0]["term_id"],
                        "term_ids": unique_clean_strings(term_ids),
                        "priority_level": CONTENT_PRIORITY_CORE,
                        "paper_key": paper_key,
                        "year": year,
                        "paper": qdoc.get("paper"),
                        "question_number": question_number,
                        "source_label": source_label,
                        "source_title": article_title or str(qdoc.get("title") or ""),
                        "stem": truncate_excerpt(prompt_text, 160),
                        "options": options,
                    }
                )
                explanation = f"{source_label}，正确答案为 {answer_label}。"
                if "相同" in prompt_text:
                    explanation += " 正确项中的两句虚词意义和用法相同。"
                elif "不同" in prompt_text:
                    explanation += " 正确项中的两句虚词意义和用法不同。"
                answer_keys[challenge_id] = {
                    "challenge_id": challenge_id,
                    "kind": "function_word",
                    "question_type": "xuci_pair_compare",
                    "term_id": options[0]["term_id"],
                    "term_ids": unique_clean_strings(term_ids),
                    "priority_level": CONTENT_PRIORITY_CORE,
                    "source_type": "exam",
                    "source_ref": {
                        "year": year,
                        "paper": qdoc.get("paper"),
                        "paper_key": paper_key,
                        "question_number": question_number,
                    },
                    "correct_label": answer_label,
                    "correct_text": next((option["headword"] for option in options if option["label"] == answer_label), ""),
                    "explanation": explanation,
                    "dict_support": [],
                    "textbook_support": [],
                    "option_analyses": [
                        {
                            "label": option["label"],
                            "text": option["headword"],
                            "is_correct": option["label"] == answer_label,
                            "analysis": (
                                f"本项符合题干要求。两句中的“{option['headword']}”{('意义和用法相同' if '相同' in prompt_text else '意义和用法不同')}。"
                                if option["label"] == answer_label
                                else f"本项不符合题干要求，标准答案不是 {option['label']}。"
                            ),
                        }
                        for option in options
                    ],
                }
                question_records.append({"question_number": question_number, "question_subtype": subtype, "challenge_id": challenge_id})
                continue

            explicit_headword_match = QUOTED_HEADWORD_RE.search(block_body)
            explicit_headword = clean_text(explicit_headword_match.group(1)) if explicit_headword_match else ""
            item_blocks = _split_items(block_body)
            unit_specs: list[dict[str, Any]] = []
            if item_blocks:
                for sub_index, item_block in item_blocks:
                    item_prompt, item_body_lines = _extract_prompt_and_body_lines(item_block)
                    if not item_body_lines:
                        continue
                    unit_specs.append(
                        {
                            "sub_index": sub_index,
                            "sentence": clean_text(item_prompt),
                            "stem": prompt_text,
                            "option_block": "\n".join(item_body_lines),
                        }
                    )
            else:
                _whole_prompt, body_lines = _extract_prompt_and_body_lines(block_body)
                if body_lines:
                    unit_specs.append(
                        {
                            "sub_index": 0,
                            "sentence": "",
                            "stem": prompt_text,
                            "option_block": "\n".join(body_lines),
                        }
                    )
            for unit_spec in unit_specs:
                sub_index = int(unit_spec["sub_index"])
                question_sentence = clean_text(unit_spec["sentence"])
                option_block = str(unit_spec["option_block"])
                unit_prompt = str(unit_spec["stem"])
                answer_label = answer_label_for_source(
                    answer_overrides,
                    paper_key,
                    question_number,
                    answer_text,
                    sub_index if sub_index else None,
                )
                if answer_label not in {"A", "B", "C", "D"}:
                    continue
                direct_options = _split_options(option_block)
                if len(direct_options) != 4:
                    continue
                if looks_like_number_combo_options(direct_options):
                    continue
                options_payload: list[dict[str, Any]] = []
                term_ids: list[str] = []
                question_headword = explicit_headword
                if not question_headword and question_sentence:
                    focus_tokens = _extract_emphasis_tokens(question_sentence)
                    if focus_tokens:
                        question_headword = clean_text(focus_tokens[0])
                for option_label, option_text in direct_options:
                    occurrence = occurrence_lookup.get((paper_key, question_number, option_label), {})
                    manual_key = exam_option_key(
                        paper_key,
                        question_number,
                        option_label,
                        sub_index if sub_index else None,
                    )
                    option_override = option_overrides.get(manual_key, {})
                    hinted_headword = clean_text(str(occurrence.get("headword") or question_headword or ""))
                    hinted_gloss = clean_direct_option_gloss(
                        hinted_headword,
                        str(option_override.get("gloss") or occurrence.get("display_gloss") or occurrence.get("gloss") or ""),
                    )
                    parsed = option_sentence_and_gloss(
                        option_text,
                        explicit_headword=explicit_headword or None,
                        hinted_headword=hinted_headword or None,
                        hinted_gloss=hinted_gloss or None,
                    )
                    headword = clean_text(str(option_override.get("headword") or parsed.get("headword") or hinted_headword or explicit_headword or ""))
                    question_headword = question_headword or headword
                    question_kind = str(occurrence.get("kind_hint") or ("function_word" if headword in COMMON_XUCI_HEADWORDS else "content_word"))
                    question_type = "function_gloss" if question_kind == "function_word" else "content_gloss"
                    term_id = f"{'function' if question_kind == 'function_word' else 'content'}::{headword}"
                    term_ids.append(term_id)
                    sentence_text = clean_text(str(option_override.get("sentence") or parsed.get("sentence") or ""))
                    gloss_text = clean_direct_option_gloss(
                        headword,
                        str(option_override.get("gloss") or parsed.get("gloss") or hinted_gloss or ""),
                    )
                    options_payload.append(
                        {
                            "label": option_label,
                            "term_id": term_id,
                            "headword": headword,
                            "sentence": truncate_excerpt(sentence_text, 120),
                            "context_window": [
                                truncate_excerpt(item, 120)
                                for item in best_context_window(source_passage, sentence_text or question_sentence, headword)
                            ],
                            "gloss": gloss_text,
                            "text": gloss_text or clean_text(option_text),
                        }
                    )
                if len(options_payload) != 4:
                    continue
                correct_option = next((option for option in options_payload if option["label"] == answer_label), options_payload[0])
                question_kind = "function_word" if str(correct_option.get("term_id") or "").startswith("function::") else "content_word"
                question_type = "function_gloss" if question_kind == "function_word" else "content_gloss"
                option_sentence_count = sum(1 for option in options_payload if clean_text(str(option.get("sentence") or "")))
                if question_kind == "content_word" and not all(
                    clean_text(str(option.get("sentence") or "")) and clean_text(str(option.get("text") or ""))
                    for option in options_payload
                ):
                    continue
                solution_key = f"{paper_key}#{question_number}#{answer_label}"
                if sub_index:
                    solution_key = f"{paper_key}#{question_number}#{sub_index}#{answer_label}"
                solution_note = solution_notes.get(solution_key, {})
                source_label = f"{year} 年北京卷《{article_title or str(qdoc.get('title') or '')}》第 {question_number} 题"
                challenge_id = (
                    f"{question_type}-{stable_slug(paper_key)}-{question_number}-{sub_index}"
                    if sub_index
                    else f"{question_type}-{stable_slug(paper_key)}-{question_number}"
                )
                support_record = record_by_term.get(str(correct_option.get("term_id") or ""), {})
                manual_dict_support = build_manual_dict_support(solution_note)
                actual_gloss = clean_text(str(solution_note.get("actual_gloss") or correct_option.get("text") or ""))
                filtered_textbook_support = filter_matching_textbook_support(
                    clean_text(str(correct_option.get("headword") or "")),
                    actual_gloss,
                    list(support_record.get("textbook_refs", [])),
                )
                question_context = (
                    [truncate_excerpt(item, 120) for item in best_context_window(source_passage, question_sentence, question_headword)]
                    if question_sentence and question_headword
                    else []
                )
                display_sentence = clean_text(str(question_sentence or ""))
                display_context = list(question_context or [])
                if option_sentence_count <= 1:
                    display_sentence = clean_text(str(correct_option.get("sentence") or question_sentence or ""))
                    display_context = list(correct_option.get("context_window") or question_context or [])
                explanation = build_challenge_explanation(
                    unit_prompt,
                    source_label,
                    correct_option,
                    solution_note,
                    support_record.get("dict_refs", []),
                    filtered_textbook_support,
                )
                bank[question_type].append(
                    {
                        "challenge_id": challenge_id,
                        "question_type": question_type,
                        "kind": question_kind,
                        "source_kind": "exam",
                        "term_id": str(correct_option.get("term_id") or ""),
                        "term_ids": unique_clean_strings(term_ids),
                        "priority_level": CONTENT_PRIORITY_CORE,
                        "paper_key": paper_key,
                        "year": year,
                        "paper": qdoc.get("paper"),
                        "question_number": question_number,
                        "source_label": source_label,
                        "source_title": article_title or str(qdoc.get("title") or ""),
                        "stem": unit_prompt,
                        "sentence": truncate_excerpt(display_sentence, 120),
                        "context_window": [truncate_excerpt(item, 120) for item in display_context[:7]],
                        "options": options_payload,
                    }
                )
                answer_keys[challenge_id] = {
                    "challenge_id": challenge_id,
                    "kind": question_kind,
                    "question_type": question_type,
                    "term_id": str(correct_option.get("term_id") or ""),
                    "term_ids": unique_clean_strings(term_ids),
                    "priority_level": CONTENT_PRIORITY_CORE,
                    "source_type": "exam",
                    "source_ref": {
                        "year": year,
                        "paper": qdoc.get("paper"),
                        "paper_key": paper_key,
                        "question_number": question_number,
                    },
                    "correct_label": answer_label,
                    "correct_text": (
                        f"{clean_text(str(correct_option.get('headword') or ''))}：{clean_text(str(solution_note.get('actual_gloss') or ''))}"
                        if ("不正确" in unit_prompt or "不同的一项" in unit_prompt) and clean_text(str(solution_note.get("actual_gloss") or ""))
                        else (
                            f"{clean_text(str(correct_option.get('sentence') or ''))} —— {clean_text(str(correct_option.get('text') or ''))}"
                            if clean_text(str(correct_option.get("sentence") or ""))
                            else str(correct_option.get("text") or "")
                        )
                    ),
                    "explanation": explanation,
                    "dict_support": (manual_dict_support or support_record.get("dict_refs", []))[:2],
                    "textbook_support": filtered_textbook_support[:2],
                    "option_analyses": [
                        {
                            "label": option["label"],
                            "text": (
                                f"{clean_text(str(option.get('sentence') or ''))} —— {clean_text(str(option.get('text') or ''))}"
                                if clean_text(str(option.get("sentence") or ""))
                                else option.get("text") or ""
                            ),
                            "is_correct": option["label"] == answer_label,
                            "analysis": build_option_analysis_for_direct(
                                unit_prompt,
                                option,
                                option["label"] == answer_label,
                                answer_label,
                                correct_option,
                                solution_note,
                            ),
                        }
                        for option in options_payload
                    ],
                }
                question_records.append({"question_number": question_number, "question_subtype": question_type, "challenge_id": challenge_id})
        exam_docs.append(
            {
                "paper_key": paper_key,
                "scope": "beijing",
                "year": year,
                "paper": qdoc.get("paper"),
                "question_number": None,
                "question_subtype": "mixed",
                "text": truncate_excerpt(text, 4000),
                "answer": truncate_excerpt(answer_text, 800),
                "term_occurrences": question_records,
            }
        )

    for question_type in QUESTION_TYPES:
        bank[question_type] = sorted(
            bank[question_type],
            key=lambda item: (
                0 if str(item.get("source_kind") or "") == "exam" else 1,
                int(item.get("year") or 0),
                str(item.get("challenge_id") or ""),
            ),
        )
    return bank, answer_keys, exam_docs


def choose_gloss_distractors(correct_gloss: str, pool: list[str], seed: str) -> list[str]:
    candidates = []
    for item in pool:
        cleaned = clean_text(item)
        if not cleaned or cleaned == correct_gloss:
            continue
        if cleaned in BANNED_GLOSS_CANDIDATES:
            continue
        if cleaned in correct_gloss or correct_gloss in cleaned:
            continue
        candidates.append(cleaned)
    return stable_pick(unique_clean_strings(candidates), seed, 3)


def textbook_function_gloss_attributed(headword: str, label_text: str, note_block: str) -> bool:
    """A function-word gloss must explain the function word itself.

    Textbook notes on a phrase often gloss another character (唯求则非邦也与 →
    "唯，语气助词，用于句首") or translate the whole phrase (执者失之). Such a
    note cannot be the answer to "the meaning/usage of 与/之".
    """
    label = normalize_label_headword(label_text)
    if not label or label == headword:
        return True
    note = clean_text(note_block)
    attributed = {ch for ch in set(label) if f"{ch}，" in note}
    if headword in attributed:
        return True
    if attributed:
        return False
    return len(label) <= 2


def textbook_dict_refs_for_target(record: dict[str, Any], headword: str, label_text: str) -> list[dict[str, Any]]:
    """Only dictionary entries whose headword is the question's own target word."""
    targets = {headword, normalize_label_headword(label_text)}
    kept = []
    for item in list(record.get("dict_refs") or []):
        if str(item.get("headword") or "") not in targets:
            continue
        summary = re.sub(r"\s*部首外筆畫數：.*$", "", clean_text(str(item.get("summary") or "")))
        if not summary or re.match(r"^「.+?」的異體字。?$", summary):
            continue
        kept.append({**item, "summary": summary})
    return kept[:2]


def build_textbook_question_bank(
    textbook_refs: dict[str, list[dict[str, Any]]],
    record_by_term: dict[str, dict[str, Any]],
    function_usage_catalog: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    bank: dict[str, list[dict[str, Any]]] = {
        "xuci_pair_compare": [],
        "function_gloss": [],
        "function_profile": [],
        "content_gloss": [],
        "sentence_meaning": [],
        "passage_meaning": [],
    }
    answer_keys: dict[str, dict[str, Any]] = {}
    textbook_distractor_catalog = build_textbook_distractor_catalog(textbook_refs)
    seen_textbook_prompt_targets: set[tuple[str, str, str, str]] = set()

    for term_id, refs in sorted(textbook_refs.items()):
        if not refs:
            continue
        record = record_by_term.get(term_id, {})
        headword = str(record.get("headword") or term_id.split("::", 1)[1])
        kind = str(record.get("kind") or ("function_word" if term_id.startswith("function::") else "content_word"))
        question_type = "function_gloss" if kind == "function_word" else "sentence_meaning"
        for index, ref in enumerate(refs):
            answer_text = textbook_distractor_answer_text(ref, kind, headword)
            correct_gloss = (
                normalize_function_profile_text(str(ref.get("gloss") or ref.get("answer_text") or ref.get("note_block") or ""))
                if kind == "function_word"
                else clean_text(str(ref.get("gloss") or ref.get("note_block") or ""))
            )
            if not correct_gloss or not answer_text:
                continue
            label_text = str(ref.get("label_text") or "")
            normalized_label = normalize_label_headword(label_text)
            display_sentence = best_textbook_focus_sentence(
                str(ref.get("sentence") or ""),
                list(ref.get("context_window") or []),
                headword,
                label_text,
            )
            if not textbook_target_in_text(display_sentence, headword, str(ref.get("label_text") or "")):
                continue
            if normalized_label and len(normalized_label) > 1 and normalized_label not in compact_hanzi_text(display_sentence):
                continue
            focus_text = (
                headword
                if kind == "function_word"
                else (label_text if normalized_label and normalized_label in compact_hanzi_text(display_sentence) else headword)
            )
            if kind == "content_word" and not textbook_content_target_allowed(
                headword,
                label_text,
                str(ref.get("note_block") or ""),
                str(ref.get("gloss") or ""),
                answer_text,
            ):
                continue
            if kind == "content_word" and textbook_content_phrase_like(headword, label_text, str(ref.get("note_block") or "")):
                continue
            if kind == "function_word" and not textbook_function_gloss_attributed(
                headword, label_text, str(ref.get("note_block") or "")
            ):
                continue
            target_dict_refs = textbook_dict_refs_for_target(record, headword, label_text)
            display_context = [
                truncate_excerpt_around_target(item, headword, label_text, 120)
                for item in (ref.get("context_window") or [])[:7]
            ]
            display_sentence_truncated = truncate_excerpt_around_target(display_sentence, headword, label_text, 120)
            if display_sentence_truncated and display_sentence_truncated not in display_context:
                display_context.insert(min(len(display_context), 3), display_sentence_truncated)
                display_context = display_context[:7]
            prompt_target_signature = (
                question_type,
                term_id,
                re.sub(r"\s+", "", display_sentence_truncated),
                clean_text(focus_text or headword),
            )
            if prompt_target_signature in seen_textbook_prompt_targets:
                continue
            seen_textbook_prompt_targets.add(prompt_target_signature)
            distractors = build_textbook_note_distractors(
                ref,
                term_id,
                kind,
                answer_text,
                textbook_distractor_catalog,
                f"{term_id}:{index}",
            )
            if len(distractors) < 3:
                continue
            article_id = stable_slug(f"{ref.get('book_key')}:{ref.get('title')}")
            source_label = f"{ref.get('school_stage')}教材《{ref.get('title')}》"
            if ref.get("book_title"):
                source_label += f"（{ref.get('book_title')}）"
            stem = (
                f"根据课下注释，下列对句中“{focus_text or headword}”的解释，最恰当的一项是"
                if kind == "content_word"
                else f"根据课下注释，下列对句中“{focus_text or headword}”的意义和用法概括，最恰当的一项是"
            )
            for variant_index, distractor_options in enumerate([distractors], start=1):
                option_candidates = [{"text": answer_text, "origin": "textbook_note"}] + distractor_options
                ordered_indices = sorted(
                    range(len(option_candidates)),
                    key=lambda idx: hashlib.sha1(
                        f"{term_id}:{index}:variant:{variant_index}:option:{idx}:{option_candidates[idx]['text']}".encode("utf-8")
                    ).hexdigest(),
                )
                ordered_options = [option_candidates[idx] for idx in ordered_indices]
                labels = ["A", "B", "C", "D"]
                correct_label = labels[next(idx for idx, option in enumerate(ordered_options) if option["origin"] == "textbook_note")]
                challenge_id = (
                    f"{'function' if kind == 'function_word' else 'content'}-textbook-{stable_slug(str(ref.get('ref_id') or ''))}-v{variant_index}"
                )
                bank[question_type].append(
                    {
                        "challenge_id": challenge_id,
                        "question_type": question_type,
                        "kind": kind,
                        "source_kind": "textbook",
                        "term_id": term_id,
                        "term_ids": [term_id],
                        "priority_level": str(record.get("priority_level") or CONTENT_PRIORITY_SECONDARY),
                        "source_label": source_label,
                        "source_title": str(ref.get("title") or ""),
                        "source_meta": {
                            "author": ref.get("author"),
                            "dynasty": ref.get("dynasty"),
                            "book_title": ref.get("book_title"),
                            "school_stage": ref.get("school_stage"),
                            "book_key": ref.get("book_key"),
                            "article_id": article_id,
                            "article_title": ref.get("title"),
                            "label_text": ref.get("label_text"),
                            "note_order": ref.get("note_order"),
                            "term_kind": kind,
                        },
                        "stem": stem,
                        "sentence": display_sentence_truncated,
                        "context_window": display_context,
                        "options": [
                            {
                                "label": label,
                                "term_id": term_id,
                                "headword": headword,
                                "sentence": display_sentence_truncated if kind == "function_word" else "",
                                "context_window": display_context,
                                "text": option["text"],
                                "origin": option["origin"],
                            }
                            for label, option in zip(labels, ordered_options)
                        ],
                    }
                )
                explanation = (
                    f"{source_label}中，这一处“{focus_text or headword}”的课下注释是“{answer_text}”。 "
                    f"所在句为“{truncate_excerpt_around_target(display_sentence or str(ref.get('sentence') or ''), headword, str(ref.get('label_text') or ''), 100)}”。"
                )
                if target_dict_refs:
                    explanation += " 辞典参照：" + " ".join(
                        truncate_excerpt(str(item.get("summary") or ""), 90) + "。"
                        for item in target_dict_refs
                    )
                same_term_support = [
                    item
                    for item in refs
                    if str(item.get("ref_id") or "") != str(ref.get("ref_id") or "")
                ][:3]
                if same_term_support:
                    explanation += " 同词其他课文用例（义项不一定相同）：" + " ".join(
                        f"{item.get('title')}“{truncate_excerpt_around_target(str(item.get('sentence') or ''), headword, str(item.get('label_text') or ''), 52)}”。"
                        for item in same_term_support
                        if clean_text(str(item.get("sentence") or ""))
                    )
                answer_keys[challenge_id] = {
                    "challenge_id": challenge_id,
                    "kind": kind,
                    "question_type": question_type,
                    "term_id": term_id,
                    "term_ids": [term_id],
                    "priority_level": str(record.get("priority_level") or CONTENT_PRIORITY_SECONDARY),
                    "source_type": "textbook",
                    "source_ref": {
                        "year": None,
                        "paper": str(ref.get("book_title") or ""),
                        "paper_key": str(ref.get("book_key") or ""),
                        "question_number": None,
                    },
                    "source_meta": {
                        "article_id": article_id,
                        "article_title": str(ref.get("title") or ""),
                        "book_key": str(ref.get("book_key") or ""),
                        "note_order": ref.get("note_order"),
                        "term_kind": kind,
                    },
                    "correct_label": correct_label,
                    "correct_text": answer_text,
                    "explanation": explanation,
                    "dict_support": target_dict_refs,
                    "textbook_support": [ref],
                    "option_analyses": [
                        {
                            "label": label,
                            "text": option["text"],
                            "is_correct": label == correct_label,
                            "analysis": (
                                f"本项与教材注释一致。“{focus_text or headword}”在这里应解释为“{answer_text}”。"
                                if label == correct_label
                                else (
                                f"本项不当。该项虽来自其他课下注释，但不能落实到本句；本句这一处“{focus_text or headword}”应按教材注释解释为“{answer_text}”。"
                                if option["origin"] == "textbook_note_distractor"
                                else f"本项不当。该项不是本句这一处“{focus_text or headword}”的课下注释；应落实为“{answer_text}”。"
                            )
                        ),
                        }
                        for label, option in zip(labels, ordered_options)
                    ],
                }
    return bank, answer_keys


def build_textbook_corpus_passages(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    passages: list[dict[str, Any]] = []
    for section in sections:
        passages.append(
            {
                "source": "textbook",
                "book_key": section.get("book_key"),
                "school_stage": section.get("school_stage"),
                "title": section.get("title"),
                "kind": section.get("kind"),
                "author": section.get("author"),
                "dynasty": section.get("dynasty"),
                "text": section.get("body_text") or "",
            }
        )
    return passages


def build_exam_corpus_passages(question_docs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    passages: list[dict[str, Any]] = []
    for paper_key, qdoc in sorted(question_docs.items(), key=lambda item: (int(item[1].get("year") or 0), str(item[0]))):
        source_passage = extract_source_passage(str(qdoc.get("text") or ""))
        if not source_passage:
            continue
        passages.append(
            {
                "source": "exam",
                "paper_key": paper_key,
                "scope": "beijing" if str(paper_key).startswith("beijing-") else "national",
                "year": int(qdoc.get("year") or 0),
                "paper": qdoc.get("paper"),
                "title": extract_exam_article_title(source_passage) or str(qdoc.get("title") or paper_key),
                "text": source_passage,
            }
        )
    return passages


def build_public_corpus_indexes(
    textbook_passages: list[dict[str, Any]],
    exam_passages: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "textbook": [
            {
                "book_key": item.get("book_key"),
                "school_stage": item.get("school_stage"),
                "title": item.get("title"),
                "kind": item.get("kind"),
                "author": item.get("author"),
                "dynasty": item.get("dynasty"),
                "char_count": len("".join(CHINESE_CHAR_RE.findall(str(item.get("text") or "")))),
            }
            for item in textbook_passages
        ],
        "exam": [
            {
                "paper_key": item.get("paper_key"),
                "scope": item.get("scope"),
                "year": item.get("year"),
                "paper": item.get("paper"),
                "title": item.get("title"),
                "char_count": len("".join(CHINESE_CHAR_RE.findall(str(item.get("text") or "")))),
            }
            for item in exam_passages
        ],
    }


def build_textbook_note_table(textbook_refs: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for term_id, refs in sorted(textbook_refs.items()):
        for ref in refs:
            context_window = [clean_text(item) for item in list(ref.get("context_window") or [])[:7] if clean_text(item)]
            sentence = clean_text(str(ref.get("sentence") or ""))
            focus_index = next((idx for idx, item in enumerate(context_window) if item == sentence), 0)
            rows.append(
                {
                    "ref_id": str(ref.get("ref_id") or ""),
                    "term_id": term_id,
                    "kind": "function_word" if term_id.startswith("function::") else "content_word",
                    "headword": clean_text(str(ref.get("headword") or "")),
                    "label_text": clean_text(str(ref.get("label_text") or "")),
                    "gloss": clean_text(str(ref.get("gloss") or "")),
                    "answer_text": clean_text(str(ref.get("answer_text") or "")),
                    "note_block": clean_text(str(ref.get("note_block") or "")),
                    "sentence": sentence,
                    "context_window": context_window,
                    "context_focus_index": focus_index,
                    "source_title": clean_text(str(ref.get("title") or "")),
                    "author": clean_text(str(ref.get("author") or "")),
                    "dynasty": clean_text(str(ref.get("dynasty") or "")),
                    "book_title": clean_text(str(ref.get("book_title") or "")),
                    "school_stage": clean_text(str(ref.get("school_stage") or "")),
                    "book_key": clean_text(str(ref.get("book_key") or "")),
                    "page_start": ref.get("page_start"),
                    "page_end": ref.get("page_end"),
                    "dict_headwords": [clean_text(str(item)) for item in list(ref.get("dict_headwords") or []) if clean_text(str(item))],
                    "manifest_title": clean_text(str(ref.get("manifest_title") or "")),
                    "note_order": ref.get("note_order"),
                    "body_source_mode": clean_text(str(ref.get("body_source_mode") or "")),
                    "body_source_title": clean_text(str(ref.get("body_source_title") or "")),
                    "body_source_path": clean_text(str(ref.get("body_source_path") or "")),
                    "annotation_scope": clean_text(str(ref.get("annotation_scope") or "body")),
                    "match_status": clean_text(str(ref.get("match_status") or "")),
                    "match_mode": clean_text(str(ref.get("match_mode") or "")),
                    "match_confidence": ref.get("match_confidence"),
                    "source_sentence": clean_text(str(ref.get("source_sentence") or sentence)),
                    "md_path": clean_text(str(ref.get("md_path") or "")),
                    "middle_path": clean_text(str(ref.get("middle_path") or "")),
                }
            )
    return rows


def build_textbook_note_term_index(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        term_id = str(row.get("term_id") or "")
        bucket = grouped.setdefault(
            term_id,
            {
                "term_id": term_id,
                "kind": row.get("kind"),
                "headword": row.get("headword"),
                "note_count": 0,
                "source_titles": [],
                "book_titles": [],
                "school_stages": [],
                "ref_ids": [],
            },
        )
        bucket["note_count"] += 1
        bucket["source_titles"].append(clean_text(str(row.get("source_title") or "")))
        bucket["book_titles"].append(clean_text(str(row.get("book_title") or "")))
        bucket["school_stages"].append(clean_text(str(row.get("school_stage") or "")))
        bucket["ref_ids"].append(clean_text(str(row.get("ref_id") or "")))
    results: list[dict[str, Any]] = []
    for bucket in grouped.values():
        bucket["source_titles"] = unique_clean_strings(bucket["source_titles"])
        bucket["book_titles"] = unique_clean_strings(bucket["book_titles"])
        bucket["school_stages"] = unique_clean_strings(bucket["school_stages"])
        bucket["ref_ids"] = unique_clean_strings(bucket["ref_ids"])
        results.append(bucket)
    return sorted(results, key=lambda item: (-int(item.get("note_count") or 0), str(item.get("term_id") or "")))


def build_textbook_note_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_kind = Counter(str(row.get("kind") or "") for row in rows if clean_text(str(row.get("kind") or "")))
    by_stage = Counter(clean_text(str(row.get("school_stage") or "")) for row in rows if clean_text(str(row.get("school_stage") or "")))
    by_book = Counter(clean_text(str(row.get("book_title") or "")) for row in rows if clean_text(str(row.get("book_title") or "")))
    by_title = Counter(clean_text(str(row.get("source_title") or "")) for row in rows if clean_text(str(row.get("source_title") or "")))
    by_headword = Counter(clean_text(str(row.get("headword") or "")) for row in rows if clean_text(str(row.get("headword") or "")))
    return {
        "total_notes": len(rows),
        "content_notes": sum(1 for row in rows if str(row.get("kind") or "") == "content_word"),
        "function_notes": sum(1 for row in rows if str(row.get("kind") or "") == "function_word"),
        "unique_terms": len({str(row.get("term_id") or "") for row in rows if str(row.get("term_id") or "")}),
        "by_kind": dict(by_kind),
        "by_school_stage": dict(by_stage),
        "top_books": [{"book_title": key, "count": value} for key, value in by_book.most_common(20)],
        "top_titles": [{"source_title": key, "count": value} for key, value in by_title.most_common(30)],
        "top_headwords": [{"headword": key, "count": value} for key, value in by_headword.most_common(40)],
    }


def build_textbook_article_challenge_assets(
    article_rows: list[dict[str, Any]],
    textbook_bank: dict[str, list[dict[str, Any]]],
    answer_keys: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    article_index = {
        stable_slug(f"{clean_text(str(row.get('book_key') or ''))}:{clean_text(str(row.get('title') or ''))}"): row
        for row in article_rows
        if clean_text(str(row.get("book_key") or "")) and clean_text(str(row.get("title") or ""))
    }
    grouped_items: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for items in textbook_bank.values():
        for item in items:
            challenge_id = clean_text(str(item.get("challenge_id") or ""))
            answer_key = answer_keys.get(challenge_id)
            if not answer_key:
                continue
            support_ref = list(answer_key.get("textbook_support") or [])[:1]
            ref = support_ref[0] if support_ref else {}
            article_id = clean_text(str((item.get("source_meta") or {}).get("article_id") or ""))
            if not article_id:
                article_id = stable_slug(f"{clean_text(str(ref.get('book_key') or ''))}:{clean_text(str(ref.get('title') or ''))}")
            if not article_id:
                continue
            normalized_item = dict(item)
            source_meta = dict(item.get("source_meta") or {})
            source_meta["article_id"] = article_id
            source_meta["article_title"] = clean_text(str(source_meta.get("article_title") or ref.get("title") or item.get("source_title") or ""))
            source_meta["book_key"] = clean_text(str(source_meta.get("book_key") or ref.get("book_key") or ""))
            source_meta["note_order"] = source_meta.get("note_order") if source_meta.get("note_order") is not None else ref.get("note_order")
            source_meta["term_kind"] = clean_text(str(source_meta.get("term_kind") or answer_key.get("kind") or item.get("kind") or ""))
            normalized_item["source_meta"] = source_meta
            normalized_item["article_id"] = article_id
            grouped_items[article_id].append(normalized_item)

    article_catalog: list[dict[str, Any]] = []
    article_bank: dict[str, dict[str, Any]] = {}
    for article_id, raw_items in grouped_items.items():
        article = article_index.get(article_id, {})
        items = sorted(
            raw_items,
            key=lambda item: (
                int((item.get("source_meta") or {}).get("note_order") or 0),
                0 if clean_text(str((item.get("source_meta") or {}).get("term_kind") or "")) == "content_word" else 1,
                clean_text(str(item.get("challenge_id") or "")),
            ),
        )
        content_count = sum(
            1 for item in items if clean_text(str((item.get("source_meta") or {}).get("term_kind") or "")) == "content_word"
        )
        function_count = sum(
            1 for item in items if clean_text(str((item.get("source_meta") or {}).get("term_kind") or "")) == "function_word"
        )
        meta = {
            "article_id": article_id,
            "book_key": clean_text(str(article.get("book_key") or (items[0].get("source_meta") or {}).get("book_key") or "")),
            "book_title": clean_text(str(article.get("book_title") or "")),
            "school_stage": textbook_school_stage(clean_text(str(article.get("book_key") or "")))
            if clean_text(str(article.get("book_key") or ""))
            else "",
            "title": clean_text(str(article.get("title") or (items[0].get("source_meta") or {}).get("article_title") or "")),
            "manifest_title": clean_text(str(article.get("manifest_title") or "")),
            "author": clean_text(str(article.get("author") or "")),
            "dynasty": clean_text(str(article.get("dynasty") or "")),
            "page_start": article.get("page_start_print"),
            "page_end": article.get("page_end_print"),
            "challenge_count": len(items),
            "content_count": content_count,
            "function_count": function_count,
            "note_count": int(article.get("note_count") or 0),
        }
        article_catalog.append(meta)
        article_bank[article_id] = {
            "article": meta,
            "items": items,
        }

    article_catalog = sorted(
        article_catalog,
        key=lambda item: (
            clean_text(str(item.get("book_key") or "")),
            int(item.get("page_start") or 0),
            clean_text(str(item.get("title") or "")),
        ),
    )
    return article_catalog, article_bank


def textbook_note_review_key(
    book_key: str,
    title: str,
    note_order: Any,
    annotation_scope: str,
    label_text: str,
    note_text: str,
) -> tuple[str, str, int, str, str, str]:
    return (
        clean_text(book_key),
        clean_text(title),
        int(note_order or 0),
        clean_text(annotation_scope or "body"),
        clean_text(label_text),
        clean_text(note_text),
    )


def build_textbook_challenge_index(answer_keys: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for challenge_id, payload in answer_keys.items():
        if str(payload.get("source_type") or "") != "textbook":
            continue
        for ref in list(payload.get("textbook_support") or [])[:1]:
            ref_id = clean_text(str(ref.get("ref_id") or ""))
            if not ref_id:
                continue
            index[ref_id].append(
                {
                    "challenge_id": clean_text(challenge_id),
                    "question_type": clean_text(str(payload.get("question_type") or "")),
                    "kind": clean_text(str(payload.get("kind") or "")),
                    "correct_text": clean_text(str(payload.get("correct_text") or "")),
                }
            )
    return {
        key: sorted(value, key=lambda item: (str(item.get("question_type") or ""), str(item.get("challenge_id") or "")))
        for key, value in index.items()
    }


def summarize_dict_review_links(
    headword_candidates: list[str],
    revised_links: dict[str, list[dict[str, Any]]],
    idiom_links: dict[str, list[dict[str, Any]]],
    limit: int = 6,
) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for headword in unique_clean_strings(headword_candidates):
        if not headword:
            continue
        for item in list(revised_links.get(headword, []))[:2]:
            summary = clean_text(str(item.get("summary") or ""))
            if not summary:
                continue
            line = f"{headword}｜教育部重编：{truncate_excerpt(summary, 120)}"
            if line not in seen:
                seen.add(line)
                lines.append(line)
        for item in list(idiom_links.get(headword, []))[:2]:
            summary = clean_text(str(item.get("summary") or ""))
            if not summary:
                continue
            line = f"{headword}｜成语典：{truncate_excerpt(summary, 100)}"
            if line not in seen:
                seen.add(line)
                lines.append(line)
        if len(lines) >= limit:
            break
    return lines[:limit]


def suggest_review_split(headword: str, label_text: str, note_text: str, auto_kind: str) -> tuple[bool, str]:
    normalized_label = normalize_label_headword(label_text)
    leading_label = normalize_label_headword(leading_note_annotation(note_text))
    if not normalized_label:
        return True, "标注对象为空，需人工重定词目。"
    if re.search(r"[、，,；;/／]", clean_text(label_text)):
        return True, "标注对象包含并列或分隔符，建议拆开后再判。"
    if auto_kind == "content_word":
        if textbook_content_phrase_like(headword, label_text, note_text):
            return True, "当前标注更像短语或整句，不宜直接作为实词题目。"
        if len(normalized_label) > 2:
            return True, "实词标注超过两字，优先收敛到更小词目。"
    if auto_kind == "function_word":
        if len(normalized_label) > max(1, len(clean_text(headword))):
            return True, "虚词应优先收敛到单字或最小功能词。"
    if leading_label and leading_label != normalized_label:
        return True, "注释前导标记与当前标签不一致，建议拆分确认。"
    return False, ""


def build_review_flags(
    review_headword: str,
    label_text: str,
    note_text: str,
    row: dict[str, Any],
    auto_kind: str,
    auto_should_split: bool,
    auto_reliable: bool,
    in_current_bank: bool,
    dict_summary_lines: list[str],
) -> list[str]:
    flags: list[str] = []
    if clean_text(str(row.get("annotation_scope") or "body")) != "body":
        flags.append("annotation_not_body")
    if clean_text(str(row.get("match_status") or "")) != "matched":
        flags.append("source_not_matched")
    if auto_kind == "content_word" and textbook_content_phrase_like(
        clean_text(review_headword),
        clean_text(label_text),
        clean_text(note_text),
    ):
        flags.append("phrase_like_content")
    if auto_kind == "function_word":
        normalized_label = normalize_label_headword(clean_text(label_text))
        if len(normalized_label) > 1:
            flags.append("function_label_longer_than_headword")
    if auto_should_split:
        flags.append("needs_split")
    if not auto_reliable:
        flags.append("not_bank_ready")
    if not in_current_bank:
        flags.append("not_in_current_bank")
    if not dict_summary_lines:
        flags.append("missing_dict_support")
    if leading_note_annotation(clean_text(note_text)):
        flags.append("has_leading_annotation")
    return flags


def build_textbook_proofread_rows(
    article_rows: list[dict[str, Any]],
    note_rows: list[dict[str, Any]],
    textbook_note_table: list[dict[str, Any]],
    revised_links: dict[str, list[dict[str, Any]]],
    idiom_links: dict[str, list[dict[str, Any]]],
    answer_keys: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    article_index = {
        (clean_text(str(row.get("book_key") or "")), clean_text(str(row.get("title") or ""))): row
        for row in article_rows
    }
    note_table_index = {
        textbook_note_review_key(
            str(row.get("book_key") or ""),
            str(row.get("source_title") or ""),
            row.get("note_order"),
            str(row.get("annotation_scope") or "body"),
            str(row.get("label_text") or ""),
            str(row.get("note_block") or ""),
        ): row
        for row in textbook_note_table
    }
    challenge_index = build_textbook_challenge_index(answer_keys)

    results: list[dict[str, Any]] = []
    sorted_note_rows = sorted(
        note_rows,
        key=lambda row: (
            clean_text(str(row.get("book_key") or "")),
            clean_text(str(row.get("title") or "")),
            int(row.get("note_order") or 0),
            clean_text(str(row.get("label_text") or "")),
        ),
    )

    for row in sorted_note_rows:
        book_key = clean_text(str(row.get("book_key") or ""))
        title = clean_text(str(row.get("title") or ""))
        label_text = clean_text(str(row.get("label_text") or ""))
        note_text = clean_text(str(row.get("note_text") or ""))
        annotation_scope = clean_text(str(row.get("annotation_scope") or "body"))
        review_key = textbook_note_review_key(
            book_key,
            title,
            row.get("note_order"),
            annotation_scope,
            label_text,
            note_text,
        )
        current_row = note_table_index.get(review_key, {})
        article = article_index.get((book_key, title), {})

        master_headword = clean_text(str(row.get("headword") or ""))
        master_gloss = clean_text(str(row.get("gloss") or ""))
        initial_headword = extract_note_headword(label_text, note_text) or master_headword
        if master_gloss:
            auto_seed_gloss = master_gloss
        else:
            auto_seed_gloss = summarize_note_gloss(initial_headword, note_text)
        initial_kind = infer_textbook_term_kind(initial_headword, label_text, note_text, auto_seed_gloss)
        auto_headword = resolve_textbook_bank_headword(row, initial_headword, initial_kind) or initial_headword
        auto_kind = infer_textbook_term_kind(auto_headword, label_text, note_text, auto_seed_gloss)
        if auto_kind == "function_word":
            auto_gloss = summarize_textbook_function_gloss(auto_headword, note_text, auto_seed_gloss)
            auto_answer_text = auto_gloss
        else:
            auto_gloss = auto_seed_gloss or summarize_note_gloss(auto_headword, note_text)
            auto_answer_text = build_textbook_answer_text(label_text, note_text, auto_gloss, auto_headword)

        raw_context_window = [
            clean_text(str(item))
            for item in list(
                row.get("context_window")
                or row.get("text_context_window")
                or row.get("layout_context_window")
                or current_row.get("context_window")
                or []
            )
            if clean_text(str(item))
        ][:7]
        source_sentence = clean_text(
            str(
                row.get("source_sentence")
                or row.get("text_source_sentence")
                or row.get("layout_source_sentence")
                or current_row.get("sentence")
                or ""
            )
        )
        display_sentence = best_textbook_focus_sentence(source_sentence, raw_context_window, auto_headword, label_text)
        auto_should_split, auto_split_reason = suggest_review_split(auto_headword, label_text, note_text, auto_kind)
        auto_reliable = textbook_ref_is_reliable(
            auto_kind,
            auto_headword,
            label_text,
            note_text,
            auto_gloss or auto_answer_text,
            display_sentence or source_sentence,
        )

        current_ref_id = clean_text(str(current_row.get("ref_id") or ""))
        current_challenges = challenge_index.get(current_ref_id, [])
        current_question_types = unique_clean_strings([str(item.get("question_type") or "") for item in current_challenges])
        current_challenge_ids = unique_clean_strings([str(item.get("challenge_id") or "") for item in current_challenges])
        has_current_candidate = bool(current_row)
        in_current_bank = bool(current_challenge_ids)

        dict_headwords = unique_clean_strings(
            [
                clean_text(str(item))
                for item in (
                    [auto_headword, master_headword, normalize_label_headword(label_text)]
                    + list(row.get("dict_headwords") or [])
                    + list(current_row.get("dict_headwords") or [])
                )
                if clean_text(str(item))
            ]
        )
        dict_summary_lines = summarize_dict_review_links(dict_headwords, revised_links, idiom_links)
        auto_flags = build_review_flags(
            auto_headword,
            label_text,
            note_text,
            row,
            auto_kind,
            auto_should_split,
            auto_reliable,
            in_current_bank,
            dict_summary_lines,
        )

        results.append(
            {
                "review_id": stable_slug(f"{book_key}:{title}:{int(row.get('note_order') or 0)}:{label_text}"),
                "article_id": stable_slug(f"{book_key}:{title}"),
                "book_key": book_key,
                "book_title": clean_text(str(row.get("book_title") or article.get("book_title") or "")),
                "school_stage": textbook_school_stage(book_key),
                "source_title": title,
                "manifest_title": clean_text(str(row.get("manifest_title") or article.get("manifest_title") or "")),
                "page_start": row.get("page_start_print") if row.get("page_start_print") is not None else article.get("page_start_print"),
                "page_end": row.get("page_end_print") if row.get("page_end_print") is not None else article.get("page_end_print"),
                "author": clean_text(str(article.get("author") or "")),
                "dynasty": clean_text(str(article.get("dynasty") or "")),
                "body_source_mode": clean_text(str(row.get("body_source_mode") or article.get("body_source_mode") or "")),
                "body_source_title": clean_text(str(row.get("body_source_title") or article.get("body_source_title") or "")),
                "note_order": int(row.get("note_order") or 0),
                "annotation_scope": annotation_scope,
                "label_text": label_text,
                "normalized_label": normalize_label_headword(label_text),
                "note_text": note_text,
                "master_headword": master_headword,
                "master_kind": clean_text(str(row.get("kind") or "")),
                "master_gloss": master_gloss,
                "master_answer_text": clean_text(str(row.get("answer_text") or "")),
                "auto_headword": auto_headword,
                "auto_kind": auto_kind,
                "auto_gloss": auto_gloss,
                "auto_answer_text": auto_answer_text,
                "auto_should_split": auto_should_split,
                "auto_split_reason": auto_split_reason,
                "auto_reliable": auto_reliable,
                "auto_flags": auto_flags,
                "current_has_candidate": has_current_candidate,
                "current_in_bank": in_current_bank,
                "current_ref_id": current_ref_id,
                "current_term_id": clean_text(str(current_row.get("term_id") or "")),
                "current_kind": clean_text(str(current_row.get("kind") or "")),
                "current_headword": clean_text(str(current_row.get("headword") or "")),
                "current_gloss": clean_text(str(current_row.get("gloss") or "")),
                "current_answer_text": clean_text(str(current_row.get("answer_text") or "")),
                "current_question_types": current_question_types,
                "current_challenge_ids": current_challenge_ids,
                "dict_headwords": dict_headwords,
                "dict_summary_lines": dict_summary_lines,
                "match_status": clean_text(str(row.get("match_status") or "")),
                "match_mode": clean_text(str(row.get("match_mode") or "")),
                "match_confidence": row.get("match_confidence"),
                "source_sentence": source_sentence,
                "display_sentence": display_sentence,
                "context_window": raw_context_window,
                "human_confirm": "",
                "human_kind": "",
                "human_split": "",
                "human_term": "",
                "human_gloss": "",
                "human_action": "",
                "human_notes": "",
            }
        )
    return results


def write_textbook_proofread_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "review_id",
        "article_id",
        "book_key",
        "book_title",
        "school_stage",
        "source_title",
        "manifest_title",
        "page_start",
        "page_end",
        "author",
        "dynasty",
        "body_source_mode",
        "body_source_title",
        "note_order",
        "annotation_scope",
        "label_text",
        "normalized_label",
        "note_text",
        "master_headword",
        "master_kind",
        "master_gloss",
        "master_answer_text",
        "auto_headword",
        "auto_kind",
        "auto_gloss",
        "auto_answer_text",
        "auto_should_split",
        "auto_split_reason",
        "auto_reliable",
        "auto_flags",
        "current_has_candidate",
        "current_in_bank",
        "current_ref_id",
        "current_term_id",
        "current_kind",
        "current_headword",
        "current_gloss",
        "current_answer_text",
        "current_question_types",
        "current_challenge_ids",
        "dict_headwords",
        "dict_summary_lines",
        "match_status",
        "match_mode",
        "match_confidence",
        "source_sentence",
        "display_sentence",
        "context_window",
        "human_confirm",
        "human_kind",
        "human_split",
        "human_term",
        "human_gloss",
        "human_action",
        "human_notes",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row)
            for key in ("auto_flags", "current_question_types", "current_challenge_ids", "dict_headwords", "dict_summary_lines", "context_window"):
                payload[key] = "｜".join(payload.get(key) or [])
            writer.writerow(payload)


def safe_markdown_code_block(value: str) -> str:
    return str(value or "").replace("```", "``\u200b`")


def write_textbook_proofread_markdown(
    path: Path,
    article_rows: list[dict[str, Any]],
    review_rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    article_index = {
        stable_slug(f"{clean_text(str(row.get('book_key') or ''))}:{clean_text(str(row.get('title') or ''))}"): row
        for row in article_rows
    }
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in review_rows:
        grouped_rows[str(row.get("article_id") or "")].append(row)

    lines = [
        "# 教材正文与注释人工校对底稿",
        "",
        "## 校对规则",
        "1. 以最小干预为原则，不改写教材正文和课下注释，只填写确认、判定、拆分和处理意见。",
        "2. 先判断能否入题，再判断是实词还是虚词；短语、整句、专名、背景说明优先标记为“不入题”或“拆分后再判”。",
        "3. 若注释对象能收敛到更小词目，优先保留最小词目，不要把整句或短语直接作为题目。",
        "4. 自动词类、自动拆分建议、当前入题状态、辞典义项都只是辅助信息；人工只在确有问题时修正。",
        "5. `人工确认` 统一填写：确认 / 待核 / 剔除。",
        "6. `人工判定` 统一填写：实词 / 虚词 / 不入题 / 拆分后再判。",
        "7. `人工拆分` 统一填写：是 / 否；若填“是”，请同时填写 `人工词目` 与 `人工处理意见`。",
        "8. `人工处理动作` 建议填写：保留 / 改词目 / 改释义 / 拆分 / 剔除 / 待核。",
        "",
        "## 机器汇总",
        f"- 课文条目数：{len(grouped_rows)}",
        f"- 注释条目数：{len(review_rows)}",
        f"- 当前已入题：{sum(1 for row in review_rows if row.get('current_in_bank'))}",
        f"- 当前仅入候选未出题：{sum(1 for row in review_rows if row.get('current_has_candidate') and not row.get('current_in_bank'))}",
        f"- 自动建议拆分：{sum(1 for row in review_rows if row.get('auto_should_split'))}",
        f"- 自动判定实词：{sum(1 for row in review_rows if row.get('auto_kind') == 'content_word')}",
        f"- 自动判定虚词：{sum(1 for row in review_rows if row.get('auto_kind') == 'function_word')}",
        "",
    ]

    ordered_article_ids = sorted(
        grouped_rows,
        key=lambda article_id: (
            clean_text(str(article_index.get(article_id, {}).get("book_key") or "")),
            clean_text(str(article_index.get(article_id, {}).get("title") or "")),
        ),
    )
    for article_id in ordered_article_ids:
        article = article_index.get(article_id, {})
        rows = sorted(grouped_rows[article_id], key=lambda item: (int(item.get("note_order") or 0), str(item.get("label_text") or "")))
        title = clean_text(str(article.get("title") or rows[0].get("source_title") or "未命名条目"))
        lines.append(f"## 《{title}》")
        lines.append(f"- 教材：{clean_text(str(rows[0].get('book_title') or ''))}")
        lines.append(f"- 册别：{clean_text(str(rows[0].get('school_stage') or ''))} / {clean_text(str(rows[0].get('book_key') or ''))}")
        lines.append(f"- 作者：{clean_text(str(article.get('author') or '')) or '—'}")
        lines.append(f"- 朝代：{clean_text(str(article.get('dynasty') or '')) or '—'}")
        lines.append(
            f"- 页码：{clean_text(str(rows[0].get('page_start') or '')) or '—'}"
            + (
                f" - {clean_text(str(rows[0].get('page_end') or ''))}"
                if clean_text(str(rows[0].get('page_end') or ''))
                and clean_text(str(rows[0].get('page_end') or '')) != clean_text(str(rows[0].get('page_start') or ''))
                else ""
            )
        )
        lines.append(
            f"- 正文来源：{clean_text(str(rows[0].get('body_source_mode') or article.get('body_source_mode') or '')) or '—'} / "
            f"{clean_text(str(rows[0].get('body_source_title') or article.get('body_source_title') or '')) or '—'}"
        )
        lines.append("- 正文：")
        lines.append("```text")
        lines.append(safe_markdown_code_block(clean_text_keep_newlines(str(article.get("full_text") or article.get("ocr_full_text") or ""))))
        lines.append("```")
        lines.append("")
        for row in rows:
            lines.append(f"### 注释 {int(row.get('note_order') or 0):02d}｜{clean_text(str(row.get('label_text') or '')) or '未命名'}")
            lines.append(f"- 原标签：{clean_text(str(row.get('label_text') or '')) or '—'}")
            lines.append(f"- 现有注释：{clean_text(str(row.get('note_text') or '')) or '—'}")
            lines.append(f"- 自动抽取词目：{clean_text(str(row.get('auto_headword') or '')) or '—'}")
            lines.append(
                f"- 自动判定：{'虚词' if str(row.get('auto_kind') or '') == 'function_word' else '实词' if str(row.get('auto_kind') or '') == 'content_word' else '—'}"
            )
            lines.append(f"- 自动释义：{clean_text(str(row.get('auto_answer_text') or row.get('auto_gloss') or '')) or '—'}")
            lines.append(f"- 自动建议拆分：{'是' if row.get('auto_should_split') else '否'}")
            lines.append(f"- 拆分理由：{clean_text(str(row.get('auto_split_reason') or '')) or '—'}")
            lines.append(f"- 当前已出题：{'是' if row.get('current_in_bank') else '否'}")
            lines.append(f"- 当前已入候选：{'是' if row.get('current_has_candidate') else '否'}")
            lines.append(f"- 当前候选词目：{clean_text(str(row.get('current_headword') or '')) or '—'}")
            lines.append(f"- 当前候选词类：{clean_text(str(row.get('current_kind') or '')) or '—'}")
            lines.append(f"- 当前候选释义：{clean_text(str(row.get('current_answer_text') or '')) or '—'}")
            lines.append(
                f"- 当前题型：{(' / '.join(row.get('current_question_types') or [])) or '—'}"
            )
            lines.append(
                f"- 题目 ID：{(' / '.join(row.get('current_challenge_ids') or [])) or '—'}"
            )
            lines.append(
                f"- 对位状态：{clean_text(str(row.get('match_status') or '')) or '—'} / {clean_text(str(row.get('match_mode') or '')) or '—'} / {clean_text(str(row.get('match_confidence') or '')) or '—'}"
            )
            lines.append(f"- 命中句：{clean_text(str(row.get('display_sentence') or row.get('source_sentence') or '')) or '—'}")
            lines.append(f"- 上下文：{('｜'.join(row.get('context_window') or [])) or '—'}")
            lines.append(f"- 关联辞典词目：{('｜'.join(row.get('dict_headwords') or [])) or '—'}")
            lines.append("- 关联辞典义项：")
            if row.get("dict_summary_lines"):
                for item in row.get("dict_summary_lines") or []:
                    lines.append(f"  - {item}")
            else:
                lines.append("  - —")
            lines.append(f"- 自动标记：{('｜'.join(row.get('auto_flags') or [])) or '—'}")
            lines.append(f"- 人工确认：{clean_text(str(row.get('human_confirm') or ''))}")
            lines.append(f"- 人工判定：{clean_text(str(row.get('human_kind') or ''))}")
            lines.append(f"- 人工拆分：{clean_text(str(row.get('human_split') or ''))}")
            lines.append(f"- 人工词目：{clean_text(str(row.get('human_term') or ''))}")
            lines.append(f"- 人工释义：{clean_text(str(row.get('human_gloss') or ''))}")
            lines.append(f"- 人工处理动作：{clean_text(str(row.get('human_action') or ''))}")
            lines.append(f"- 人工处理意见：{clean_text(str(row.get('human_notes') or ''))}")
            lines.append("")
        lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_textbook_article_packets(article_rows: list[dict[str, Any]], review_rows: list[dict[str, Any]]) -> None:
    article_index = {
        stable_slug(f"{clean_text(str(row.get('book_key') or ''))}:{clean_text(str(row.get('title') or ''))}"): row
        for row in article_rows
        if clean_text(str(row.get("book_key") or "")) and clean_text(str(row.get("title") or ""))
    }
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in review_rows:
        article_id = clean_text(str(row.get("article_id") or ""))
        if article_id:
            grouped_rows[article_id].append(row)

    if TEXTBOOK_ARTICLE_PACKET_DIR.exists():
        shutil.rmtree(TEXTBOOK_ARTICLE_PACKET_DIR)
    if TEXTBOOK_ARTICLE_PACKET_MD_DIR.exists():
        shutil.rmtree(TEXTBOOK_ARTICLE_PACKET_MD_DIR)
    TEXTBOOK_ARTICLE_PACKET_DIR.mkdir(parents=True, exist_ok=True)
    TEXTBOOK_ARTICLE_PACKET_MD_DIR.mkdir(parents=True, exist_ok=True)

    for article_id, rows in grouped_rows.items():
        article = article_index.get(article_id, {})
        ordered_rows = sorted(rows, key=lambda item: (int(item.get("note_order") or 0), clean_text(str(item.get("label_text") or ""))))
        summary = {
            "article_id": article_id,
            "book_key": clean_text(str(article.get("book_key") or ordered_rows[0].get("book_key") or "")),
            "book_title": clean_text(str(article.get("book_title") or ordered_rows[0].get("book_title") or "")),
            "title": clean_text(str(article.get("title") or ordered_rows[0].get("source_title") or "")),
            "manifest_title": clean_text(str(article.get("manifest_title") or ordered_rows[0].get("manifest_title") or "")),
            "author": clean_text(str(article.get("author") or "")),
            "dynasty": clean_text(str(article.get("dynasty") or "")),
            "page_start": article.get("page_start_print"),
            "page_end": article.get("page_end_print"),
            "body_source_mode": clean_text(str(article.get("body_source_mode") or ordered_rows[0].get("body_source_mode") or "")),
            "body_source_title": clean_text(str(article.get("body_source_title") or ordered_rows[0].get("body_source_title") or "")),
            "full_text": clean_text_keep_newlines(str(article.get("full_text") or article.get("ocr_full_text") or "")),
            "row_count": len(ordered_rows),
            "in_bank_count": sum(1 for row in ordered_rows if row.get("current_in_bank")),
            "needs_split_count": sum(1 for row in ordered_rows if row.get("auto_should_split")),
        }
        json_payload = {
            "article": summary,
            "rows": ordered_rows,
        }
        (TEXTBOOK_ARTICLE_PACKET_DIR / f"{article_id}.json").write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        title = summary["title"] or article_id
        lines = [
            f"# {title}",
            "",
            "## 校对规则",
            "",
            "- 最小干预：优先确认系统自动抽取是否准确，不必重写整条注释。",
            "- 只保留能稳定落到句中具体字词的条目；整句、整分句、短语拼接项标记为需拆分或不入题。",
            "- 重点核对：是否实词 / 虚词、当前词目是否准确、课下注释是否可直接作答、是否需要拆分。",
            "- 可直接填写 human_kind / human_split / human_term / human_gloss / human_action / human_notes。",
            "",
            "## 篇目信息",
            "",
            f"- 书册：{summary['book_title'] or '—'}",
            f"- 篇目：{summary['manifest_title'] or title}",
            f"- 作者：{summary['author'] or '—'}",
            f"- 朝代：{summary['dynasty'] or '—'}",
            f"- 页码：{summary['page_start'] or '—'} - {summary['page_end'] or '—'}",
            f"- 当前可出题：{summary['in_bank_count']}",
            f"- 待拆分：{summary['needs_split_count']}",
            f"- 正文来源：{summary['body_source_mode'] or '—'} / {summary['body_source_title'] or '—'}",
            "",
            "## 正文",
            "",
            safe_markdown_code_block(summary["full_text"]),
            "",
            "## 注释条目",
            "",
        ]
        for row in ordered_rows:
            lines.extend(
                [
                    f"### {int(row.get('note_order') or 0)}. {clean_text(str(row.get('label_text') or '')) or '未命名注释'}",
                    "",
                    f"- 自动词目：{clean_text(str(row.get('auto_headword') or '')) or '—'}",
                    f"- 自动判定：{'虚词' if str(row.get('auto_kind') or '') == 'function_word' else '实词' if str(row.get('auto_kind') or '') == 'content_word' else '—'}",
                    f"- 当前已出题：{'是' if row.get('current_in_bank') else '否'}",
                    f"- 建议拆分：{'是' if row.get('auto_should_split') else '否'}",
                    f"- 自动释义：{clean_text(str(row.get('auto_answer_text') or '')) or '—'}",
                    f"- 命中句：{clean_text(str(row.get('display_sentence') or row.get('source_sentence') or '')) or '—'}",
                    f"- 课下注释：{clean_text(str(row.get('note_text') or '')) or '—'}",
                    f"- 辞典义项：{' / '.join(list(row.get('dict_summary_lines') or [])[:4]) or '—'}",
                    f"- 人工确认：{clean_text(str(row.get('human_confirm') or '')) or '—'}",
                    f"- 人工词类：{clean_text(str(row.get('human_kind') or '')) or '—'}",
                    f"- 人工拆分：{clean_text(str(row.get('human_split') or '')) or '—'}",
                    f"- 人工词目：{clean_text(str(row.get('human_term') or '')) or '—'}",
                    f"- 人工释义：{clean_text(str(row.get('human_gloss') or '')) or '—'}",
                    f"- 处理意见：{clean_text(str(row.get('human_action') or '')) or '—'}",
                    f"- 备注：{clean_text(str(row.get('human_notes') or '')) or '—'}",
                    "",
                ]
            )
        (TEXTBOOK_ARTICLE_PACKET_MD_DIR / f"{article_id}.md").write_text(
            "\n".join(lines).rstrip() + "\n",
            encoding="utf-8",
        )


def write_table_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "ref_id",
        "term_id",
        "kind",
        "headword",
        "label_text",
        "gloss",
        "answer_text",
        "note_block",
        "sentence",
        "context_window",
        "context_focus_index",
        "source_title",
        "author",
        "dynasty",
        "book_title",
        "school_stage",
        "book_key",
        "page_start",
        "page_end",
        "dict_headwords",
        "manifest_title",
        "note_order",
        "body_source_mode",
        "body_source_title",
        "body_source_path",
        "annotation_scope",
        "match_status",
        "match_mode",
        "match_confidence",
        "source_sentence",
        "md_path",
        "middle_path",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row)
            payload["context_window"] = "｜".join(payload.get("context_window") or [])
            payload["dict_headwords"] = "｜".join(payload.get("dict_headwords") or [])
            writer.writerow(payload)


def write_private_corpus_documents(
    textbook_passages: list[dict[str, Any]],
    exam_passages: list[dict[str, Any]],
) -> None:
    PRIVATE_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    textbook_lines = ["# 教材文言总语料", ""]
    for item in textbook_passages:
        title = clean_text(str(item.get("title") or ""))
        meta = " / ".join(
            [
                clean_text(str(item.get("school_stage") or "")),
                clean_text(str(item.get("book_key") or "")),
                clean_text(str(item.get("author") or "")),
                clean_text(str(item.get("dynasty") or "")),
            ]
        ).strip(" /")
        textbook_lines.append(f"## {title}")
        if meta:
            textbook_lines.append(meta)
        textbook_lines.append(clean_text_keep_newlines(str(item.get("text") or "")))
        textbook_lines.append("")
    (PRIVATE_RUNTIME_DIR / "textbook_classical_corpus.md").write_text("\n".join(textbook_lines), encoding="utf-8")

    exam_lines = ["# 真题文言总语料", ""]
    for item in exam_passages:
        title = clean_text(str(item.get("title") or ""))
        meta = " / ".join(
            [
                clean_text(str(item.get("scope") or "")),
                clean_text(str(item.get("year") or "")),
                clean_text(str(item.get("paper") or "")),
                clean_text(str(item.get("paper_key") or "")),
            ]
        ).strip(" /")
        exam_lines.append(f"## {title}")
        if meta:
            exam_lines.append(meta)
        exam_lines.append(clean_text_keep_newlines(str(item.get("text") or "")))
        exam_lines.append("")
    (PRIVATE_RUNTIME_DIR / "exam_classical_corpus.md").write_text("\n".join(exam_lines), encoding="utf-8")


def build_function_usage_table(
    function_records: list[dict[str, Any]],
    function_usage_catalog: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []
    for record in function_records:
        headword = clean_text(str(record.get("headword") or ""))
        profiles = function_usage_catalog.get(headword, [])
        table.append(
            {
                "term_id": record.get("term_id"),
                "headword": headword,
                "beijing_frequency": record.get("beijing_frequency", 0),
                "question_type_counts": record.get("question_type_counts", {}),
                "profiles": profiles,
                "textbook_refs": [
                    {
                        "title": ref.get("title"),
                        "sentence": ref.get("sentence"),
                        "gloss": ref.get("gloss"),
                    }
                    for ref in list(record.get("textbook_refs") or [])[:8]
                ],
            }
        )
    return sorted(table, key=lambda item: (-int(item.get("beijing_frequency") or 0), str(item.get("headword") or "")))


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
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "manifest.json").write_bytes(encoded)


def write_private_answer_keys(answer_keys: dict[str, Any]) -> None:
    encoded = json.dumps(answer_keys, ensure_ascii=False, indent=2).encode("utf-8")
    PRIVATE_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    (PRIVATE_RUNTIME_DIR / "answer_keys.json").write_bytes(encoded)
    (GENERATED_DIR / "answer_keys.json").write_bytes(encoded)


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


def clear_old_runtime_files() -> None:
    keep_private_json_names = {
        TEXTBOOK_ARTICLE_MASTER_PATH.name,
        TEXTBOOK_NOTE_MASTER_PATH.name,
        "textbook_note_unresolved_table.json",
        "forum_textbook_topics_raw.json",
        "forum_textbook_source_cache.json",
    }
    for output_dir in (RUNTIME_MIRROR_DIR, PUBLIC_RUNTIME_DIR):
        output_dir.mkdir(parents=True, exist_ok=True)
        for path in output_dir.glob("*.json"):
            path.unlink()
    PRIVATE_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    for path in PRIVATE_RUNTIME_DIR.glob("*.json"):
        if path.name in keep_private_json_names:
            continue
        path.unlink()


def main() -> int:
    source_report = collect_source_report()
    ensure_sources_or_raise(source_report)

    xuci = load_json(XUCI_PATH)
    shici = load_json(SHICI_PATH)
    question_docs = {**xuci.get("question_docs", {}), **shici.get("question_docs", {})}
    answer_overrides = load_manual_answer_overrides()
    solution_notes = load_solution_notes()
    option_overrides = load_manual_option_overrides()
    function_detail_terms = load_function_detail_terms()
    function_usage_catalog = build_function_usage_catalog(function_detail_terms)
    function_source_terms = apply_manual_term_source_corrections(list(xuci.get("terms", [])), option_overrides, solution_notes)
    content_source_terms = apply_manual_term_source_corrections(list(shici.get("terms", [])), option_overrides, solution_notes)
    function_raw_terms = merge_function_terms(function_source_terms)
    content_raw_terms = merge_content_terms(content_source_terms, question_docs)
    occurrence_lookup = build_exam_occurrence_lookup(function_source_terms, content_source_terms)

    article_rows, note_rows = load_textbook_source_master_tables()
    sections, textbook_refs = load_textbook_sections_from_master_tables(article_rows, note_rows)
    textbook_note_table = build_textbook_note_table(textbook_refs)
    textbook_note_content = [row for row in textbook_note_table if str(row.get("kind") or "") == "content_word"]
    textbook_note_function = [row for row in textbook_note_table if str(row.get("kind") or "") == "function_word"]
    textbook_note_term_index = build_textbook_note_term_index(textbook_note_table)
    textbook_note_stats = build_textbook_note_stats(textbook_note_table)
    all_headwords = sorted(
        {
            str(term.get("headword") or "")
            for term in [*function_raw_terms, *content_raw_terms]
        }
        | {term_id.split("::", 1)[1] for term_id in textbook_refs}
        | {
            clean_text(str(candidate))
            for refs in textbook_refs.values()
            for ref in refs
            for candidate in list(ref.get("dict_headwords") or [])
            if clean_text(str(candidate))
        }
        | {clean_text(str(note.get("dict_headword") or "")) for note in solution_notes.values() if note.get("dict_headword")}
    )
    revised_links = query_revised_links(all_headwords)
    idiom_links = query_idiom_links(all_headwords)

    function_records = build_union_term_records(
        function_raw_terms, textbook_refs, revised_links, idiom_links, function_usage_catalog, "function_word"
    )
    content_records = build_union_term_records(
        content_raw_terms, textbook_refs, revised_links, idiom_links, function_usage_catalog, "content_word"
    )
    record_by_term = {record["term_id"]: record for record in [*function_records, *content_records]}

    exam_bank, exam_answer_keys, exam_docs = parse_beijing_exam_bank(
        question_docs,
        answer_overrides,
        solution_notes,
        option_overrides,
        record_by_term,
        occurrence_lookup,
    )
    textbook_bank, textbook_answer_keys = build_textbook_question_bank(textbook_refs, record_by_term, function_usage_catalog)

    challenge_bank = {
        key: sorted(
            [*exam_bank.get(key, []), *textbook_bank.get(key, [])],
            key=lambda item: (
                0 if str(item.get("source_kind") or "") == "exam" else 1,
                int(item.get("year") or 0),
                str(item.get("challenge_id") or ""),
            ),
        )
        for key in QUESTION_TYPES
    }
    answer_keys = {**exam_answer_keys, **textbook_answer_keys}
    challenge_bank, answer_keys = dedupe_challenge_bank(challenge_bank, answer_keys)

    exam_questions = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "question_docs": exam_docs,
        "challenge_bank": challenge_bank,
        "question_templates": load_question_templates(),
    }
    dict_links = {
        record["term_id"]: {
            "headword": record["headword"],
            "kind": record["kind"],
            "revised_sense_links": record["dict_refs"],
            "idiom_links": record["idiom_refs"],
        }
        for record in [*function_records, *content_records]
    }
    textbook_examples = {term_id: refs for term_id, refs in textbook_refs.items() if refs}
    textbook_passages = build_textbook_corpus_passages(sections)
    exam_passages = build_exam_corpus_passages(question_docs)
    write_private_corpus_documents(textbook_passages, exam_passages)
    segmentation_vocabulary = build_segmentation_vocabulary(function_records, content_records, textbook_refs)
    textbook_frequency_table = build_corpus_frequency_table(textbook_passages, segmentation_vocabulary)
    exam_frequency_table = build_corpus_frequency_table(exam_passages, segmentation_vocabulary)
    union_frequency_counter = Counter()
    for row in textbook_frequency_table:
        union_frequency_counter[str(row["token"])] += int(row["frequency"])
    for row in exam_frequency_table:
        union_frequency_counter[str(row["token"])] += int(row["frequency"])
    union_frequency_table = [
        {"token": token, "frequency": frequency}
        for token, frequency in sorted(union_frequency_counter.items(), key=lambda item: (-item[1], item[0]))
    ]
    corpus_indexes = build_public_corpus_indexes(textbook_passages, exam_passages)
    exam_tested_terms = build_headword_frequency_records(function_records, content_records)
    function_usage_table = build_function_usage_table(function_records, function_usage_catalog)
    textbook_article_catalog, textbook_article_bank = build_textbook_article_challenge_assets(
        article_rows,
        textbook_bank,
        answer_keys,
    )
    proofread_rows = build_textbook_proofread_rows(
        article_rows,
        note_rows,
        textbook_note_table,
        revised_links,
        idiom_links,
        answer_keys,
    )

    clear_old_runtime_files()
    write_private_answer_keys(answer_keys)
    TEXTBOOK_PROOFREAD_TABLE_PATH.write_text(json.dumps(proofread_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_textbook_proofread_csv(TEXTBOOK_PROOFREAD_TABLE_CSV_PATH, proofread_rows)
    write_textbook_proofread_markdown(TEXTBOOK_PROOFREAD_MD_PATH, article_rows, proofread_rows)
    write_textbook_article_packets(article_rows, proofread_rows)
    write_table_csv(PRIVATE_RUNTIME_DIR / "textbook_notes_table.csv", textbook_note_table)
    write_table_csv(PRIVATE_RUNTIME_DIR / "textbook_notes_content.csv", textbook_note_content)
    write_table_csv(PRIVATE_RUNTIME_DIR / "textbook_notes_function.csv", textbook_note_function)

    manifest_payload = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "asset_max_bytes": ASSET_MAX_BYTES,
        "assets": {},
        "stats": {
            "terms_function": len(function_records),
            "terms_content": len(content_records),
            "exam_question_docs": len(exam_docs),
            "textbook_corpus_docs": len(textbook_passages),
            "exam_corpus_docs": len(exam_passages),
            "textbook_notes": len(textbook_note_table),
            "textbook_content_notes": len(textbook_note_content),
            "textbook_function_notes": len(textbook_note_function),
            "textbook_article_count": len(textbook_article_catalog),
            "textbook_article_challenges": sum(int(item.get("challenge_count") or 0) for item in textbook_article_catalog),
            "challenge_counts": {key: len(value) for key, value in challenge_bank.items()},
        },
    }
    write_runtime_asset("terms_function", function_records, "list", manifest_payload)
    write_runtime_asset("terms_content", content_records, "list", manifest_payload)
    write_runtime_asset("exam_questions", exam_questions, "object", manifest_payload)
    write_runtime_asset("textbook_examples", textbook_examples, "object", manifest_payload)
    write_runtime_asset("textbook_notes_table", textbook_note_table, "list", manifest_payload)
    write_runtime_asset("textbook_notes_content", textbook_note_content, "list", manifest_payload)
    write_runtime_asset("textbook_notes_function", textbook_note_function, "list", manifest_payload)
    write_runtime_asset("textbook_note_term_index", textbook_note_term_index, "list", manifest_payload)
    write_runtime_asset("textbook_note_stats", textbook_note_stats, "object", manifest_payload)
    write_runtime_asset("textbook_article_catalog", textbook_article_catalog, "list", manifest_payload)
    write_runtime_asset("textbook_article_bank", textbook_article_bank, "object", manifest_payload)
    write_runtime_asset("dict_links", dict_links, "object", manifest_payload)
    write_runtime_asset("corpus_indexes", corpus_indexes, "object", manifest_payload)
    write_runtime_asset("textbook_frequency_table", textbook_frequency_table, "list", manifest_payload)
    write_runtime_asset("exam_frequency_table", exam_frequency_table, "list", manifest_payload)
    write_runtime_asset("union_frequency_table", union_frequency_table, "list", manifest_payload)
    write_runtime_asset("exam_tested_terms", exam_tested_terms, "list", manifest_payload)
    write_runtime_asset("function_usage_table", function_usage_table, "list", manifest_payload)
    write_manifest(manifest_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
