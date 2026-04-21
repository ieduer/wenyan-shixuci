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
)


ASSET_MAX_BYTES = int(os.environ.get("ASSET_MAX_BYTES", "26214400"))
RUNTIME_MIRROR_DIR = REPO_ROOT / "data" / "runtime"
PUBLIC_RUNTIME_DIR = REPO_ROOT / "public" / "runtime"
QUESTION_TEMPLATES_DIR = REPO_ROOT / "question_templates"


QUESTION_TYPE_TO_BASIS = {
    "xuci_compare_same": "direct_choice",
    "xuci_compare_diff": "direct_choice",
    "xuci_explanation": "direct_choice",
    "shici_explanation": "direct_choice",
    "national_raw_gloss_option": "direct_choice",
    "national_raw_translation_keyword": "translation_keypoint",
    "translation_keypoint": "translation_keypoint",
    "sentence_meaning": "sentence_meaning",
    "passage_meaning": "passage_meaning",
    "analysis_short": "analysis_short",
}

QUESTION_TYPES = [
    "xuci_pair_compare",
    "content_gloss",
    "translation_keypoint",
    "sentence_meaning",
    "passage_meaning",
    "analysis_short",
]

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
    text = text.replace("*", "")
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def stable_slug(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "-", text)
    return text.strip("-") or hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]


def hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def answer_label_for_question(answer_text: str, question_number: int) -> str:
    match = re.search(rf"(?m)^\s*{question_number}\.\s*([A-D])\b", answer_text or "")
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
    chunks = re.split(r"(?<=[。！？；!?:：])|(?<=/)", prepared)
    return [chunk.strip(" /") for chunk in chunks if chunk.strip(" /")]


def truncate_excerpt(text: str, limit: int = 200) -> str:
    cleaned = clean_text(text)
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 1] + "…"


def locate_section(text: str, title: str, window: int = 9000) -> str:
    normalized_target = normalize_title(title)
    best_start = -1
    for match in re.finditer(r"(?m)^#.*$", text):
        line = match.group(0)
        if normalized_target and normalized_target in normalize_title(line):
            best_start = match.start()
            break
    if best_start < 0:
        pos = normalize_title(text).find(normalized_target)
        if pos < 0:
            return ""
        return text[max(0, pos - 400) : pos + window]
    return text[best_start : best_start + window]


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
                if headword in sentence:
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
        for basis_type in ("sentence_meaning", "passage_meaning", "analysis_short"):
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


def choose_gloss_distractors(all_glosses: list[str], correct_gloss: str, seed: str) -> list[str]:
    pool = sorted({item for item in all_glosses if item and item != correct_gloss})
    return stable_pick(pool, seed, 3)


def find_passage(qdoc_text: str, excerpt: str) -> str:
    probe = clean_text(excerpt).split("/")[0].strip()
    if not probe:
        return truncate_excerpt(qdoc_text, 220)
    for paragraph in re.split(r"\n{2,}", qdoc_text):
        if probe[:8] and probe[:8] in clean_text(paragraph):
            return truncate_excerpt(paragraph, 240)
    return truncate_excerpt(qdoc_text, 240)


def build_function_question_bank(function_terms: list[dict[str, Any]], question_docs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
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

    bank: list[dict[str, Any]] = []
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
            sentences = [clean_text(item.get("excerpt") or "") for item in ordered]
            if (len(sentences) == 1 or len(set(sentences)) == 1) and sentences:
                if "/" in sentences[0]:
                    sentences = [part.strip() for part in sentences[0].split("/") if part.strip()]
                else:
                    sentences = split_sentences(sentences[0])
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
        bank.append(
            {
                "challenge_id": challenge_id,
                "question_type": "xuci_pair_compare",
                "kind": "function_word",
                "term_id": options[0]["term_id"],
                "term_ids": sorted(group["term_ids"]),
                "paper_key": group["paper_key"],
                "year": group["year"],
                "paper": group["paper"],
                "question_number": group["question_number"],
                "stem": stem,
                "options": options,
                "answer": {"label": answer_label},
                "explanation": f"依据 {group['year']} 年 {group['paper']} 第 {group['question_number']} 题答案 {answer_label}。",
            }
        )
    return sorted(bank, key=lambda item: (item["year"], item["question_number"]))


def build_content_question_bank(content_terms: list[dict[str, Any]], question_docs: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    all_glosses = sorted(
        {
            clean_text(str(occ.get("gloss") or ""))
            for term in content_terms
            for occ in term.get("occurrences", [])
            if occ.get("gloss")
        }
    )
    bank: dict[str, list[dict[str, Any]]] = {question_type: [] for question_type in QUESTION_TYPES if question_type != "xuci_pair_compare"}
    option_labels = ["A", "B", "C", "D"]

    for term in content_terms:
        term_id = f"content::{term['headword']}"
        headword = str(term["headword"])
        for index, occurrence in enumerate(term.get("occurrences", [])):
            gloss = clean_text(str(occurrence.get("gloss") or ""))
            excerpt = clean_text(str(occurrence.get("excerpt") or ""))
            if not gloss or not excerpt:
                continue
            paper_key = str(occurrence.get("paper_key") or "")
            qdoc = question_docs.get(paper_key, {})
            seed = f"{term_id}:{paper_key}:{occurrence.get('question_number')}:{index}"
            distractors = choose_gloss_distractors(all_glosses, gloss, seed)
            if len(distractors) < 3:
                continue
            gloss_options = stable_pick([gloss, *distractors], seed + ":gloss", 4)
            correct_label = option_labels[gloss_options.index(gloss)]

            base_meta = {
                "term_id": term_id,
                "headword": headword,
                "paper_key": paper_key,
                "year": occurrence.get("year"),
                "paper": occurrence.get("paper"),
                "question_number": occurrence.get("question_number"),
                "evidence_excerpt": truncate_excerpt(excerpt, 120),
            }

            bank["content_gloss"].append(
                {
                    "challenge_id": f"content-{stable_slug(seed)}",
                    "question_type": "content_gloss",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"下列对句中“{headword}”的解释，最恰当的一项是",
                    "sentence": truncate_excerpt(excerpt, 120),
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, gloss_options)],
                    "answer": {"label": correct_label},
                    "explanation": f"{headword} 在这里应解释为“{gloss}”。",
                }
            )

            bank["translation_keypoint"].append(
                {
                    "challenge_id": f"translation-{stable_slug(seed)}",
                    "question_type": "translation_keypoint",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"如果把这句话译成现代汉语，“{headword}”最关键的意思是",
                    "sentence": truncate_excerpt(excerpt, 120),
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, gloss_options)],
                    "answer": {"label": correct_label},
                    "explanation": f"翻译时需要把“{headword}”落实为“{gloss}”。",
                }
            )

            meaning_options = [
                f"这里的“{headword}”表示“{option}”，据此整句应这样理解。"
                for option in gloss_options
            ]
            bank["sentence_meaning"].append(
                {
                    "challenge_id": f"sentence-{stable_slug(seed)}",
                    "question_type": "sentence_meaning",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"结合语境，哪一项最能说明句中“{headword}”对句意的影响？",
                    "sentence": truncate_excerpt(excerpt, 120),
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, meaning_options)],
                    "answer": {"label": correct_label},
                    "explanation": f"词义定为“{gloss}”时，句意才与上下文一致。",
                }
            )

            passage = find_passage(str(qdoc.get("text") or ""), excerpt)
            passage_options = [
                f"这段文字中，“{headword}”可理解为“{option}”，因此相关文意判断成立。"
                for option in gloss_options
            ]
            bank["passage_meaning"].append(
                {
                    "challenge_id": f"passage-{stable_slug(seed)}",
                    "question_type": "passage_meaning",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"结合整段语境，哪一项对“{headword}”的理解最稳妥？",
                    "passage": passage,
                    "options": [{"label": label, "text": option} for label, option in zip(option_labels, passage_options)],
                    "answer": {"label": correct_label},
                    "explanation": f"整段语境仍要求把“{headword}”落实为“{gloss}”。",
                }
            )

            analysis_options = [
                {"key": "A", "text": "它决定了句中核心动作或状态的译法。"},
                {"key": "B", "text": "它参与交代人物、对象或事件关系。"},
                {"key": "C", "text": "忽略它会导致句意或文意判断失真。"},
                {"key": "D", "text": "它只起音节作用，对理解整段并不重要。"},
            ]
            correct_keys = ["A", "C"]
            if len(headword) > 1 or any(word in gloss for word in ("给", "对", "面对", "承担", "使", "归属")):
                correct_keys = ["A", "B", "C"]
            bank["analysis_short"].append(
                {
                    "challenge_id": f"analysis-{stable_slug(seed)}",
                    "question_type": "analysis_short",
                    "kind": "content_word",
                    **base_meta,
                    "stem": f"要拿下这道题，关于“{headword}”至少应确认哪些要点？（多选）",
                    "sentence": truncate_excerpt(excerpt, 120),
                    "options": analysis_options,
                    "answer": {"keys": correct_keys},
                    "explanation": f"至少要确认“{headword}”的词义，并知道它会牵动句意判断；本题词义为“{gloss}”。",
                    "response_mode": "multi_select",
                }
            )
    return bank


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
        usage_relations: list[dict[str, Any]]
        if kind == "function_word":
            usage_relations = FUNCTION_WORD_PROFILES.get(headword, [])
        else:
            gloss_counter = Counter(
                clean_text(str(item.get("gloss") or ""))
                for item in term.get("occurrences", [])
                if item.get("gloss")
            )
            usage_relations = [
                {"semantic_value": gloss, "evidence_count": count}
                for gloss, count in gloss_counter.most_common(6)
            ]
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
                "sample_glosses": [clean_text(item) for item in term.get("sample_glosses", []) if clean_text(item)],
                "textbook_refs": term_textbook_refs,
                "dict_refs": term_dict_refs,
                "idiom_refs": term_idiom_refs,
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
    content_raw_terms = list(shici.get("terms", []))
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

    function_bank = build_function_question_bank(function_raw_terms, question_docs)
    content_bank = build_content_question_bank(content_raw_terms, question_docs)
    exam_question_docs = build_exam_question_docs(question_docs, function_raw_terms, content_raw_terms)

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
            "xuci_pair_compare": function_bank,
            "content_gloss": content_bank["content_gloss"],
            "translation_keypoint": content_bank["translation_keypoint"],
            "sentence_meaning": content_bank["sentence_meaning"],
            "passage_meaning": content_bank["passage_meaning"],
            "analysis_short": content_bank["analysis_short"],
        },
        "question_templates": templates,
    }

    clear_old_runtime_files()
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
