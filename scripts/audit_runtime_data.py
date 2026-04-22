#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from build_runtime_data import (
    BANNED_GLOSS_CANDIDATES,
    GENERATED_DIR,
    PRIVATE_RUNTIME_DIR,
    PUBLIC_RUNTIME_DIR,
    RUNTIME_MIRROR_DIR,
    build_headword_frequency_records,
    clean_gloss,
    clean_text,
    load_json,
    looks_like_clean_gloss,
    merge_content_terms,
)
from check_sources import REPO_ROOT, SHICI_PATH, XUCI_PATH, collect_source_report


DOCS_DIR = REPO_ROOT / "docs"
JSON_REPORT_PATH = DOCS_DIR / "DATA_AUDIT_REPORT.json"
MD_REPORT_PATH = DOCS_DIR / "DATA_AUDIT_REPORT.md"
DATA_QUALITY_PATHS = [
    PUBLIC_RUNTIME_DIR / "data_quality.json",
    RUNTIME_MIRROR_DIR / "data_quality.json",
]


def looks_like_public_answer_leak(item: dict[str, Any]) -> bool:
    return "answer" in item or "explanation" in item or "correct_label" in item


def validate_option_text(text: str, question_type: str) -> str | None:
    cleaned = clean_text(text)
    if not cleaned:
        return "blank_option"
    if question_type == "content_gloss":
        if not looks_like_clean_gloss(cleaned):
            return "dirty_gloss_option"
        if cleaned in BANNED_GLOSS_CANDIDATES:
            return "banned_gloss_option"
    if question_type == "function_gloss":
        if len(cleaned) > 40:
            return "dirty_gloss_option"
        if cleaned in BANNED_GLOSS_CANDIDATES:
            return "banned_gloss_option"
    return None


def build_filtered_raw_examples(runtime_terms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw_terms = merge_content_terms(load_json(SHICI_PATH).get("terms", []), load_json(XUCI_PATH).get("question_docs", {}))
    runtime_ids = {term["term_id"] for term in runtime_terms}
    examples: list[dict[str, Any]] = []
    for term in raw_terms:
        term_id = f"content::{term['headword']}"
        if term_id not in runtime_ids:
            continue
        for occurrence in term.get("occurrences", []):
            cleaned = clean_gloss(str(term["headword"]), str(occurrence.get("gloss") or ""), str(occurrence.get("excerpt") or ""))
            if looks_like_clean_gloss(cleaned):
                continue
            examples.append(
                {
                    "term_id": term_id,
                    "paper_key": occurrence.get("paper_key"),
                    "question_number": occurrence.get("question_number"),
                    "raw_gloss": clean_text(str(occurrence.get("gloss") or "")),
                    "cleaned_gloss": cleaned,
                    "excerpt": clean_text(str(occurrence.get("excerpt") or ""))[:120],
                }
            )
            if len(examples) >= 20:
                return examples
    return examples


def answer_key_issue_counts(
    challenge_bank: dict[str, list[dict[str, Any]]],
    answer_keys: dict[str, dict[str, Any]],
) -> tuple[Counter[str], list[dict[str, Any]]]:
    issue_counts: Counter[str] = Counter()
    issue_examples: list[dict[str, Any]] = []

    for question_type, items in challenge_bank.items():
        for item in items:
            challenge_id = str(item.get("challenge_id") or "")
            answer_key = answer_keys.get(challenge_id)
            if not answer_key:
                issue_counts["missing_answer_key"] += 1
                if len(issue_examples) < 20:
                    issue_examples.append({"reason": "missing_answer_key", "challenge_id": challenge_id, "question_type": question_type})
                continue

            if str(answer_key.get("correct_label") or "") not in {"A", "B", "C", "D"}:
                issue_counts["invalid_correct_label"] += 1
            option_analyses = answer_key.get("option_analyses") or []
            if len(option_analyses) != len(item.get("options") or []):
                issue_counts["option_analysis_mismatch"] += 1
            if not clean_text(str(answer_key.get("explanation") or "")):
                issue_counts["empty_explanation"] += 1

            for option in item.get("options") or []:
                reason = validate_option_text(str(option.get("text") or option.get("headword") or ""), question_type)
                if reason:
                    issue_counts[reason] += 1
                    if len(issue_examples) < 20:
                        issue_examples.append(
                            {
                                "reason": reason,
                                "challenge_id": challenge_id,
                                "question_type": question_type,
                                "option": option,
                            }
                        )
            if question_type == "xuci_pair_compare":
                if len(item.get("options") or []) != 4:
                    issue_counts["xuci_option_count"] += 1
                for option in item.get("options") or []:
                    if len(option.get("sentences") or []) != 2:
                        issue_counts["xuci_sentence_pair"] += 1
            elif question_type == "sentence_meaning" and str(item.get("source_kind") or "") == "textbook":
                answer_label = clean_text(str(answer_key.get("correct_label") or ""))
                for option in item.get("options") or []:
                    if clean_text(str(option.get("label") or "")) == answer_label:
                        continue
                    if clean_text(str(option.get("origin") or "")) != "dict_sense":
                        issue_counts["textbook_content_non_dict_distractor"] += 1
            elif question_type in {"content_gloss", "function_gloss"} and str(item.get("source_kind") or "") == "exam":
                option_sentences = [clean_text(str(option.get("sentence") or "")) for option in item.get("options") or []]
                if not all(option_sentences):
                    issue_counts["exam_option_sentence_missing"] += 1
            elif len(item.get("options") or []) != 4:
                issue_counts["single_choice_option_count"] += 1

            if looks_like_public_answer_leak(item):
                issue_counts["public_answer_leak"] += 1

    return issue_counts, issue_examples


def build_summary_report() -> dict[str, Any]:
    source_report = collect_source_report()
    manifest = load_json(PUBLIC_RUNTIME_DIR / "manifest.json")
    terms_function = load_json(PUBLIC_RUNTIME_DIR / "terms_function.json")
    terms_content = load_json(PUBLIC_RUNTIME_DIR / "terms_content.json")
    exam_questions = load_json(PUBLIC_RUNTIME_DIR / "exam_questions.json")
    corpus_indexes = load_json(PUBLIC_RUNTIME_DIR / "corpus_indexes.json")
    textbook_frequency_table = load_json(PUBLIC_RUNTIME_DIR / "textbook_frequency_table.json")
    exam_frequency_table = load_json(PUBLIC_RUNTIME_DIR / "exam_frequency_table.json")
    union_frequency_table = load_json(PUBLIC_RUNTIME_DIR / "union_frequency_table.json")
    function_usage_table = load_json(PUBLIC_RUNTIME_DIR / "function_usage_table.json")
    textbook_note_stats = load_json(PUBLIC_RUNTIME_DIR / "textbook_note_stats.json")
    answer_keys = load_json(PRIVATE_RUNTIME_DIR / "answer_keys.json")

    challenge_bank = exam_questions["challenge_bank"]
    issue_counts, issue_examples = answer_key_issue_counts(challenge_bank, answer_keys)

    content_core = [term for term in terms_content if str(term.get("priority_level") or "") == "core"]
    content_secondary = [term for term in terms_content if str(term.get("priority_level") or "") == "secondary"]
    function_core = [term for term in terms_function if str(term.get("priority_level") or "") == "core"]

    support_miss_count = sum(
        1
        for term in [*terms_function, *terms_content]
        if not (term.get("dict_refs") or term.get("textbook_refs") or term.get("idiom_refs"))
    )
    content_dirty_samples = build_filtered_raw_examples(terms_content)
    correct_label_counter = Counter()
    for answer in answer_keys.values():
        label = clean_text(str(answer.get("correct_label") or ""))
        if label in {"A", "B", "C", "D"}:
            correct_label_counter[label] += 1

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_ok": bool(source_report.get("ok")),
        "source_report": source_report,
        "manifest_stats": manifest.get("stats", {}),
        "content": {
            "term_count": len(terms_content),
            "core_term_count": len(content_core),
            "secondary_term_count": len(content_secondary),
            "challenge_counts": {
                "content_gloss": len(challenge_bank.get("content_gloss", [])),
                "sentence_meaning": len(challenge_bank.get("sentence_meaning", [])),
                "passage_meaning": len(challenge_bank.get("passage_meaning", [])),
            },
            "support_miss_count": support_miss_count,
            "filtered_raw_examples": content_dirty_samples,
            "top_textbook_tokens": textbook_frequency_table[:30],
        },
        "function": {
            "term_count": len(terms_function),
            "core_term_count": len(function_core),
            "challenge_counts": {
                "xuci_pair_compare": len(challenge_bank.get("xuci_pair_compare", [])),
                "function_gloss": len(challenge_bank.get("function_gloss", [])),
                "function_profile": len(challenge_bank.get("function_profile", [])),
            },
            "usage_table_count": len(function_usage_table),
        },
        "corpus": {
            "textbook_doc_count": len(corpus_indexes.get("textbook", [])),
            "exam_doc_count": len(corpus_indexes.get("exam", [])),
            "textbook_note_count": int(textbook_note_stats.get("total_notes") or 0),
            "textbook_content_note_count": int(textbook_note_stats.get("content_notes") or 0),
            "textbook_function_note_count": int(textbook_note_stats.get("function_notes") or 0),
            "textbook_token_count": len(textbook_frequency_table),
            "exam_token_count": len(exam_frequency_table),
            "union_token_count": len(union_frequency_table),
            "top_union_tokens": union_frequency_table[:40],
            "top_textbook_note_titles": list(textbook_note_stats.get("top_titles") or [])[:20],
        },
        "runtime": {
            "challenge_counts": {key: len(value) for key, value in challenge_bank.items()},
            "answer_key_count": len(answer_keys),
            "correct_label_distribution": dict(correct_label_counter),
            "issue_counts": dict(issue_counts),
            "issue_examples": issue_examples,
            "private_answer_key_paths": [
                str(PRIVATE_RUNTIME_DIR / "answer_keys.json"),
                str(GENERATED_DIR / "answer_keys.json"),
            ],
        },
    }
    return report


def write_reports(report: dict[str, Any]) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    JSON_REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    runtime_issues = report["runtime"]["issue_counts"]
    lines = [
        "# 数据审计报告",
        "",
        f"- 生成时间：{report['generated_at']}",
        f"- 源数据检查：{'通过' if report['source_ok'] else '失败'}",
        "",
        "## 实词题库",
        f"- 实词条目：{report['content']['term_count']}",
        f"- 重点实词：{report['content']['core_term_count']}",
        f"- 次重点实词：{report['content']['secondary_term_count']}",
        f"- 题目数量：{report['content']['challenge_counts']}",
        "",
        "## 虚词题库",
        f"- 虚词条目：{report['function']['term_count']}",
        f"- 教材联动核心虚词：{report['function']['core_term_count']}",
        f"- 题目数量：{report['function']['challenge_counts']}",
        f"- 虚词义项表条目：{report['function']['usage_table_count']}",
        "",
        "## 语料与切分",
        f"- 教材篇目数：{report['corpus']['textbook_doc_count']}",
        f"- 教材注释数：{report['corpus']['textbook_note_count']}",
        f"- 教材实词注释数：{report['corpus']['textbook_content_note_count']}",
        f"- 教材虚词注释数：{report['corpus']['textbook_function_note_count']}",
        f"- 真题文段数：{report['corpus']['exam_doc_count']}",
        f"- 教材切分词数：{report['corpus']['textbook_token_count']}",
        f"- 真题切分词数：{report['corpus']['exam_token_count']}",
        f"- 合并切分词数：{report['corpus']['union_token_count']}",
        "",
        "## 运行时验证",
        f"- 题库总量：{report['runtime']['challenge_counts']}",
        f"- 答案键总量：{report['runtime']['answer_key_count']}",
        f"- 正确答案分布：{report['runtime']['correct_label_distribution']}",
        f"- 问题计数：{runtime_issues if runtime_issues else '{}'}",
        "",
        "## 高频词样例",
    ]
    for example in report["corpus"]["top_union_tokens"][:15]:
        lines.append(f"- {example['token']}: {example['frequency']}")
    lines.extend([
        "",
        "## 已过滤的原始脏数据样例",
    ])
    if report["content"]["filtered_raw_examples"]:
        for example in report["content"]["filtered_raw_examples"][:10]:
            lines.append(
                f"- {example['term_id']} / {example['paper_key']} q{example['question_number']} / raw={example['raw_gloss']} / cleaned={example['cleaned_gloss']}"
            )
    else:
        lines.append("- 未发现需展示的脏数据样例。")
    lines.extend(["", "## 运行时问题样例"])
    if report["runtime"]["issue_examples"]:
        for example in report["runtime"]["issue_examples"][:10]:
            lines.append(f"- {json.dumps(example, ensure_ascii=False)}")
    else:
        lines.append("- 当前未检出运行时结构问题。")
    MD_REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    quality = {
        "generated_at": report["generated_at"],
        "source_ok": report["source_ok"],
        "content": {
            "term_count": report["content"]["term_count"],
            "core_term_count": report["content"]["core_term_count"],
            "secondary_term_count": report["content"]["secondary_term_count"],
        },
        "function": {
            "term_count": report["function"]["term_count"],
            "core_term_count": report["function"]["core_term_count"],
        },
        "corpus": {
            "textbook_doc_count": report["corpus"]["textbook_doc_count"],
            "exam_doc_count": report["corpus"]["exam_doc_count"],
            "union_token_count": report["corpus"]["union_token_count"],
        },
        "runtime": {
            "challenge_counts": report["runtime"]["challenge_counts"],
            "issue_counts": report["runtime"]["issue_counts"],
            "answer_key_count": report["runtime"]["answer_key_count"],
            "correct_label_distribution": report["runtime"]["correct_label_distribution"],
        },
    }
    encoded = json.dumps(quality, ensure_ascii=False, indent=2)
    for path in DATA_QUALITY_PATHS:
        path.write_text(encoded, encoding="utf-8")


def main() -> int:
    report = build_summary_report()
    write_reports(report)
    print(json.dumps({"ok": True, "report": str(JSON_REPORT_PATH), "issues": report["runtime"]["issue_counts"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
