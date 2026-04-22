#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from build_runtime_data import (
    PUBLIC_RUNTIME_DIR,
    QUESTION_TYPES,
    RUNTIME_MIRROR_DIR,
    answer_label_for_question,
    clean_gloss,
    clean_text,
    derive_canonical_content_headword,
    extract_function_option_sentences,
    find_sentence_context,
    load_json,
    looks_like_clean_gloss,
    looks_like_passage_context,
    looks_like_sentence_context,
    merge_content_terms,
    merge_question_docs,
    normalize_occurrence_headword,
    refine_content_headword_with_qdoc,
)
from check_sources import (
    REPO_ROOT,
    SHICI_PATH,
    XUCI_PATH,
    collect_source_report,
)


RUNTIME_DIR = REPO_ROOT / "public" / "runtime"
DOCS_DIR = REPO_ROOT / "docs"
JSON_REPORT_PATH = DOCS_DIR / "DATA_AUDIT_REPORT.json"
MD_REPORT_PATH = DOCS_DIR / "DATA_AUDIT_REPORT.md"
DATA_QUALITY_PATHS = [
    PUBLIC_RUNTIME_DIR / "data_quality.json",
    RUNTIME_MIRROR_DIR / "data_quality.json",
]


def option_headword_map_from_question(terms: list[dict[str, Any]], paper_key: str, question_number: int, subtype: str) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for term in terms:
        for occurrence in term.get("occurrences", []):
            if (
                occurrence.get("paper_key") == paper_key
                and int(occurrence.get("question_number") or 0) == question_number
                and str(occurrence.get("question_subtype") or "") == subtype
            ):
                label = str(occurrence.get("option_label") or "").strip().upper()
                if label:
                    grouped[label].append(str(term.get("headword") or ""))
    return {label: sorted(set(values)) for label, values in grouped.items()}


def audit_xuci_groups(
    raw_function_terms: list[dict[str, Any]],
    raw_content_terms: list[dict[str, Any]],
    question_docs: dict[str, dict[str, Any]],
    runtime_exam_questions: dict[str, Any],
) -> dict[str, Any]:
    grouped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for term in raw_function_terms:
        for occurrence in term.get("occurrences", []):
            if occurrence.get("scope") != "beijing":
                continue
            subtype = str(occurrence.get("question_subtype") or "")
            if subtype not in {"xuci_compare_same", "xuci_compare_diff"}:
                continue
            key = (str(occurrence.get("paper_key") or ""), int(occurrence.get("question_number") or 0), subtype)
            group = grouped.setdefault(
                key,
                {
                    "paper_key": key[0],
                    "question_number": key[1],
                    "question_subtype": subtype,
                    "year": occurrence.get("year"),
                    "paper": occurrence.get("paper"),
                    "options": defaultdict(list),
                },
            )
            option_label = str(occurrence.get("option_label") or "").strip().upper()
            group["options"][option_label].append({**occurrence, "headword": term["headword"]})

    emitted_keys = {
        (item["paper_key"], int(item["question_number"]), "xuci_compare_same" if item["stem"].endswith("相同的一项是") else "xuci_compare_diff")
        for item in runtime_exam_questions["challenge_bank"]["xuci_pair_compare"]
    }
    status_counter: Counter[str] = Counter()
    group_reports: list[dict[str, Any]] = []
    complete_group_count = 0

    for key in sorted(grouped, key=lambda item: (grouped[item]["year"] or 0, item[1])):
        paper_key, question_number, subtype = key
        group = grouped[key]
        qdoc = question_docs.get(paper_key, {})
        answer_text = str(qdoc.get("answer") or "")
        parsed_answer = answer_label_for_question(answer_text, question_number)
        option_labels = sorted(label for label in group["options"] if label)
        if set(option_labels) == {"A", "B", "C", "D"}:
            complete_group_count += 1

        segmentation_failures: list[str] = []
        option_examples: dict[str, Any] = {}
        for label in ("A", "B", "C", "D"):
            entries = sorted(group["options"].get(label, []), key=lambda item: int(item.get("pair_index") or 0))
            if not entries:
                continue
            sentences = extract_function_option_sentences(entries)
            option_examples[label] = {
                "headwords": sorted({str(item.get("headword") or "") for item in entries}),
                "sentences": sentences,
            }
            if len(sentences) < 2:
                segmentation_failures.append(label)

        raw_content_heads = option_headword_map_from_question(raw_content_terms, paper_key, question_number, subtype)
        if key in emitted_keys:
            status = "emitted"
        elif parsed_answer not in {"A", "B", "C", "D"}:
            status = "missing_answer"
        elif set(option_labels) != {"A", "B", "C", "D"}:
            status = "incomplete_option_set"
        elif segmentation_failures:
            status = "segmentation_failed"
        else:
            status = "filtered_unknown"
        status_counter[status] += 1
        group_reports.append(
            {
                "paper_key": paper_key,
                "year": group["year"],
                "paper": group["paper"],
                "question_number": question_number,
                "question_subtype": subtype,
                "status": status,
                "parsed_answer": parsed_answer or None,
                "option_labels_present": option_labels,
                "segmentation_failures": segmentation_failures,
                "content_compare_occurrences": raw_content_heads,
                "option_examples": option_examples,
            }
        )

    return {
        "raw_group_count": len(grouped),
        "complete_function_option_group_count": complete_group_count,
        "emitted_count": len(runtime_exam_questions["challenge_bank"]["xuci_pair_compare"]),
        "status_counts": dict(status_counter),
        "groups": group_reports,
    }


def audit_content_terms(
    raw_content_terms: list[dict[str, Any]],
    question_docs: dict[str, dict[str, Any]],
    runtime_terms_content: list[dict[str, Any]],
    runtime_exam_questions: dict[str, Any],
) -> dict[str, Any]:
    normalized_terms = merge_content_terms(raw_content_terms, question_docs)
    replacements: list[dict[str, Any]] = []
    for term in raw_content_terms:
        raw_headword = clean_text(str(term.get("headword") or ""))
        canonical = derive_canonical_content_headword(raw_headword, list(term.get("occurrences", [])))
        canonical = refine_content_headword_with_qdoc(raw_headword, canonical, list(term.get("occurrences", [])), question_docs)
        if canonical != raw_headword:
            replacements.append(
                {
                    "raw_headword": raw_headword,
                    "canonical_headword": canonical,
                    "occurrence_count": len(term.get("occurrences", [])),
                    "examples": [clean_text(str(item.get("excerpt") or "")) for item in term.get("occurrences", [])[:2]],
                }
            )

    rejection_counter: Counter[str] = Counter()
    rejected_examples: list[dict[str, Any]] = []
    salvaged_examples: list[dict[str, Any]] = []
    total_occurrences = 0
    accepted_occurrences = 0

    for term in normalized_terms:
        headword = str(term["headword"])
        for occurrence in term.get("occurrences", []):
            total_occurrences += 1
            gloss = clean_gloss(headword, str(occurrence.get("gloss") or ""), str(occurrence.get("excerpt") or ""))
            qdoc = question_docs.get(str(occurrence.get("paper_key") or ""), {})
            raw_excerpt = str(occurrence.get("excerpt") or "")
            raw_context = clean_text(raw_excerpt)
            sanitized = find_sentence_context(str(qdoc.get("text") or ""), raw_excerpt, normalize_occurrence_headword(headword, raw_excerpt))
            raw_valid = looks_like_sentence_context(raw_context, headword)
            sanitized_valid = bool(gloss) and looks_like_sentence_context(sanitized, headword)
            if raw_valid is False and sanitized_valid:
                salvaged_examples.append(
                    {
                        "headword": headword,
                        "paper_key": occurrence.get("paper_key"),
                        "question_number": occurrence.get("question_number"),
                        "raw_excerpt": raw_context,
                        "sanitized_context": sanitized,
                    }
                )
            if not gloss:
                rejection_counter["missing_gloss"] += 1
                if len(rejected_examples) < 12:
                    rejected_examples.append(
                        {
                            "reason": "missing_gloss",
                            "headword": headword,
                            "paper_key": occurrence.get("paper_key"),
                            "question_number": occurrence.get("question_number"),
                            "raw_excerpt": raw_context,
                        }
                    )
                continue
            if not looks_like_clean_gloss(gloss):
                rejection_counter["invalid_gloss"] += 1
                if len(rejected_examples) < 12:
                    rejected_examples.append(
                        {
                            "reason": "invalid_gloss",
                            "headword": headword,
                            "paper_key": occurrence.get("paper_key"),
                            "question_number": occurrence.get("question_number"),
                            "raw_excerpt": raw_context,
                            "gloss": gloss,
                        }
                    )
                continue
            if not sanitized:
                rejection_counter["empty_context"] += 1
                if len(rejected_examples) < 12:
                    rejected_examples.append(
                        {
                            "reason": "empty_context",
                            "headword": headword,
                            "paper_key": occurrence.get("paper_key"),
                            "question_number": occurrence.get("question_number"),
                            "raw_excerpt": raw_context,
                        }
                    )
                continue
            if not looks_like_sentence_context(sanitized, headword):
                rejection_counter["invalid_context"] += 1
                if len(rejected_examples) < 12:
                    rejected_examples.append(
                        {
                            "reason": "invalid_context",
                            "headword": headword,
                            "paper_key": occurrence.get("paper_key"),
                            "question_number": occurrence.get("question_number"),
                            "raw_excerpt": raw_context,
                            "sanitized_context": sanitized,
                        }
                    )
                continue
            accepted_occurrences += 1

    runtime_non_simple = [
        {
            "headword": item["headword"],
            "needs_manual_review": item["needs_manual_review"],
            "dict_ref_count": len(item.get("dict_refs", [])),
        }
        for item in runtime_terms_content
        if len(item["headword"]) > 3
    ]

    runtime_question_counts = {
        question_type: len(runtime_exam_questions["challenge_bank"][question_type])
        for question_type in QUESTION_TYPES
        if question_type != "xuci_pair_compare"
    }

    return {
        "raw_term_count": len(raw_content_terms),
        "normalized_term_count": len(normalized_terms),
        "headword_replacements": replacements,
        "total_occurrences": total_occurrences,
        "accepted_occurrences": accepted_occurrences,
        "rejected_occurrence_counts": dict(rejection_counter),
        "rejected_examples": rejected_examples,
        "salvaged_polluted_examples": salvaged_examples[:12],
        "runtime_non_simple_headwords": runtime_non_simple,
        "runtime_question_counts": runtime_question_counts,
    }


def audit_runtime_question_bank(runtime_exam_questions: dict[str, Any]) -> dict[str, Any]:
    issues: dict[str, list[dict[str, Any]]] = defaultdict(list)
    counts = {
        question_type: len(items)
        for question_type, items in runtime_exam_questions["challenge_bank"].items()
    }

    for item in runtime_exam_questions["challenge_bank"]["xuci_pair_compare"]:
        if len(item.get("options", [])) != 4:
            issues["xuci_pair_compare"].append({"challenge_id": item["challenge_id"], "issue": "option_count"})
        if item.get("answer", {}).get("label") not in {"A", "B", "C", "D"}:
            issues["xuci_pair_compare"].append({"challenge_id": item["challenge_id"], "issue": "missing_answer"})
        for option in item.get("options", []):
            if len(option.get("sentences", [])) != 2:
                issues["xuci_pair_compare"].append(
                    {
                        "challenge_id": item["challenge_id"],
                        "issue": "pair_sentence_count",
                        "label": option.get("label"),
                        "sentences": option.get("sentences"),
                    }
                )

    for question_type in ("content_gloss", "translation_keypoint", "sentence_meaning", "passage_meaning", "analysis_short"):
        context_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for item in runtime_exam_questions["challenge_bank"][question_type]:
            context_key = (
                item.get("headword"),
                item.get("paper_key"),
                item.get("question_number"),
                item.get("sentence") or item.get("passage") or "",
            )
            context_groups[context_key].append(item)
            option_texts = [option.get("text") for option in item.get("options", []) if option.get("text")]
            if len(option_texts) != len(set(option_texts)):
                issues[question_type].append({"challenge_id": item["challenge_id"], "issue": "duplicate_options"})
            if question_type == "analysis_short":
                if item.get("answer", {}).get("label") not in {"A", "B", "C", "D"}:
                    issues[question_type].append({"challenge_id": item["challenge_id"], "issue": "missing_answer"})
            elif item.get("answer", {}).get("label") not in {"A", "B", "C", "D"}:
                issues[question_type].append({"challenge_id": item["challenge_id"], "issue": "missing_answer"})
            if question_type in {"content_gloss", "translation_keypoint"}:
                answer_label = item.get("answer", {}).get("label")
                option_map = {option.get("label"): option.get("text") for option in item.get("options", [])}
                answer_text = clean_text(option_map.get(answer_label) or "")
                if not looks_like_clean_gloss(answer_text):
                    issues[question_type].append(
                        {
                            "challenge_id": item["challenge_id"],
                            "issue": "polluted_correct_option",
                            "headword": item.get("headword"),
                            "answer_text": answer_text,
                        }
                    )
            evidence_text = item.get("sentence") or item.get("passage") or ""
            evidence_ok = (
                looks_like_passage_context(evidence_text, str(item.get("headword") or ""))
                if question_type == "passage_meaning"
                else looks_like_sentence_context(evidence_text, str(item.get("headword") or ""))
            )
            if not evidence_ok:
                issues[question_type].append(
                    {
                        "challenge_id": item["challenge_id"],
                        "issue": "invalid_evidence_text",
                        "headword": item.get("headword"),
                        "evidence_text": evidence_text,
                    }
                )
        for grouped_items in context_groups.values():
            if len(grouped_items) <= 1:
                continue
            issues[question_type].append(
                {
                    "challenge_ids": [item["challenge_id"] for item in grouped_items],
                    "issue": "duplicate_context_question",
                    "headword": grouped_items[0].get("headword"),
                    "paper_key": grouped_items[0].get("paper_key"),
                    "question_number": grouped_items[0].get("question_number"),
                    "evidence_text": grouped_items[0].get("sentence") or grouped_items[0].get("passage"),
                }
            )

    return {
        "challenge_counts": counts,
        "issue_counts": {question_type: len(items) for question_type, items in issues.items()},
        "issues": issues,
    }


def build_architecture_reflection(report: dict[str, Any]) -> list[str]:
    xuci = report["xuci"]
    content = report["content"]
    reflection = [
        "北京卷四选项八短句题不能假设所有原始 excerpt 都带完整句号；应优先按加点词标记位置切分，再用标点作回退。",
        "北京卷比较题不能假设 question_docs.answer 一定存在。答案字段为空时，题面可保存，但不可进入可答题运行库。",
        "“虚词比较题”不能直接等同于“所有选项都来自 xuci 词表”。当前原始索引里存在同题缺失部分选项的情况，运行时必须把缺项题标成证据不足，而不是拼凑四项。",
        "实词题的原始 excerpt 不能直接信任。带“词:释义”“翻译为”“参考答案”等污染标记的 excerpt 必须先经语境回收，再决定是否可出题。",
        "实词词头也不能直接信任。像“之衣柜籍”“以为贺预”“典禁旅典”这类污染词头需要在构建期规范化，否则会污染词表、词典映射和错题追踪。",
    ]
    reflection.append(
        f"本次真实审计后，纯功能词比较题从 {xuci['status_counts'].get('emitted', 0)} / {xuci['raw_group_count']} 可运行，未运行题主要由缺答案或缺完整选项引起。"
    )
    reflection.append(
        f"实词侧共有 {len(content['headword_replacements'])} 个污染词头被规范化，运行时仍保留的长词头仅剩 {len(content['runtime_non_simple_headwords'])} 个，其中应视为合法词组单独处理。"
    )
    return reflection


def render_markdown(report: dict[str, Any]) -> str:
    source = report["source_report"]
    xuci = report["xuci"]
    content = report["content"]
    runtime = report["runtime"]
    emitted_examples = [item for item in xuci["groups"] if item["status"] == "emitted"][:5]
    blocked_examples = [item for item in xuci["groups"] if item["status"] != "emitted"][:8]
    lines = [
        "# DATA_AUDIT_REPORT",
        "",
        f"- Generated at: {report['generated_at']}",
        f"- Source check ok: {source['ok']}",
        f"- Question docs: {source.get('question_doc_count')}",
        f"- Beijing year coverage: {source.get('beijing_years', [None])[0]}-{source.get('beijing_years', [None])[-1]}",
        "",
        "## Summary",
        "",
        f"- Pure function-word compare groups in raw data: {xuci['raw_group_count']}",
        f"- Pure function-word compare groups with complete A-D options: {xuci['complete_function_option_group_count']}",
        f"- Emitted function-word compare challenges: {xuci['emitted_count']}",
        f"- Content headword replacements applied: {len(content['headword_replacements'])}",
        f"- Content occurrences accepted into runtime: {content['accepted_occurrences']} / {content['total_occurrences']}",
        "",
        "## Xuci Audit",
        "",
        f"- Status counts: `{json.dumps(xuci['status_counts'], ensure_ascii=False)}`",
        "",
        "### Emitted Examples",
        "",
    ]
    for item in emitted_examples:
        lines.append(
            f"- {item['year']} q{item['question_number']} {item['question_subtype']}: answer={item['parsed_answer']} labels={','.join(item['option_labels_present'])}"
        )
    lines.extend(["", "### Blocked Examples", ""])
    for item in blocked_examples:
        lines.append(
            f"- {item['year']} q{item['question_number']} {item['question_subtype']}: status={item['status']} labels={','.join(item['option_labels_present']) or 'none'} answer={item['parsed_answer'] or 'missing'}"
        )

    lines.extend(["", "## Content Audit", ""])
    for item in content["headword_replacements"]:
        lines.append(
            f"- normalized `{item['raw_headword']}` -> `{item['canonical_headword']}`"
        )
    if not content["headword_replacements"]:
        lines.append("- no polluted headwords were normalized")
    lines.extend(
        [
            "",
            f"- Rejected occurrence counts: `{json.dumps(content['rejected_occurrence_counts'], ensure_ascii=False)}`",
            f"- Remaining non-simple runtime headwords: `{json.dumps(content['runtime_non_simple_headwords'], ensure_ascii=False)}`",
            "",
            "### Salvaged Polluted Examples",
            "",
        ]
    )
    for item in content["salvaged_polluted_examples"][:6]:
        lines.append(
            f"- `{item['headword']}` {item['paper_key']} q{item['question_number']}: raw=`{item['raw_excerpt'][:60]}` -> sanitized=`{item['sanitized_context']}`"
        )

    lines.extend(["", "## Runtime Validity", ""])
    lines.append(f"- Challenge counts: `{json.dumps(runtime['challenge_counts'], ensure_ascii=False)}`")
    lines.append(f"- Issue counts: `{json.dumps(runtime['issue_counts'], ensure_ascii=False)}`")

    lines.extend(["", "## Architecture Reflection", ""])
    for item in report["architecture_reflection"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    source_report = collect_source_report()
    xuci = load_json(XUCI_PATH)
    shici = load_json(SHICI_PATH)
    runtime_exam_questions = load_json(RUNTIME_DIR / "exam_questions.json")
    runtime_terms_content = load_json(RUNTIME_DIR / "terms_content.json")
    question_docs = merge_question_docs(xuci, shici)

    xuci_report = audit_xuci_groups(
        list(xuci.get("terms", [])),
        list(shici.get("terms", [])),
        question_docs,
        runtime_exam_questions,
    )
    content_report = audit_content_terms(
        list(shici.get("terms", [])),
        question_docs,
        runtime_terms_content,
        runtime_exam_questions,
    )
    runtime_report = audit_runtime_question_bank(runtime_exam_questions)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_report": source_report,
        "xuci": xuci_report,
        "content": content_report,
        "runtime": runtime_report,
    }
    report["architecture_reflection"] = build_architecture_reflection(report)

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    JSON_REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    MD_REPORT_PATH.write_text(render_markdown(report), encoding="utf-8")
    runtime_quality = {
        "generated_at": report["generated_at"],
        "source_ok": bool(source_report.get("ok")),
        "xuci": {
            "raw_group_count": xuci_report["raw_group_count"],
            "complete_function_option_group_count": xuci_report["complete_function_option_group_count"],
            "emitted_count": xuci_report["emitted_count"],
            "status_counts": xuci_report["status_counts"],
        },
        "content": {
            "accepted_occurrences": content_report["accepted_occurrences"],
            "total_occurrences": content_report["total_occurrences"],
            "rejected_occurrence_counts": content_report["rejected_occurrence_counts"],
            "headword_replacements_count": len(content_report["headword_replacements"]),
        },
        "runtime": {
            "challenge_counts": runtime_report["challenge_counts"],
            "issue_counts": runtime_report["issue_counts"],
        },
    }
    for path in DATA_QUALITY_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(runtime_quality, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "ok": True,
                "json_report": str(JSON_REPORT_PATH),
                "md_report": str(MD_REPORT_PATH),
                "runtime_quality": str(DATA_QUALITY_PATHS[0]),
                "xuci_status_counts": xuci_report["status_counts"],
                "runtime_issue_counts": runtime_report["issue_counts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
