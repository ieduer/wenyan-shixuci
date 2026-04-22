#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path("/Users/ylsuen/textbook_ai_migration")
MANIFEST_PATH = SOURCE_ROOT / "platform" / "backend" / "textbook_classics_manifest.json"
VERSION_MANIFEST_PATH = SOURCE_ROOT / "platform" / "backend" / "textbook_version_manifest.json.pre_chuzhong"
MINERU_OUTPUT_ROOT = SOURCE_ROOT / "data" / "mineru_output"
JUNIOR_MD_PATH = SOURCE_ROOT / "export" / "notebooklm" / "初中_语文.md"
SENIOR_MD_PATH = SOURCE_ROOT / "export" / "notebooklm" / "高中_语文.md"
XUCI_PATH = SOURCE_ROOT / "data" / "index" / "dict_exam_xuci.json"
SHICI_PATH = SOURCE_ROOT / "data" / "index" / "dict_exam_shici.json"
MOE_REVISED_PATH = SOURCE_ROOT / "data" / "index" / "dict_moe_revised.db"
MOE_IDIOMS_PATH = SOURCE_ROOT / "data" / "index" / "dict_moe_idioms.db"

EXPECTED_BEIJING_YEARS = list(range(2002, 2026))
EXPECTED_SQLITE_TABLES = {"entries", "metadata"}
EXPECTED_ENTRY_COLUMNS = {"id", "headword", "content_text", "raw_json"}
NOTE_PATTERN = re.compile(r"〔([^〕]+)〕")
TITLE_ALIASES = {
    "芣苢": ["荣苣", "茉苣"],
}


@dataclass
class CheckFailure:
    code: str
    detail: str


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_title(value: str) -> str:
    text = str(value or "")
    text = text.replace("／", "/").replace("·", "").replace("•", "")
    text = re.sub(r"[\s#\-\(\)（）《》“”\"'，,。:：;；?!？！·\[\]【】]", "", text)
    return text.lower()


def split_title_parts(value: str) -> list[str]:
    return [part.strip() for part in re.split(r"/|／", str(value or "")) if part.strip()]


def title_part_variants(title_part: str) -> list[str]:
    cleaned = str(title_part or "").strip()
    variants = [cleaned]
    variants.extend(TITLE_ALIASES.get(cleaned, []))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in variants:
        normalized = normalize_title(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(item)
    return deduped


def title_present_in_corpus(title: str, corpus: str) -> bool:
    normalized_corpus = normalize_title(corpus)
    parts = split_title_parts(title)
    if not parts:
        return False
    return all(
        any(normalize_title(variant) in normalized_corpus for variant in title_part_variants(part))
        for part in parts
    )


def resolve_language_book_paths() -> dict[str, Path]:
    payload = load_json(VERSION_MANIFEST_PATH)
    by_book_key = payload.get("by_book_key", {}) if isinstance(payload, dict) else {}
    resolved: dict[str, Path] = {}
    for book_key in sorted(by_book_key):
        if "_语文_" not in str(book_key):
            continue
        direct_dir = MINERU_OUTPUT_ROOT / str(book_key)
        candidates: list[Path] = []
        if direct_dir.exists():
            candidates.extend(sorted(direct_dir.glob("*.md")))
        if not candidates:
            candidates.extend(
                sorted(path for path in MINERU_OUTPUT_ROOT.glob(f"{book_key}*/**/*.md") if path.name.endswith(".md"))
            )
        if candidates:
            resolved[str(book_key)] = candidates[0]
    return resolved


def open_sqlite_readonly(path: Path) -> tuple[sqlite3.Connection, tempfile.TemporaryDirectory[str] | None]:
    tmp_dir: tempfile.TemporaryDirectory[str] | None = None
    try:
        conn = sqlite3.connect(str(path))
        conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
        return conn, None
    except sqlite3.Error:
        tmp_dir = tempfile.TemporaryDirectory(prefix="wenyan-db-")
        tmp_path = Path(tmp_dir.name) / path.name
        shutil.copy2(path, tmp_path)
        conn = sqlite3.connect(str(tmp_path))
        conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
        return conn, tmp_dir


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _entry_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(entries)").fetchall()
    return {str(row[1]) for row in rows}


def _collect_years(question_docs: dict[str, dict]) -> list[int]:
    years = set()
    for doc in question_docs.values():
        year = doc.get("year")
        if isinstance(year, int):
            years.add(year)
        elif isinstance(year, str) and year.isdigit():
            years.add(int(year))
    return sorted(years)


def _group_option_labels(terms: list[dict]) -> dict[tuple[str, int, str], set[str]]:
    grouped: dict[tuple[str, int, str], set[str]] = {}
    for term in terms:
        for occ in term.get("occurrences", []):
            if occ.get("scope") != "beijing":
                continue
            subtype = str(occ.get("question_subtype") or "")
            if subtype not in {
                "xuci_compare_same",
                "xuci_compare_diff",
                "xuci_explanation",
                "shici_explanation",
            }:
                continue
            paper_key = str(occ.get("paper_key") or "")
            question_number = int(occ.get("question_number") or 0)
            option_label = str(occ.get("option_label") or "").strip().upper()
            key = (paper_key, question_number, subtype)
            grouped.setdefault(key, set())
            if option_label:
                grouped[key].add(option_label)
    return grouped


def _check_option_sets(grouped: dict[tuple[str, int, str], set[str]]) -> list[str]:
    failures: list[str] = []
    for key, labels in sorted(grouped.items()):
        paper_key, question_number, subtype = key
        if subtype in {
            "xuci_compare_same",
            "xuci_compare_diff",
            "xuci_explanation",
            "shici_explanation",
        } and labels != {"A", "B", "C", "D"}:
            failures.append(
                f"{paper_key} q{question_number} {subtype} option labels != A/B/C/D: {sorted(labels)}"
            )
    return failures


def _check_term_occurrences(terms: list[dict]) -> list[str]:
    failures: list[str] = []
    for term in terms:
        headword = str(term.get("headword") or "")
        for occ in term.get("occurrences", []):
            year = occ.get("year")
            question_number = occ.get("question_number")
            if occ.get("scope") == "beijing" and (not isinstance(year, int) or year not in EXPECTED_BEIJING_YEARS):
                failures.append(f"{headword}: invalid Beijing year {year!r}")
            subtype = str(occ.get("question_subtype") or "")
            if subtype and question_number in (None, "", 0):
                failures.append(f"{headword}: missing question_number for subtype={subtype}")
    return failures


def _manifest_alignment(manifest: dict[str, list[dict]], corpora: dict[str, str]) -> dict[str, Any]:
    total = 0
    matched = 0
    missing: list[str] = []
    for book_key, items in manifest.items():
        corpus = corpora.get(book_key, "")
        for item in items:
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            total += 1
            if title_present_in_corpus(title, corpus):
                matched += 1
            else:
                missing.append(f"{book_key}:{title}")
    return {
        "total_titles": total,
        "matched_titles": matched,
        "missing_titles": missing[:80],
    }


def collect_source_report() -> dict[str, Any]:
    failures: list[CheckFailure] = []
    warnings: list[str] = []

    required_paths = [
        MANIFEST_PATH,
        VERSION_MANIFEST_PATH,
        XUCI_PATH,
        SHICI_PATH,
        MOE_REVISED_PATH,
        MOE_IDIOMS_PATH,
    ]
    file_stats: dict[str, Any] = {}

    for path in required_paths:
        if not path.exists():
            failures.append(CheckFailure("missing_file", str(path)))
            continue
        size = path.stat().st_size
        file_stats[str(path)] = {"size_bytes": size}
        if size < 128:
            failures.append(CheckFailure("file_too_small", f"{path} ({size} bytes)"))

    if failures:
        return {
            "ok": False,
            "failures": [failure.__dict__ for failure in failures],
            "warnings": warnings,
            "file_stats": file_stats,
        }

    manifest = load_json(MANIFEST_PATH)
    language_book_paths = resolve_language_book_paths()
    if len(language_book_paths) < 10:
        failures.append(CheckFailure("language_books_missing", f"resolved={len(language_book_paths)}"))
    for path in language_book_paths.values():
        if not path.exists():
            failures.append(CheckFailure("missing_mineru_book", str(path)))
            continue
        size = path.stat().st_size
        file_stats[str(path)] = {"size_bytes": size}
        if size < 512:
            failures.append(CheckFailure("mineru_book_too_small", f"{path} ({size} bytes)"))
    xuci = load_json(XUCI_PATH)
    shici = load_json(SHICI_PATH)

    corpora = {
        book_key: path.read_text(encoding="utf-8")
        for book_key, path in language_book_paths.items()
    }
    alignment = _manifest_alignment(manifest, corpora)
    if alignment["matched_titles"] != alignment["total_titles"]:
        message = f"matched={alignment['matched_titles']} total={alignment['total_titles']}"
        if alignment["matched_titles"] / max(1, alignment["total_titles"]) >= 0.95:
            warnings.append(message)
        else:
            failures.append(CheckFailure("manifest_alignment", message))

    xuci_docs = xuci.get("question_docs", {})
    shici_docs = shici.get("question_docs", {})
    merged_docs = {**xuci_docs, **shici_docs}
    beijing_years = sorted(
        year for year in _collect_years(merged_docs) if year in EXPECTED_BEIJING_YEARS
    )
    if beijing_years != EXPECTED_BEIJING_YEARS:
        failures.append(
            CheckFailure(
                "beijing_year_range",
                f"expected={EXPECTED_BEIJING_YEARS[0]}-{EXPECTED_BEIJING_YEARS[-1]} actual={beijing_years[:1]}...{beijing_years[-1:]}"
            )
        )

    xuci_terms = xuci.get("terms", [])
    shici_terms = shici.get("terms", [])
    failures.extend(CheckFailure("term_occurrence", item) for item in _check_term_occurrences(xuci_terms))
    failures.extend(CheckFailure("term_occurrence", item) for item in _check_term_occurrences(shici_terms))

    option_failures = _check_option_sets(_group_option_labels(xuci_terms + shici_terms))
    warnings.extend(option_failures)

    sqlite_reports: dict[str, Any] = {}
    for label, db_path in {
        "moe_revised": MOE_REVISED_PATH,
        "moe_idioms": MOE_IDIOMS_PATH,
    }.items():
        temp_ctx: tempfile.TemporaryDirectory[str] | None = None
        try:
            conn, temp_ctx = open_sqlite_readonly(db_path)
            tables = _table_names(conn)
            columns = _entry_columns(conn)
            sqlite_reports[label] = {
                "tables": sorted(tables),
                "entry_columns": sorted(columns),
            }
            if not EXPECTED_SQLITE_TABLES.issubset(tables):
                failures.append(CheckFailure("sqlite_tables", f"{label}:{sorted(tables)}"))
            if not EXPECTED_ENTRY_COLUMNS.issubset(columns):
                failures.append(CheckFailure("sqlite_columns", f"{label}:{sorted(columns)}"))
        except sqlite3.Error as error:
            failures.append(CheckFailure("sqlite_open", f"{label}:{error}"))
        finally:
            if temp_ctx is not None:
                temp_ctx.cleanup()

    report = {
        "ok": not failures,
        "failures": [failure.__dict__ for failure in failures],
        "warnings": warnings,
        "file_stats": file_stats,
        "alignment": alignment,
        "beijing_years": beijing_years,
        "question_doc_count": len(merged_docs),
        "term_counts": {
            "function_word": len(xuci_terms),
            "content_word": len(shici_terms),
        },
        "sqlite_reports": sqlite_reports,
    }

    if not report["ok"] and alignment["missing_titles"]:
        report["alignment"]["missing_titles_preview"] = alignment["missing_titles"][:20]
    return report


def ensure_sources_or_raise(report: dict[str, Any]) -> None:
    if report.get("ok"):
        return
    messages = [f"{item['code']}: {item['detail']}" for item in report.get("failures", [])]
    raise SystemExit("Source check failed:\n- " + "\n- ".join(messages))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Print JSON only")
    args = parser.parse_args()

    report = collect_source_report()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report.get("ok") else 1

    print("Source Check Report")
    print(f"ok: {report['ok']}")
    print(f"question_doc_count: {report.get('question_doc_count', 0)}")
    print(f"beijing_years: {report.get('beijing_years', [])}")
    print(f"term_counts: {report.get('term_counts', {})}")
    print(
        "manifest_alignment:"
        f" {report.get('alignment', {}).get('matched_titles', 0)}"
        f"/{report.get('alignment', {}).get('total_titles', 0)}"
    )
    if report.get("failures"):
        print("failures:")
        for item in report["failures"]:
            print(f"- {item['code']}: {item['detail']}")
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
