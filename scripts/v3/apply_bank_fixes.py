#!/Users/ylsuen/.venv/bin/python
"""Apply reviewed textbook-question fixes on top of the built runtime data.

Runs after build_v3 (see scripts/build_runtime_data.py) and can also be run on
its own against an existing runtime build. It is idempotent.

Two layers:

1. Rules for every textbook-sourced item (exam items are never touched):
   - R1: keep a dictionary reference (answer key ``dict_support`` and the
     "辞典参照" sentence of the explanation) only when its headword is the
     question's own target word; drop pure variant/stroke-count metadata and
     the stroke-count tail of a kept summary. The generator used to attach the
     entries of other characters in the sentence (拟 → 复, 长 → 人/者).
   - R2: "同词课文参照" becomes "同词其他课文用例（义项不一定相同）", because the
     listed sentences often use the word in a different sense.
   - R3: restore characters the textbook OCR dropped or stored as private-use
     code points (庖丁解牛 騞/軱, 谏逐客书 駃騠, 阿房宫赋 囷囷; basic-plane forms
     so every device renders them) in every runtime asset that quotes them.
   - R4: drop a leading closing quote mark that belongs to the previous
     sentence (”客有吹洞箫者…) from the material sentence and its quotes.

2. Per-item fixes from data/manual/v3/bank_fixes_*.json (two-round model review
   of 2026-11-04, then item-by-item judgement). A fix may set the stem, the
   material sentence, option texts, the correct label and a full explanation.
   The answer key's correct_text is always re-derived from the option at the
   correct label, and option analyses are regenerated for items whose options
   or label changed.

Every challenge item is patched in both places it is served from:
exam_questions (challenge_bank) and textbook_article_bank (article mode).
The script ends with a consistency gate and refreshes manifest sizes/hashes.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_DIR = ROOT / "data" / "runtime"
PRIVATE_RUNTIME_DIR = ROOT / "data" / "runtime_private"
PUBLIC_RUNTIME_DIR = ROOT / "public" / "runtime"
GENERATED_DIR = ROOT / "src" / "generated"
FIXES_DIR = ROOT / "data" / "manual" / "v3"
LABELS = ["A", "B", "C", "D"]

DICT_SEGMENT = " 辞典参照："
SAME_TERM_OLD = " 同词课文参照："
SAME_TERM_NEW = " 同词其他课文用例（义项不一定相同）："
TEXT_REPAIRS = [
    ("奏刀然", "奏刀騞然"),  # 庖丁解牛, character dropped by OCR
    ("而况大乎", "而况大軱乎"),  # 庖丁解牛, character dropped by OCR
    ("而骏良不实外厩", "而骏良駃騠不实外厩"),  # 谏逐客书, characters dropped by OCR
    ("盘盘焉，\ue296\ue296焉", "盘盘焉，囷囷焉"),  # 阿房宫赋, private-use code point for 囷
]
META_TAIL = re.compile(r"\s*部首外筆畫數：.*$")
VARIANT_ONLY = re.compile(r"^「.+?」的異體字。?$")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def truncate(text: str, limit: int = 90) -> str:
    cleaned = clean_text(text)
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 1] + "…"


def strip_reading(label: str) -> str:
    return re.sub(r"[（(].*?[)）]", "", str(label or "")).strip()


def target_words(key: dict[str, Any]) -> set[str]:
    ref = (key.get("textbook_support") or [{}])[0]
    words = {str(ref.get("headword") or ""), strip_reading(str(ref.get("label_text") or ""))}
    term = str(key.get("term_id") or "")
    if "::" in term:
        words.add(term.split("::", 1)[1])
    return {w for w in words if w}


def usable_dict_entries(key: dict[str, Any]) -> list[dict[str, Any]]:
    targets = target_words(key)
    kept = []
    for entry in key.get("dict_support") or []:
        if str(entry.get("headword") or "") not in targets:
            continue
        summary = META_TAIL.sub("", clean_text(str(entry.get("summary") or "")))
        if not summary or VARIANT_ONLY.match(summary):
            continue
        kept.append({**entry, "summary": summary})
    return kept


def rebuild_rule_explanation(explanation: str, entries: list[dict[str, Any]]) -> str:
    text = explanation
    start = text.find(DICT_SEGMENT)
    if start >= 0:
        end_candidates = [i for i in (text.find(SAME_TERM_OLD, start), text.find(SAME_TERM_NEW, start)) if i >= 0]
        end = min(end_candidates) if end_candidates else len(text)
        dict_text = ""
        if entries:
            dict_text = DICT_SEGMENT + " ".join(truncate(e["summary"]) + "。" for e in entries[:2])
        text = text[:start] + dict_text + text[end:]
    return text.replace(SAME_TERM_OLD, SAME_TERM_NEW)


def focus_of(item: dict[str, Any]) -> str:
    match = re.search(r"“(.+?)”", str(item.get("stem") or ""))
    return match.group(1) if match else str(item.get("term_id") or "").split("::", 1)[-1]


def option_analyses(item: dict[str, Any], correct_label: str) -> list[dict[str, Any]]:
    focus = focus_of(item)
    by_label = {o["label"]: o["text"] for o in item["options"]}
    answer = by_label[correct_label]
    return [
        {
            "label": label,
            "text": by_label[label],
            "is_correct": label == correct_label,
            "analysis": (
                f"本项正确。本句这一处“{focus}”应理解为“{answer}”。"
                if label == correct_label
                else f"本项不当，不符合本句这一处“{focus}”的意思；应理解为“{answer}”。"
            ),
        }
        for label in LABELS
        if label in by_label
    ]


def load_fixes() -> tuple[dict[str, dict[str, Any]], list[str]]:
    fixes: dict[str, dict[str, Any]] = {}
    sources = []
    for path in sorted(FIXES_DIR.glob("bank_fixes_*.json")):
        payload = read_json(path)
        sources.append(path.name)
        for fix in payload["items"]:
            if fix.get("decision") != "apply":
                continue
            if fix["id"] in fixes:
                raise SystemExit(f"duplicate fix for {fix['id']} in {path.name}")
            fixes[fix["id"]] = fix
    return fixes, sources


def replace_sentence(item: dict[str, Any], old: str, new: str) -> None:
    item["sentence"] = new
    item["context_window"] = [new if c == old else c for c in item.get("context_window") or []]
    for option in item["options"]:
        if old and option.get("sentence") == old:
            option["sentence"] = new
        if option.get("context_window"):
            option["context_window"] = list(item["context_window"])


def strip_leading_close_quote(item: dict[str, Any]) -> bool:
    old = str(item.get("sentence") or "")
    if not old.startswith("”"):
        return False
    replace_sentence(item, old, old.lstrip("”"))
    return True


def patch_item(item: dict[str, Any], fix: dict[str, Any]) -> None:
    if fix.get("stem"):
        item["stem"] = fix["stem"]
    if fix.get("sentence"):
        replace_sentence(item, item.get("sentence") or "", fix["sentence"])
    for option in item["options"]:
        new_text = (fix.get("options") or {}).get(option["label"])
        if new_text:
            option["text"] = new_text
            option["origin"] = "reviewed_fix"
    if fix.get("correct_label"):
        for option in item["options"]:
            if option["label"] == fix["correct_label"]:
                option["origin"] = "reviewed_fix_correct"
            elif option.get("origin") in ("textbook_note", "reviewed_fix_correct"):
                option["origin"] = "reviewed_fix"


def repaired_runtime_texts(manifest: dict[str, Any]) -> tuple[dict[Path, str], dict[str, int]]:
    """Read every runtime asset (and the answer keys) with TEXT_REPAIRS applied in memory."""
    texts: dict[Path, str] = {}
    counts: dict[str, int] = {}
    paths = [RUNTIME_DIR / s["file_name"] for a in manifest["assets"].values() for s in a["shards"]]
    for path in [*paths, PRIVATE_RUNTIME_DIR / "answer_keys.json"]:
        text = path.read_text(encoding="utf-8")
        new = text
        for old, fixed in TEXT_REPAIRS:
            new = new.replace(old, fixed)
        if new != text:
            counts[path.name] = sum(text.count(old) for old, _ in TEXT_REPAIRS)
        texts[path] = new
    return texts, counts


def main() -> int:
    fixes, fix_sources = load_fixes()
    manifest = read_json(RUNTIME_DIR / "manifest.json")
    texts, text_repairs = repaired_runtime_texts(manifest)
    exam_shards = [s["file_name"] for s in manifest["assets"]["exam_questions"]["shards"]]
    article_shards = [s["file_name"] for s in manifest["assets"]["textbook_article_bank"]["shards"]]
    exam_parts = {name: json.loads(texts[RUNTIME_DIR / name]) for name in exam_shards}
    article_parts = {name: json.loads(texts[RUNTIME_DIR / name]) for name in article_shards}
    keys_path = PRIVATE_RUNTIME_DIR / "answer_keys.json"
    keys = json.loads(texts[keys_path])

    served: dict[str, list[dict[str, Any]]] = {}
    for part in exam_parts.values():
        for items in (part.get("challenge_bank") or {}).values():
            for item in items:
                served.setdefault(item["challenge_id"], []).append(item)
    for part in article_parts.values():
        for article in part.values():
            for item in article.get("items") or []:
                served.setdefault(item["challenge_id"], []).append(item)

    missing = sorted(set(fixes) - set(served))
    if missing:
        raise SystemExit(f"fixes for unknown challenge ids: {missing[:5]}")

    stats = {"fixed_items": 0, "label_changes": 0, "dict_entries_dropped": 0, "dict_sentences_removed": 0, "leading_quotes_removed": 0}
    for challenge_id, copies in served.items():
        key = keys.get(challenge_id)
        if not key or key.get("source_type") != "textbook":
            continue
        fix = fixes.get(challenge_id)
        if fix:
            for item in copies:
                patch_item(item, fix)
            stats["fixed_items"] += 1
        stripped = [strip_leading_close_quote(item) for item in copies]
        if any(stripped):
            stats["leading_quotes_removed"] += 1
        key["explanation"] = key.get("explanation", "").replace("所在句为“”", "所在句为“")
        primary = copies[0]
        before_entries = len(key.get("dict_support") or [])
        entries = usable_dict_entries(key)
        stats["dict_entries_dropped"] += before_entries - len(entries)
        key["dict_support"] = entries
        if fix and fix.get("explanation"):
            key["explanation"] = fix["explanation"]
        else:
            had = DICT_SEGMENT in key.get("explanation", "")
            key["explanation"] = rebuild_rule_explanation(key.get("explanation", ""), entries)
            if had and DICT_SEGMENT not in key["explanation"]:
                stats["dict_sentences_removed"] += 1
        if fix:
            label = fix.get("correct_label") or key["correct_label"]
            if label != key["correct_label"]:
                stats["label_changes"] += 1
            key["correct_label"] = label
            key["correct_text"] = next(o["text"] for o in primary["options"] if o["label"] == label)
            if fix.get("options") or fix.get("correct_label") or fix.get("stem"):
                key["option_analyses"] = option_analyses(primary, label)
            key["review"] = {"batch": fix.get("batch", "bank-qa-20261104"), "aspects": fix.get("aspects", [])}

    # Consistency gate.
    errors = []
    for challenge_id, copies in served.items():
        key = keys.get(challenge_id)
        if not key:
            continue
        def comparable(item: dict[str, Any]) -> str:
            return json.dumps({k: v for k, v in item.items() if k != "article_id"}, ensure_ascii=False, sort_keys=True)

        first = comparable(copies[0])
        if any(comparable(c) != first for c in copies[1:]):
            errors.append(f"{challenge_id}: copies differ between exam bank and article bank")
        options = copies[0].get("options") or []
        by_label = {o["label"]: clean_text(o["text"]) for o in options}
        if key.get("source_type") != "textbook":
            continue
        if clean_text(key["correct_text"]) != by_label.get(key["correct_label"]):
            errors.append(f"{challenge_id}: correct_label {key['correct_label']} text differs from correct_text")
        if len(set(by_label.values())) != len(by_label):
            errors.append(f"{challenge_id}: duplicate option texts")
        for entry in key.get("dict_support") or []:
            if entry.get("headword") not in target_words(key):
                errors.append(f"{challenge_id}: dict entry {entry.get('headword')} is not the target word")
        if SAME_TERM_OLD in key.get("explanation", ""):
            errors.append(f"{challenge_id}: old same-term label left in explanation")
        fix = fixes.get(challenge_id)
        if fix and fix.get("explanation"):
            picks = re.findall(r"故选\s*([A-D])", key["explanation"])
            if picks and picks[-1] != key["correct_label"]:
                errors.append(f"{challenge_id}: explanation picks {picks[-1]} but key is {key['correct_label']}")
    if errors:
        print("\n".join(errors[:40]), file=sys.stderr)
        raise SystemExit(f"consistency gate failed: {len(errors)} errors")

    bank_paths = {RUNTIME_DIR / n for n in [*exam_shards, *article_shards]} | {keys_path}
    for path, text in texts.items():
        if path not in bank_paths and text != path.read_text(encoding="utf-8"):
            path.write_text(text, encoding="utf-8")
    for name, data in exam_parts.items():
        write_json(RUNTIME_DIR / name, data)
    for name, data in article_parts.items():
        write_json(RUNTIME_DIR / name, data)
    write_json(keys_path, keys)
    write_json(GENERATED_DIR / "answer_keys.json", keys)

    for asset in manifest["assets"].values():
        for shard in asset["shards"]:
            path = RUNTIME_DIR / shard["file_name"]
            shard["size_bytes"] = path.stat().st_size
            shard["sha256"] = sha256_file(path)
    if (manifest.get("bank_fixes") or {}).get("sources") != fix_sources:
        # first application of this fix set; a re-run keeps the original record
        manifest["bank_fixes"] = {"sources": fix_sources, "text_repairs": text_repairs, **stats}
    write_json(RUNTIME_DIR / "manifest.json", manifest)

    if PUBLIC_RUNTIME_DIR.exists():
        all_shards = [s["file_name"] for a in manifest["assets"].values() for s in a["shards"]]
        for name in [*all_shards, "manifest.json"]:
            (PUBLIC_RUNTIME_DIR / name).write_bytes((RUNTIME_DIR / name).read_bytes())

    print(json.dumps({"fix_sources": fix_sources, "text_repairs": text_repairs, **stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
