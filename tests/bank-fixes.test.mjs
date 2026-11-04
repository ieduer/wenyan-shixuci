// Consistency gate for the served question bank after scripts/v3/apply_bank_fixes.py.
// Reads the runtime exactly as the Worker does (manifest shards from public/runtime,
// answer keys bundled from src/generated/answer_keys.json).
// Optional: BASELINE_RUNTIME_DIR=<dir with the pre-fix runtime + answer_keys.json>
// additionally proves that exam items and unreviewed textbook options did not change.
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const runtimeDir = path.join(root, "public", "runtime");
const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8"));
const haveRuntime = fs.existsSync(path.join(runtimeDir, "manifest.json"));

function loadBank(dir, keysPath) {
  const manifest = readJson(path.join(dir, "manifest.json"));
  const shard = (asset) => manifest.assets[asset].shards.map((s) => readJson(path.join(dir, s.file_name)));
  const exam = Object.assign({}, ...shard("exam_questions"));
  const articles = Object.assign({}, ...shard("textbook_article_bank"));
  const items = new Map();
  for (const list of Object.values(exam.challenge_bank)) for (const q of list) items.set(q.challenge_id, q);
  const articleItems = [];
  for (const a of Object.values(articles)) for (const q of a.items || []) articleItems.push(q);
  return { manifest, items, articleItems, keys: readJson(keysPath) };
}

const clean = (s) => String(s || "").replace(/\s+/g, " ").trim();

test("every served item's keyed label points at its correct_text", { skip: !haveRuntime }, () => {
  const { items, articleItems, keys } = loadBank(runtimeDir, path.join(root, "src", "generated", "answer_keys.json"));
  assert.equal(Object.keys(keys).length, 1858);
  let checked = 0;
  for (const [id, q] of items) {
    const k = keys[id];
    assert.ok(k, "missing key " + id);
    const opt = q.options.find((o) => o.label === k.correct_label);
    assert.ok(opt, `${id}: label ${k.correct_label} missing`);
    assert.equal(clean(opt.text).replace(/——.*$/, "").trim(), clean(k.correct_text).replace(/——.*$/, "").trim(), id);
    checked++;
  }
  assert.equal(checked, 1858);
  for (const q of articleItems) {
    const { article_id, ...rest } = q;
    const { article_id: _ignored, ...main } = items.get(q.challenge_id);
    assert.deepEqual(rest, main, "article copy differs: " + q.challenge_id);
  }
});

test("reviewed fixes are served and textbook rules hold", { skip: !haveRuntime }, () => {
  const { items, keys, manifest } = loadBank(runtimeDir, path.join(root, "src", "generated", "answer_keys.json"));
  const fixes = readJson(path.join(root, "data", "manual", "v3", "bank_fixes_20261104.json")).items.filter((f) => f.decision === "apply");
  assert.equal(fixes.length, 438);
  assert.deepEqual(manifest.bank_fixes.sources, ["bank_fixes_20261104.json"]);
  for (const f of fixes) {
    const q = items.get(f.id), k = keys[f.id];
    if (f.stem) assert.equal(q.stem, f.stem, f.id);
    for (const [label, text] of Object.entries(f.options || {})) assert.equal(q.options.find((o) => o.label === label).text, text, f.id);
    if (f.correct_label) assert.equal(k.correct_label, f.correct_label, f.id);
    if (f.explanation) assert.equal(k.explanation, f.explanation, f.id);
    assert.equal(new Set(q.options.map((o) => clean(o.text))).size, 4, "duplicate options " + f.id);
  }
  for (const [id, k] of Object.entries(keys)) {
    if (k.source_type !== "textbook") continue;
    assert.ok(!k.explanation.includes("同词课文参照："), id);
    const ref = k.textbook_support[0];
    const targets = new Set([ref.headword, String(ref.label_text || "").replace(/[（(].*?[)）]/g, "").trim(), k.term_id.split("::")[1]]);
    for (const d of k.dict_support || []) assert.ok(targets.has(d.headword), `${id}: dict entry ${d.headword}`);
  }
  for (const q of items.values()) if (q.source_kind === "textbook") assert.ok(!q.sentence.startsWith("”"), "leading closing quote " + q.challenge_id);
  for (const k of Object.values(keys)) assert.ok(!k.explanation.includes("所在句为“”"), k.challenge_id);
  const served = JSON.stringify([...items.values()]);
  for (const lost of ["奏刀然", "而况大乎", "而骏良不实外厩", ""]) assert.ok(!served.includes(lost), "unrepaired text " + JSON.stringify(lost));
  // the four reported examples
  const yu = keys["function-textbook-function-与-高中-语文-普通高中教科书-语文必修-下册-子路-曾皙-冉有-公西华侍坐-39-v1"];
  assert.match(yu.correct_text, /句末/);
});

test("exam items and unreviewed options are unchanged", { skip: !haveRuntime || !process.env.BASELINE_RUNTIME_DIR }, () => {
  const base = process.env.BASELINE_RUNTIME_DIR;
  const before = loadBank(base, path.join(base, "answer_keys.json"));
  const after = loadBank(runtimeDir, path.join(root, "src", "generated", "answer_keys.json"));
  const fixed = new Set(readJson(path.join(root, "data", "manual", "v3", "bank_fixes_20261104.json")).items.filter((f) => f.decision === "apply").map((f) => f.id));
  assert.deepEqual([...after.items.keys()].sort(), [...before.items.keys()].sort());
  let examSame = 0;
  for (const [id, q] of before.items) {
    const a = after.items.get(id);
    if (q.source_kind === "exam") {
      assert.deepEqual(a, q, "exam item changed " + id);
      assert.deepEqual(after.keys[id], before.keys[id], "exam key changed " + id);
      examSame++;
    } else if (!fixed.has(id)) {
      assert.deepEqual(a.options.map((o) => o.text), q.options.map((o) => o.text), "unreviewed options changed " + id);
      assert.equal(after.keys[id].correct_label, before.keys[id].correct_label, id);
    }
  }
  assert.equal(examSame, 64);
});
