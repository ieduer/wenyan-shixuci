#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

from runtime_generation_v2 import *  # noqa: F401,F403
from build_textbook_source_tables import main as build_textbook_source_tables_main
from fetch_forum_textbook_topics import main as fetch_forum_textbook_topics_main
from runtime_generation_v2 import main as build_runtime_main


def main() -> int:
    forum_status = fetch_forum_textbook_topics_main()
    if int(forum_status or 0) != 0:
        return int(forum_status or 1)
    source_status = build_textbook_source_tables_main()
    if int(source_status or 0) != 0:
        return int(source_status or 1)
    rc = int(build_runtime_main() or 0)
    if rc != 0:
        return rc
    # v3 后处理：白名单过滤 + 北京卷 v3 真题答案与解析
    import importlib, sys, pathlib
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    v3_dir = repo_root / "scripts" / "v3"
    sys.path.insert(0, str(v3_dir))
    v3_build = importlib.import_module("build_v3")
    v3_audit = importlib.import_module("audit_v3")
    rc = int(v3_build.main() or 0)
    if rc != 0:
        return rc
    # reviewed per-item fixes + dictionary/same-term rules (data/manual/v3/bank_fixes_*.json)
    rc = int(importlib.import_module("apply_bank_fixes").main() or 0)
    if rc != 0:
        return rc
    return int(v3_audit.main() or 0)


if __name__ == "__main__":
    raise SystemExit(main())
