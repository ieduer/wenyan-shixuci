#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

from runtime_generation_v2 import *  # noqa: F401,F403
from build_textbook_source_tables import main as build_textbook_source_tables_main
from runtime_generation_v2 import main as build_runtime_main


def main() -> int:
    source_status = build_textbook_source_tables_main()
    if int(source_status or 0) != 0:
        return int(source_status or 1)
    return int(build_runtime_main() or 0)


if __name__ == "__main__":
    raise SystemExit(main())
