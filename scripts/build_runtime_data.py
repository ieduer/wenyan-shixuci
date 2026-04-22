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
    return int(build_runtime_main() or 0)


if __name__ == "__main__":
    raise SystemExit(main())
