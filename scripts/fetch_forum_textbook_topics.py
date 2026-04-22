#!/Users/ylsuen/.venv/bin/python
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone

from check_sources import REPO_ROOT


OUTPUT_PATH = REPO_ROOT / "data" / "runtime_private" / "forum_textbook_topics_raw.json"
CATEGORY_IDS = (11, 12, 13, 17, 18)
REMOTE_COMMAND = "docker exec -i app su postgres -c 'psql discourse -At'"
SQL = f"""
select json_build_object(
  'category_id', c.id,
  'category_name', c.name,
  'topic_id', t.id,
  'topic_title', t.title,
  'topic_slug', t.slug,
  'post_id', p.id,
  'post_number', p.post_number,
  'raw', p.raw
)::text
from topics t
join posts p on p.topic_id = t.id
join categories c on c.id = t.category_id
where t.deleted_at is null
  and p.deleted_at is null
  and p.post_type = 1
  and t.category_id in ({", ".join(str(item) for item in CATEGORY_IDS)})
order by t.category_id, t.id, p.post_number;
"""


def main() -> int:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["ssh", "forum-backend", REMOTE_COMMAND],
        input=SQL,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or f"forum export failed with code {result.returncode}")

    posts: list[dict[str, object]] = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        posts.append(json.loads(stripped))

    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "forum-backend:discourse.posts.raw",
        "category_ids": list(CATEGORY_IDS),
        "post_count": len(posts),
        "topic_count": len({str(item.get("topic_id") or "") for item in posts}),
        "posts": posts,
        "topics": posts,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "topic_count": len({str(item.get("topic_id") or "") for item in posts}),
                "post_count": len(posts),
                "output": str(OUTPUT_PATH),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
