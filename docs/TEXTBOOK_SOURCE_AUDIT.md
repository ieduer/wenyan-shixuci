# 高中语文教材源数据抽取审计

## 结论
- 教材册数：5
- 文言篇目数：69
- 注释总数：2297
- 未精确定位注释数：0
- 正文来源分布：{"forum_raw": 69}

## 文件样貌
- 论坛 raw 缓存：`/Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/forum_textbook_topics_raw.json`，来自 `forum-backend` 上 Discourse 数据库 `posts.raw`。
- 每册目录下都存在单册 `*.md`、`*_middle.json`、`*_content_list.json`、`*_origin.pdf`、`images/*.jpg`。
- 单册 `*.md` / `middle.json` 保留作页内锚点、标签对位与兜底校验，不再作为正文权威源。
- `*_middle.json` / `*_content_list.json` 提供 `page_idx`、`bbox`、block type，适合做页内精确定位。
- `export/notebooklm/高中_语文.md` 只适合汇总阅读，不适合作为精确映射底座。

## 推荐抽取规则
- 正文：优先使用 `forum-backend` 中教材主题首帖 `raw` 的文言文正文。
- 注释：优先使用论坛首帖 `raw` 内脚注；本机 MinerU 注释块用于脚注顺序对齐、标签补全和页内锚点。
- 对位：`source_sentence/context_window` 以论坛 raw 的脚注标记定位；`source_page_idx/source_block_index` 保留教材页块锚点。
- 低质量条目：如果论坛正文脚注无法稳定定位，或本机锚点无法补齐，则进入 `textbook_note_unresolved_table`，不混入主表。

## 分册统计
- 普通高中教科书·语文必修 上册（人教版）：篇目 18，校对正文 18，注释 418，未定位 0
- 普通高中教科书·语文必修 下册（人教版）：篇目 16，校对正文 16，注释 667，未定位 0
- 普通高中教科书·语文选择性必修 上册（人教版）：篇目 10，校对正文 10，注释 164，未定位 0
- 普通高中教科书·语文选择性必修 下册（人教版）：篇目 17，校对正文 17，注释 637，未定位 0
- 普通高中教科书·语文选择性必修 中册（人教版）：篇目 8，校对正文 8，注释 411，未定位 0

## 匹配模式统计
- {"forum_raw_marker": 2242, "direct_probe": 55}

## 主要产物
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_article_master_table.json
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_master_table.json
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_unresolved_table.json
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_article_master_table.csv
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_master_table.csv
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_unresolved_table.csv
- /Users/ylsuen/CF/wenyan-shixuci/docs/TEXTBOOK_SOURCE_AUDIT.json
