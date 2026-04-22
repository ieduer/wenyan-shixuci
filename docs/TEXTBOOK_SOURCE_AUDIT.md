# 高中语文教材源数据抽取审计

## 结论
- 教材册数：5
- 文言篇目数：69
- 注释总数：1401
- 未精确定位注释数：79
- 正文来源分布：{"mineru_md": 11, "legacy_structured_text": 58}

## 文件样貌
- 每册目录下都存在单册 `*.md`、`*_middle.json`、`*_content_list.json`、`*_origin.pdf`、`images/*.jpg`。
- 单册 `*.md` 适合抽线性注释和 OCR 正文兜底。
- `*_middle.json` / `*_content_list.json` 提供 `page_idx`、`bbox`、block type，适合做页内精确定位。
- `export/notebooklm/高中_语文.md` 只适合汇总阅读，不适合作为精确映射底座。

## 推荐抽取规则
- 正文：优先使用本机 `jks/_legacy/yuwen/public/data/*.json` 中结构化的 `main_text` 全文；缺失篇目再退回单册 MinerU OCR 正文。
- 注释：继续以单册 MinerU md / `middle.json` 注释块为权威注释文本。
- 对位：正文命中与页内定位分开保存。`source_sentence/context_window` 走校对正文；`source_page_idx/source_block_index` 保留教材页块锚点。
- 低质量条目：如果校对正文无法精确命中注释标签，则进入 `textbook_note_unresolved_table`，不混入主表。

## 分册统计
- 普通高中教科书·语文必修 上册（人教版）：篇目 18，校对正文 16，注释 251，未定位 24
- 普通高中教科书·语文必修 下册（人教版）：篇目 16，校对正文 16，注释 442，未定位 15
- 普通高中教科书·语文选择性必修 上册（人教版）：篇目 10，校对正文 9，注释 57，未定位 1
- 普通高中教科书·语文选择性必修 下册（人教版）：篇目 17，校对正文 17，注释 374，未定位 28
- 普通高中教科书·语文选择性必修 中册（人教版）：篇目 8，校对正文 0，注释 277，未定位 11

## 匹配模式统计
- {"fallback_probe": 75, "direct_probe": 1048, "exact_label": 275, "headword_fallback": 3}

## 主要产物
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_article_master_table.json
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_master_table.json
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_unresolved_table.json
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_article_master_table.csv
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_master_table.csv
- /Users/ylsuen/CF/wenyan-shixuci/data/runtime_private/textbook_note_unresolved_table.csv
- /Users/ylsuen/CF/wenyan-shixuci/docs/TEXTBOOK_SOURCE_AUDIT.json
