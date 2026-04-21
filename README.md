# 文言實虛詞

獨立 Cloudflare Worker 項目，面向北京高考文言文實詞/虛詞訓練。

## 倉庫邊界

- 本倉只提交代碼、schema、構建腳本、衍生索引與小樣例
- 不提交教材 PDF、真題原始庫、`dict_moe_revised.db`、`dict_moe_idioms.db` 等原始工件
- 正式運行時數據由本機 `scripts/build_runtime_data.py` 從既有本地資源構建
- 公開站只展示必要短句、必要摘錄、映射與來源引用；不公開分發教材原文、真題全文或辭典長段原文
- 官方原文只在本機私有構建與授權環境讀取；公開站不承擔全文展示

## License 分層

- 代碼：MIT
- 倉庫內樣例與衍生索引：`LICENSE-DATA.md`
- 上游教材、真題、辭典資料：不隨倉庫分發，按各自來源授權處理

## 本地來源

- `/Users/ylsuen/textbook_ai_migration/platform/backend/textbook_classics_manifest.json`
- `/Users/ylsuen/textbook_ai_migration/export/notebooklm/初中_语文.md`
- `/Users/ylsuen/textbook_ai_migration/export/notebooklm/高中_语文.md`
- `/Users/ylsuen/textbook_ai_migration/data/index/dict_exam_xuci.json`
- `/Users/ylsuen/textbook_ai_migration/data/index/dict_exam_shici.json`
- `/Users/ylsuen/textbook_ai_migration/data/index/dict_moe_revised.db`
- `/Users/ylsuen/textbook_ai_migration/data/index/dict_moe_idioms.db`

## 命令

```bash
npm install
cp .dev.vars.example .dev.vars
npm run check:sources
npm run build:data
npm run check
npm run dev
```

`npm run check:sources` 會硬失敗於來源缺失、SQLite 無法打開、北京卷年份覆蓋不足、教材對齊異常等問題，避免靜默生成殘缺 JSON。

## 第一版範圍

- 本機構建 `terms_function.json`、`terms_content.json`、`exam_questions.json`、`textbook_examples.json`、`dict_links.json`
- Worker 啟動後支持匿名熱身、實詞/虛詞挑戰、即時反饋、錯題追擊、單次 `report.md` / `report.json`
- 已登入用戶在報告下載與挑戰結束時同步 `progress`、`recordDownload`、`mastery report`
- 週報月報、班級榜、自由文本簡答評分、PDF 報告暫不在第一版

## 部署

```bash
npx wrangler whoami
npm run db:migrate:remote
npm run deploy
```

部署前需要確認：

- `bdfz.net` 已在當前 Cloudflare account 下管理
- `wy.bdfz.net` 未被其他 Worker Route、Pages 或 DNS 記錄佔用
- `my.bdfz.net` 的登入回跳白名單已包含 `https://wy.bdfz.net`
- `wrangler.toml` 中的 D1 `database_id`、R2 bucket 綁定為真實值
- `SIGNING_SECRET`、如啟用則 `TURNSTILE_SECRET` 等 secret 已配置
- Worker Static Assets 單文件不得超過 `25 MiB`；超限知識庫資產必須分片或轉 R2
