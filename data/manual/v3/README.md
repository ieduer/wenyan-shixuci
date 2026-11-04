# 北京卷字词题 v3 答案与解析（人手撰写）

本目录是 v3 重做后的真题字词题（实词 / 虚词 / 古今义 / 句意）的权威答案与解析层。
- 每年一个文件 `exam_solutions_<year>.json`
- 每条对应一个独立 challenge
- `verification: "manual_authored_v3"` 表示该条答案与解析均由我直接撰写，不再依赖 v2 pipeline 推断
- pipeline 读 `data/manual/v3/exam_solutions_*.json` 时按 challenge_id 匹配，覆写所有 v2 推断结果

## 题型代码

- `shici_explanation` —— 加点实词解释，找误释项 / 正确项
- `shici_modern_diff` —— 加点词与现代汉语意义比较（古今异义）
- `xuci_pair_compare` —— 两组虚词比较意义和用法是否相同
- `xuci_explanation` —— 单组虚词意义、用法解释
- `sentence_meaning` —— 文中语句的理解 / 翻译辨析

## 解析体例（四段式）

每条 `explanation` 字段固定包含：

1. **题源与考点**：年份、卷别、原句、考查的字头与设题方向
2. **正解证据链**：正答字头的本义、词性、在原句中的句法位置与上下文支撑
3. **三个干扰项逐条驳斥**：每个干扰项是哪个字头被怎样误释 / 错配，放回原句为什么读不通
4. **辞典参照**：《重編國語辭典》《古代汉语虚词词典》或《考试说明》虚词表的简要引证

`option_analyses[i].analysis` 为每个选项独立写一段，不复用模板套话。
