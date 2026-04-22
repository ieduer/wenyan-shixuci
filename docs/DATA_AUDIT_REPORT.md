# DATA_AUDIT_REPORT

- Generated at: 2026-04-22T03:13:16.384248+00:00
- Source check ok: True
- Question docs: 52
- Beijing year coverage: 2002-2025

## Summary

- Pure function-word compare groups in raw data: 17
- Pure function-word compare groups with complete A-D options: 8
- Emitted function-word compare challenges: 5
- Content headword replacements applied: 3
- Content occurrences accepted into runtime: 225 / 244

## Xuci Audit

- Status counts: `{"emitted": 5, "missing_answer": 7, "incomplete_option_set": 5}`

### Emitted Examples

- 2002 q9 xuci_compare_same: answer=B labels=A,B,C,D
- 2004 q10 xuci_compare_same: answer=D labels=A,B,C,D
- 2013 q7 xuci_compare_same: answer=D labels=A,B,C,D
- 2017 q10 xuci_compare_diff: answer=C labels=A,B,C,D
- 2021 q7 xuci_compare_same: answer=B labels=A,B,C,D

### Blocked Examples

- 2003 q13 xuci_compare_same: status=missing_answer labels=A,B,C,D answer=missing
- 2005 q7 xuci_compare_same: status=incomplete_option_set labels=B,C answer=A
- 2010 q7 xuci_compare_same: status=incomplete_option_set labels=A,B,C answer=A
- 2011 q7 xuci_compare_same: status=incomplete_option_set labels=A,B answer=D
- 2012 q7 xuci_compare_same: status=missing_answer labels=B,C,D answer=missing
- 2015 q10 xuci_compare_diff: status=missing_answer labels=C,D answer=missing
- 2016 q10 xuci_compare_diff: status=missing_answer labels=A,B,D answer=missing
- 2018 q9 xuci_compare_diff: status=incomplete_option_set labels=C,D answer=C

## Content Audit

- normalized `之衣柜籍` -> `籍`
- normalized `以为贺预` -> `预`
- normalized `典禁旅典` -> `典`

- Rejected occurrence counts: `{"invalid_context": 15, "missing_gloss": 1, "invalid_gloss": 3}`
- Remaining non-simple runtime headwords: `[{"headword": "好生之德", "needs_manual_review": false, "dict_ref_count": 1}]`

### Salvaged Polluted Examples

- `使` beijing-2014-None q9: raw=`使堤土石幸久不朽 使:假如` -> sanitized=`使堤土石幸久不朽`
- `使` beijing-2021-None q6: raw=`使礼义废,纲纪败 使:假如` -> sanitized=`使礼义废,纲纪败`
- `什` beijing-2016-None q9: raw=`什至而金千斤也 什:十倍` -> sanitized=`什至而金千斤也`
- `典` beijing-2013-None q6: raw=`太祖典禁旅典:主管,掌管` -> sanitized=`初,太祖典禁旅,彬中立不倚,非风湿未尝造门,群居燕会,亦所罕预,由是器重焉,建隆二年,自平阳召归,胃曰:“我畸昔常欲新汝,汝何故疏我?`
- `几` beijing-2022-None q6: raw=`祸几及身 几:大多` -> sanitized=`秦王置天下于法令刑罚,德泽亡一有,而怨毒盈于世,下憎恶之如仇雠,祸几及身,子孙诛绝,此天下之所共见也。`
- `切` beijing-2023-None q6: raw=`则取勇猛能操切百姓者操 切:胁迫` -> sanitized=`则取勇猛能操切百姓者操`

## Runtime Validity

- Challenge counts: `{"xuci_pair_compare": 5, "content_gloss": 216, "translation_keypoint": 216, "sentence_meaning": 216, "passage_meaning": 170, "analysis_short": 216}`
- Issue counts: `{}`

## Architecture Reflection

- 北京卷四选项八短句题不能假设所有原始 excerpt 都带完整句号；应优先按加点词标记位置切分，再用标点作回退。
- 北京卷比较题不能假设 question_docs.answer 一定存在。答案字段为空时，题面可保存，但不可进入可答题运行库。
- “虚词比较题”不能直接等同于“所有选项都来自 xuci 词表”。当前原始索引里存在同题缺失部分选项的情况，运行时必须把缺项题标成证据不足，而不是拼凑四项。
- 实词题的原始 excerpt 不能直接信任。带“词:释义”“翻译为”“参考答案”等污染标记的 excerpt 必须先经语境回收，再决定是否可出题。
- 实词词头也不能直接信任。像“之衣柜籍”“以为贺预”“典禁旅典”这类污染词头需要在构建期规范化，否则会污染词表、词典映射和错题追踪。
- 本次真实审计后，纯功能词比较题从 5 / 17 可运行，未运行题主要由缺答案或缺完整选项引起。
- 实词侧共有 3 个污染词头被规范化，运行时仍保留的长词头仅剩 1 个，其中应视为合法词组单独处理。
