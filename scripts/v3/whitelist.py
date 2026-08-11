"""
v3 词条白名单 / 黑名单 — 控制哪些字头可以进入题库。

设计目标：
- 实词题库：只接受高考真题考过的字头（dict_exam_shici）+ 教材注释中确属高考实词大纲的字头。
- 虚词题库：以《考试说明》18 虚词为基准 ∪ 真题已考虚词词典 dict_exam_xuci。
- 黑名单：地名、人名、官职、谥号、年号、干支、纯典故标签 — 在 v2 中被错误地当作实词的全部踢除。
- 切分黑名单：明显切错的串（"子哂"、"夫子哂"、"何伤"、"比及" 等不是合规词头的）一律拒绝。

不依赖 sqlite，只读上游已构建好的 JSON。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

SOURCE_ROOT = Path("/Users/ylsuen/textbook_ai_migration")
SHICI_PATH = SOURCE_ROOT / "data" / "index" / "dict_exam_shici.json"
XUCI_PATH = SOURCE_ROOT / "data" / "index" / "dict_exam_xuci.json"
XUCI_DETAILS_PATH = SOURCE_ROOT / "data" / "index" / "dict_exam_xuci_details.json"


# 高考考试说明 18 虚词（人教 / 部编通行清单）
EXAM_OUTLINE_FUNCTION_WORDS = {
    "而", "何", "乎", "乃", "其", "且", "若", "所", "为",
    "焉", "也", "以", "因", "于", "与", "则", "者", "之",
}

# 真题中已经反复考过、虽然不在 18 虚词清单里也保留的虚词
EXTRA_EXAM_FUNCTION_WORDS = {
    "或", "遂", "盖", "既", "虽", "然",
}

# v3 虚词题库白名单
FUNCTION_WHITELIST = EXAM_OUTLINE_FUNCTION_WORDS | EXTRA_EXAM_FUNCTION_WORDS

# 实词条目硬黑名单 — 这些不论上下文如何都不出题
# 涵盖：地名、人名、官职、谥号、年号、干支、神名、典故指代专名
HEADWORD_BLACKLIST_EXACT = {
    # 地名 / 国名
    "三吴", "三国", "三齐", "九州", "九土", "九国", "中国", "东宫", "东海", "东曦",
    "京华", "临汝", "丹", "丹田", "兜鍪", "佛狸", "保宫", "汾", "汾水", "齐",
    "鲁", "魏", "楚", "赵", "韩", "燕", "代", "秦", "卫", "晋",
    # 人名 / 字号
    "亚父", "丈人", "丁令", "孔子", "墨子", "孙叔敖", "晏子", "管子", "桓公", "齐桓",
    "文王", "汤", "武王", "尧", "舜", "禹", "桀", "纣", "段干木", "魏文",
    "贾谊", "贡禹", "顾炎武", "韩非", "李斯", "刘邦", "高帝", "武帝", "汉元帝",
    "景公", "庄公", "崔杼", "庆封", "邴原", "辛公义", "李疑", "范景淳", "曹彬",
    "曹子", "管仲", "张子房", "汉高帝", "白徒",
    # 官职 / 谥号 / 称号
    "相", "刺史", "太守", "丞相", "御史", "廷尉", "谒者", "都尉", "司马", "中郎将",
    "将军", "诸生", "高帝", "侯", "君", "公", "王", "皇帝", "天子", "大夫", "卿",
    # 年号 / 干支
    "乙巳", "周天和", "建隆", "庆历", "汉二年", "汉五年", "汉七年", "咸平", "贞观",
    "建安", "天宝", "开元",
    # 神名 / 典故指代
    "丘冢", "佳期", "保宫", "丘", "兜鍪",
    # 单纯短语 / 双字片段（不是规范词头）
    "比及", "何伤", "何之", "何以", "何则", "何为", "何如", "余以", "余里", "余杯",
    "子哂", "夫子哂", "子家", "云者", "会因", "会须", "会不", "且夫", "至于",
    "于是", "若夫", "苟且", "云尔", "云云", "若此", "如此", "如是", "至此",
    "其余", "其它", "其他", "其实", "其中", "亦曰", "曰诸", "兹复", "之于",
    "予人", "予我", "汝等", "彼等",
    # 礼器、年号性质名物
    "千乘", "万乘", "九鼎", "六合", "七庙", "八荒",
}

# 实词条目模糊黑名单（headword 含以下子串即剔除）
HEADWORD_BLACKLIST_PATTERNS = (
    "氏", "公", "侯", "君",  # 称谓尾字
)

# 不进入实词题的两字词组判定 — 必须看着像合成词，不是临时短语
# 凡两字词头同时满足"不在真题字典 + 不在教材高频实词"，被拒绝
SAFE_TWO_CHAR_CONTENT = {
    # 真题已考的两字实词 / 古今异义双字
    "操切", "相与", "供秩", "徜徉", "恭谨", "咨怨", "泉壤", "游幸", "不食", "伏阙",
    "力争", "民业", "规过", "欺负", "过法", "鄙儒", "丈夫",  # 高考曾考的双字
    # 教材中高频且确属合成词的双字
    "婆娑", "彷徨", "踟蹰", "蹒跚", "踯躅", "造化", "造次", "踌躇", "惆怅",
    "凄怆", "怅惘", "恍惚", "惊愕", "慷慨", "彳亍",
}


def _load_json(path: Path):
    with path.open() as f:
        return json.load(f)


def load_real_exam_shici() -> dict[str, dict]:
    """返回 dict_exam_shici 中的全部字头（已被真题考过）。"""
    raw = _load_json(SHICI_PATH)
    out: dict[str, dict] = {}
    for term in raw.get("terms", []):
        head = (term.get("display_headword") or term.get("headword") or "").strip()
        if not head:
            continue
        out[head] = term
    return out


def load_real_exam_xuci() -> dict[str, dict]:
    raw = _load_json(XUCI_PATH)
    out: dict[str, dict] = {}
    for term in raw.get("terms", []):
        head = (term.get("display_headword") or term.get("headword") or "").strip()
        if not head:
            continue
        out[head] = term
    return out


def load_xuci_details() -> dict[str, dict]:
    raw = _load_json(XUCI_DETAILS_PATH)
    return raw.get("terms", {}) or {}


def is_acceptable_content_headword(headword: str, real_exam_shici: dict) -> tuple[bool, str]:
    """返回 (可入库, 拒绝理由)。"""
    h = (headword or "").strip()
    if not h:
        return False, "empty_headword"
    if h in HEADWORD_BLACKLIST_EXACT:
        return False, "blacklist_exact"
    if any(p in h for p in HEADWORD_BLACKLIST_PATTERNS):
        # 但不要误伤"君子"、"公义"、"侯王"等正常实词；只拒纯尾字标签
        if len(h) <= 2:
            return False, "blacklist_pattern"
    n = len(h)
    if n == 1:
        # 单字实词不再额外卡白名单；交由真题/教材证据决定
        return True, ""
    if n == 2:
        if h in real_exam_shici:
            return True, ""
        if h in SAFE_TWO_CHAR_CONTENT:
            return True, ""
        return False, "two_char_not_in_whitelist"
    # 三字及以上一律拒绝（古汉语单/双音节词为主，三字几乎全是短语）
    return False, "too_long"


def is_acceptable_function_headword(headword: str) -> tuple[bool, str]:
    h = (headword or "").strip()
    if not h:
        return False, "empty_headword"
    if len(h) != 1:
        return False, "function_word_must_be_single_char"
    if h in FUNCTION_WHITELIST:
        return True, ""
    return False, "not_in_function_whitelist"


def filter_content_terms(terms: Iterable[dict], real_exam_shici: dict) -> tuple[list[dict], list[tuple[str, str]]]:
    """对 terms_content 的清单做过滤；返回 (kept, dropped[(headword, reason)])。"""
    kept: list[dict] = []
    dropped: list[tuple[str, str]] = []
    for term in terms:
        head = (term.get("display_headword") or term.get("headword") or "").strip()
        ok, reason = is_acceptable_content_headword(head, real_exam_shici)
        if ok:
            kept.append(term)
        else:
            dropped.append((head, reason))
    return kept, dropped


def filter_function_terms(terms: Iterable[dict]) -> tuple[list[dict], list[tuple[str, str]]]:
    kept: list[dict] = []
    dropped: list[tuple[str, str]] = []
    for term in terms:
        head = (term.get("display_headword") or term.get("headword") or "").strip()
        ok, reason = is_acceptable_function_headword(head)
        if ok:
            kept.append(term)
        else:
            dropped.append((head, reason))
    return kept, dropped


if __name__ == "__main__":
    shici = load_real_exam_shici()
    xuci = load_real_exam_xuci()
    print(f"上游真题实词字头 {len(shici)} 个")
    print(f"上游真题虚词字头 {len(xuci)} 个")
    print(f"v3 虚词白名单 {len(FUNCTION_WHITELIST)} 个: {sorted(FUNCTION_WHITELIST)}")
    print(f"实词硬黑名单 {len(HEADWORD_BLACKLIST_EXACT)} 个")
    print(f"双字合成词白名单 {len(SAFE_TWO_CHAR_CONTENT)} 个")
