"""Markdown blocks rendered from the literature registry.

Used by the dashboard "literature library / framework / review" pages
and by the per-track research-summary preambles. Each function returns a
plain markdown string ready for direct rendering.
"""

from __future__ import annotations

from ._data import CAMP_LABELS, TRACK_LABELS
from ._frames import build_literature_catalog_frame


def build_literature_summary_markdown() -> str:
    counts = build_literature_catalog_frame()["stance"].value_counts()
    camp_counts = build_literature_catalog_frame()["camp"].value_counts()
    lines = [
        "# 16 篇指数效应文献库",
        "",
        "这套文献库现在同时保留两种组织方式：",
        "- `立场组织`：反方 / 中性 / 正方，适合写传统文献综述。",
        "- `演进组织`：五大阵营，适合讲研究史、方法升级和策略框架。",
        "",
        f"- 反方文献：{int(counts.get('反方', 0))} 篇",
        f"- 中性文献：{int(counts.get('中性', 0))} 篇",
        f"- 正方文献：{int(counts.get('正方', 0))} 篇",
        "",
        "五大阵营：",
        f"- 创世之战：{int(camp_counts.get('创世之战', 0))} 篇",
        f"- 正方深化：{int(camp_counts.get('正方深化', 0))} 篇",
        f"- 市场摩擦与效应重估：{int(camp_counts.get('市场摩擦与效应重估', 0))} 篇",
        f"- 方法革命：{int(camp_counts.get('方法革命', 0))} 篇",
        f"- 中国 A 股主战场：{int(camp_counts.get('中国A股主战场', 0))} 篇",
        "",
        "页面中已可直接查看每篇文献的立场、阵营、核心逻辑、识别对象、挑战的假设与研究中的作用。",
    ]
    return "\n".join(lines)


def build_project_track_markdown(project_module: str) -> str:
    config = TRACK_LABELS[project_module]
    catalog = build_literature_catalog_frame()
    track = catalog.loc[catalog["project_module"] == project_module].copy()
    stance_counts = track["stance"].value_counts().to_dict()
    camp_counts = track["camp"].value_counts().to_dict()
    camp_text = "、".join(
        f"{camp} {count} 篇"
        for camp, count in sorted(
            camp_counts.items(), key=lambda item: CAMP_LABELS[item[0]]["order"]
        )
    )
    lines = [
        f"# {config['title']}",
        "",
        config["description"],
        "",
        "这条研究主线并不是只依赖某一篇论文，而是从 16 篇文献中抽取相同问题意识后组织出来的。",
        "",
        f"- 对应文献数：{len(track)} 篇",
        f"- 反方：{int(stance_counts.get('反方', 0))} 篇",
        f"- 中性：{int(stance_counts.get('中性', 0))} 篇",
        f"- 正方：{int(stance_counts.get('正方', 0))} 篇",
        f"- 涵盖阵营：{camp_text or '暂无'}",
        "",
        "推荐阅读方式：先看这条主线的支撑文献，再看下方事件研究、回归或 RDD 输出。",
    ]
    return "\n".join(lines)


def build_literature_framework_markdown() -> str:
    lines = [
        "# 指数效应研究的五大阵营",
        "",
        "这 16 篇文献现在不只是一份 PDF 清单，而是一条完整的研究史和策略框架。",
        "",
        "这一框架可按以下顺序展开：",
        "1. 先用 `创世之战` 立起争论：指数纳入后的上涨，到底是永久重估还是短期价格压力？",
        "2. 再用 `正方深化` 解释为什么效应可能部分保留：需求曲线、权重冲击、信息背书都能提供证据。",
        "3. 接着用 `市场摩擦与效应重估` 说明为什么偏差没有瞬间消失，以及为什么在现代美股里更像是公开 alpha 被压缩，而不是机制彻底消失。",
        "4. 然后用 `方法革命` 说明为什么今天必须把 RDD、样本选择和价格发现一起讨论，而不能只靠普通 CAR 争论。",
        "5. 最后落到 `中国 A 股主战场`：它更像一个制度摩擦更强的独立场景，而不是美股结论的简单复制。",
        "",
        "这一框架可进一步概括为以下三点：",
        "- 美股公开 alpha 已经被吃薄，但短期冲击、隐性换手成本与价格发现问题并未消失。",
        "- 同样的指数效应会因指数制度、套利容量和识别设计不同而呈现不同结果。",
        "- 中国市场不是美股的放大镜，而是一个需要单独处理制度摩擦与投资者结构的场景。",
        "- 三条研究主线对应了这条文献演进链在实证层面的落地。",
    ]
    return "\n".join(lines)


def build_literature_review_markdown() -> str:
    return "\n".join(
        [
            "# 文献综述导航页",
            "",
            "这个页面把 16 篇指数效应文献按 `反方 / 中性 / 正方` 三组拆开，便于组织传统文献综述。",
            "真正需要解释的不是谁简单支持、谁简单反对，而是制度背景、套利能力和价格发现条件为什么会改写结论。",
            "",
            "若需展示研究史、方法演进或策略脉络，可配合“五大阵营”页面一并阅读。",
            "",
            "建议阅读顺序：",
            "1. 先看反方：建立“指数效应并非无争议事实”的问题意识。",
            "2. 再看中性：把争论推进到“制度背景、识别策略与价格发现”层面。",
            "3. 最后看正方：为需求曲线、被动资金冲击和价格效应机制提供理论与实证支撑。",
            "",
            "建议写法：",
            "- 第一段写反方：强调短期价格压力、效应减弱或消失。",
            "- 第二段写中性：强调不同指数制度、识别方法和价格发现条件的重要性。",
            "- 第三段写正方：强调需求曲线向下倾斜、股票非完全替代、被动资金冲击与部分长期保留。",
            "",
            "该页面可与 `docs/literature_review_author_year_cn.md` 配合使用，以形成更完整的文献综述正文。",
        ]
    )
