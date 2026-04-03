from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

PDF_ROOT = Path("/Users/leonardodon/Documents/paper/index_effect_pdfs")

TRACK_LABELS = {
    "短期价格压力": {
        "title": "短期价格压力与效应减弱",
        "subtitle": "Price Pressure & Disappearing Effect",
        "description": "用反方文献和早期事件研究文献检验短窗口价格冲击、成交量放大和效应减弱问题。",
    },
    "需求曲线效应": {
        "title": "需求曲线与长期保留",
        "subtitle": "Demand Curves & Long-run Retention",
        "description": "用正方和机制文献检验股票需求曲线是否向下倾斜，以及价格效应是否只部分回吐。",
    },
    "沪深300论文复现": {
        "title": "制度识别与中国市场证据",
        "subtitle": "Identification & China Evidence",
        "description": "用中性和中国市场文献组织识别策略、对照组设计与断点回归扩展证据。",
    },
}


@dataclass(frozen=True)
class LiteraturePaper:
    paper_id: str
    stance: str
    title: str
    authors: str
    year_label: str
    market_focus: str
    method_focus: str
    project_module: str
    relevance_note: str
    pdf_path: Path

    @property
    def exists(self) -> bool:
        return self.pdf_path.exists()


PAPER_LIBRARY: tuple[LiteraturePaper, ...] = (
    LiteraturePaper(
        paper_id="yao_zhou_chen_2022",
        stance="反方",
        title="Price effects in the Chinese stock market: Evidence from the China securities index (CSI300) based on regression discontinuity",
        authors="Dongmin Yao; Shiyu Zhou; Yijing Chen",
        year_label="2022",
        market_focus="中国 / CSI300",
        method_focus="断点回归, DID",
        project_module="沪深300论文复现",
        relevance_note="适合放在“指数效应是否稳健存在仍有争议”的反方补充位置。",
        pdf_path=PDF_ROOT / "con" / "1-s2.0-S1544612321004244-main.pdf",
    ),
    LiteraturePaper(
        paper_id="harris_gurel_1986",
        stance="反方",
        title="Price and Volume Effects Associated with Changes in the S&P 500 List: New Evidence for the Existence of Price Pressures",
        authors="Lawrence Harris; Eitan Gurel",
        year_label="1986",
        market_focus="美国 / S&P 500",
        method_focus="事件研究, 价格压力",
        project_module="短期价格压力",
        relevance_note="短期价格压力路径的核心经典文献。",
        pdf_path=PDF_ROOT / "con" / "1986_Harris_Gurel_Price_and_Volume_Effects_SP500.pdf",
    ),
    LiteraturePaper(
        paper_id="kasch_sarkar_2011",
        stance="反方",
        title="Is There an S&P 500 Index Effect?",
        authors="Maria Kasch; Asani Sarkar",
        year_label="2011",
        market_focus="美国 / S&P 500",
        method_focus="指数效应再检验",
        project_module="短期价格压力",
        relevance_note="适合支撑“经典指数效应是否仍显著存在”的质疑。",
        pdf_path=PDF_ROOT / "con" / "2011_Kasch_Sarkar_Is_There_an_SP500_Index_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="coakley_et_al_2022",
        stance="反方",
        title="The S&P 500 Index inclusion effect: Evidence from the options market",
        authors="Jerry Coakley; George Dotsis; Apostolos Kourtis; Dimitris Psychoyios",
        year_label="2022",
        market_focus="美国 / S&P 500",
        method_focus="期权市场, 前瞻 beta",
        project_module="短期价格压力",
        relevance_note="更适合写进“现代市场中传统纳入效应减弱”的部分。",
        pdf_path=PDF_ROOT / "con" / "2022_Coakley_Saffi_Wang_SP500_Index_Inclusion_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="greenwood_sammon_2022",
        stance="反方",
        title="The Disappearing Index Effect",
        authors="Robin Greenwood; Marco Sammon",
        year_label="2022",
        market_focus="美国 / S&P 500",
        method_focus="长期时间趋势",
        project_module="短期价格压力",
        relevance_note="最适合放在“指数效应正在消失”这句的核心引用位置。",
        pdf_path=PDF_ROOT / "con" / "2022_Greenwood_Sammon_The_Disappearing_Index_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="petajisto_2011",
        stance="反方",
        title="The index premium and its hidden cost for index funds",
        authors="Antti Petajisto",
        year_label="2011",
        market_focus="美国 / S&P 500, Russell 2000",
        method_focus="指数溢价, 价格弹性",
        project_module="短期价格压力",
        relevance_note="适合连接被动投资、指数溢价与隐藏成本讨论。",
        pdf_path=PDF_ROOT / "con" / "Petajisto_2011_index_premium_hidden_cost.pdf",
    ),
    LiteraturePaper(
        paper_id="madhavan_2003",
        stance="中性",
        title="The Russell Reconstitution Effect",
        authors="Ananth Madhavan",
        year_label="2003",
        market_focus="美国 / Russell",
        method_focus="重构制度, 交易微观结构",
        project_module="沪深300论文复现",
        relevance_note="强调不同指数制度下效应与交易冲击的差异。",
        pdf_path=PDF_ROOT / "mid" / "2003_Madhavan_The_Russell_Reconstitution_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="ahn_patatoukas_2022",
        stance="中性",
        title="Identifying the Effect of Stock Indexing: Impetus or Impediment to Arbitrage and Price Discovery?",
        authors="Byung Hyun Ahn; Panos N. Patatoukas",
        year_label="2022",
        market_focus="美国",
        method_focus="识别策略, 套利, 价格发现",
        project_module="沪深300论文复现",
        relevance_note="适合支撑“识别设计比简单事件研究更重要”的方法论段落。",
        pdf_path=PDF_ROOT / "mid" / "2022_Ahn_Patatoukas_Identifying_the_Effect_of_Stock_Indexing.pdf",
    ),
    LiteraturePaper(
        paper_id="wurgler_zhuravskaya_2002",
        stance="中性",
        title="Does Arbitrage Flatten Demand Curves for Stocks?",
        authors="Jeffrey Wurgler; Ekaterina Zhuravskaya",
        year_label="2002",
        market_focus="跨市场 / 机制",
        method_focus="需求曲线, 套利, 市场分割",
        project_module="需求曲线效应",
        relevance_note="适合做机制桥梁，把事件效应和需求曲线讨论连起来。",
        pdf_path=PDF_ROOT / "mid" / "Wurgler_Zhuravskaya_2002_working_paper.pdf",
    ),
    LiteraturePaper(
        paper_id="chu_et_al_2021",
        stance="正方",
        title="Long-term impacts of index reconstitutions: Evidence from the CSI 300 additions and deletions",
        authors="Gang Chu; John W. Goodell; Xiao Li; Yongjie Zhang",
        year_label="2021",
        market_focus="中国 / CSI300",
        method_focus="长期效应, 调入调出",
        project_module="沪深300论文复现",
        relevance_note="适合补充中国市场长期指数重构影响的正向证据。",
        pdf_path=PDF_ROOT / "pro" / "1-s2.0-S0927538X2100158X-main.pdf",
    ),
    LiteraturePaper(
        paper_id="shleifer_1986",
        stance="正方",
        title="Do Demand Curves for Stocks Slope Down?",
        authors="Andrei Shleifer",
        year_label="1986",
        market_focus="美国 / S&P 500",
        method_focus="需求曲线, 非完全替代",
        project_module="需求曲线效应",
        relevance_note="需求曲线向下倾斜机制的理论核心文献。",
        pdf_path=PDF_ROOT / "pro" / "1986_Shleifer_Do_Demand_Curves_for_Stocks_Slope_Down.pdf",
    ),
    LiteraturePaper(
        paper_id="lynch_mendenhall_1997",
        stance="正方",
        title="New Evidence on Stock Price Effects Associated with Changes in the S&P 500 Index",
        authors="Anthony W. Lynch; Richard R. Mendenhall",
        year_label="1997",
        market_focus="美国 / S&P 500",
        method_focus="公告与生效分离, 事件研究",
        project_module="短期价格压力",
        relevance_note="适合放在早期经典正向实证证据部分。",
        pdf_path=PDF_ROOT / "pro" / "1997_Lynch_Mendenhall_New_Evidence_SP500_Index.pdf",
    ),
    LiteraturePaper(
        paper_id="kaul_mehrotra_morck_2000",
        stance="正方",
        title="Demand Curves for Stocks Do Slope Down: New Evidence from an Index Weights Adjustment",
        authors="Aditya Kaul; Vikas Mehrotra; Randall Morck",
        year_label="2000",
        market_focus="加拿大 / TSE 300",
        method_focus="指数权重调整, 需求曲线",
        project_module="需求曲线效应",
        relevance_note="与 Shleifer 一起支撑需求曲线向下倾斜。",
        pdf_path=PDF_ROOT / "pro" / "2000_Kaul_Mehrotra_Morck_Demand_Curves_for_Stocks_Do_Slope_Down.pdf",
    ),
    LiteraturePaper(
        paper_id="chang_hong_liskovich_2014",
        stance="正方",
        title="Regression Discontinuity and the Price Effects of Stock Market Indexing",
        authors="Yen-Cheng Chang; Harrison Hong; Inessa Liskovich",
        year_label="2014",
        market_focus="美国 / Russell",
        method_focus="断点回归, 指数化价格效应",
        project_module="沪深300论文复现",
        relevance_note="适合把 RDD 与指数效应机制结合起来写。",
        pdf_path=PDF_ROOT / "pro" / "2014_Chang_Hong_Liskovich_Price_Effects_of_Stock_Market_Indexing.pdf",
    ),
    LiteraturePaper(
        paper_id="yao_zhang_li_hs300",
        stance="正方",
        title="指数效应存在吗？——来自“沪深300”断点回归的证据",
        authors="姚东旻; 张日升; 李嘉晟",
        year_label="待补",
        market_focus="中国 / 沪深300",
        method_focus="断点回归, DID",
        project_module="沪深300论文复现",
        relevance_note="中国市场正向证据，最贴合当前项目的 A 股扩展模块。",
        pdf_path=PDF_ROOT / "pro" / "3439A97D91C0913CDC56FC9521E_597DEABF_14507A.pdf",
    ),
    LiteraturePaper(
        paper_id="denis_et_al_2003",
        stance="正方",
        title="S&P 500 Index Additions and Earnings Expectations",
        authors="Diane K. Denis; John J. McConnell; Alexei V. Ovtchinnikov; Yun Yu",
        year_label="2003",
        market_focus="美国 / S&P 500",
        method_focus="收益预期, 信息效应",
        project_module="短期价格压力",
        relevance_note="适合补充“纳入指数可能包含信息背书”这一机制。",
        pdf_path=PDF_ROOT / "pro" / "Denis_McConnell_Ovtchinnikov_Yu_2003.pdf",
    ),
)


def list_literature_papers() -> list[LiteraturePaper]:
    return list(PAPER_LIBRARY)


def get_literature_paper(paper_id: str) -> LiteraturePaper | None:
    for paper in PAPER_LIBRARY:
        if paper.paper_id == paper_id:
            return paper
    return None


def build_literature_catalog_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for paper in PAPER_LIBRARY:
        row = asdict(paper)
        row["pdf_path"] = str(paper.pdf_path)
        row["pdf_exists"] = paper.exists
        rows.append(row)
    return pd.DataFrame(rows)


def build_literature_dashboard_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for paper in PAPER_LIBRARY:
        open_link = f'<a href="/paper/{paper.paper_id}" target="_blank">打开 PDF</a>' if paper.exists else "PDF 不存在"
        rows.append(
            {
                "立场": paper.stance,
                "年份": paper.year_label,
                "作者": paper.authors,
                "题目": paper.title,
                "市场 / 指数": paper.market_focus,
                "方法 / 关键词": paper.method_focus,
                "项目模块": paper.project_module,
                "用途说明": paper.relevance_note,
                "PDF": open_link,
            }
        )
    return pd.DataFrame(rows)


def build_literature_summary_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    return (
        catalog.groupby(["stance", "project_module"], dropna=False)
        .agg(文献数量=("paper_id", "size"))
        .reset_index()
        .rename(columns={"stance": "立场", "project_module": "项目模块"})
    )


def build_literature_summary_markdown() -> str:
    counts = build_literature_catalog_frame()["stance"].value_counts()
    lines = [
        "# 16 篇指数效应文献库",
        "",
        "这套文献库已经按你当前的阅读框架整合进项目，并与现有三类分析模块建立了对应关系。",
        "",
        f"- 反方文献：{int(counts.get('反方', 0))} 篇",
        f"- 中性文献：{int(counts.get('中性', 0))} 篇",
        f"- 正方文献：{int(counts.get('正方', 0))} 篇",
        "",
        "推荐用法：",
        "- `短期价格压力`：优先对接 Harris and Gurel、Lynch and Mendenhall、Denis et al. 等文献。",
        "- `需求曲线效应`：优先对接 Shleifer、Kaul et al.、Wurgler and Zhuravskaya 等文献。",
        "- `沪深300论文复现`：优先对接姚东旻等、Chu et al.、Chang et al.、Ahn and Patatoukas 等文献。",
        "",
        "你现在可以在项目内直接查到每篇文献的立场、项目映射和 PDF 路径，不需要再额外手工整理。",
    ]
    return "\n".join(lines)


def build_project_track_frame(project_module: str) -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    grouped = catalog.loc[catalog["project_module"] == project_module].copy()
    grouped = grouped[
        ["stance", "year_label", "authors", "title", "market_focus", "method_focus", "relevance_note", "paper_id", "pdf_exists"]
    ].rename(
        columns={
            "stance": "立场",
            "year_label": "年份",
            "authors": "作者",
            "title": "题目",
            "market_focus": "市场 / 指数",
            "method_focus": "方法 / 关键词",
            "relevance_note": "在本项目中的作用",
            "paper_id": "paper_id",
            "pdf_exists": "pdf_exists",
        }
    )
    grouped["PDF"] = grouped.apply(
        lambda row: f'<a href="/paper/{row["paper_id"]}" target="_blank">打开 PDF</a>' if bool(row["pdf_exists"]) else "PDF 不存在",
        axis=1,
    )
    grouped = grouped.drop(columns=["paper_id", "pdf_exists"])
    return grouped.reset_index(drop=True)


def build_project_track_markdown(project_module: str) -> str:
    config = TRACK_LABELS[project_module]
    catalog = build_literature_catalog_frame()
    track = catalog.loc[catalog["project_module"] == project_module].copy()
    stance_counts = track["stance"].value_counts().to_dict()
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
        "",
        "推荐阅读方式：先看这条主线的支撑文献，再看下方事件研究、回归或 RDD 输出。",
    ]
    return "\n".join(lines)


def build_grouped_literature_frame(stance: str) -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    grouped = catalog.loc[catalog["stance"] == stance].copy()
    grouped = grouped[
        [
            "year_label",
            "authors",
            "title",
            "market_focus",
            "method_focus",
            "project_module",
            "relevance_note",
            "paper_id",
            "pdf_exists",
        ]
    ].rename(
        columns={
            "year_label": "年份",
            "authors": "作者",
            "title": "题目",
            "market_focus": "市场 / 指数",
            "method_focus": "方法 / 关键词",
            "project_module": "项目模块",
            "relevance_note": "论文里怎么用",
            "paper_id": "paper_id",
            "pdf_exists": "pdf_exists",
        }
    )
    grouped["PDF"] = grouped.apply(
        lambda row: f'<a href="/paper/{row["paper_id"]}" target="_blank">打开 PDF</a>' if bool(row["pdf_exists"]) else "PDF 不存在",
        axis=1,
    )
    grouped = grouped.drop(columns=["paper_id", "pdf_exists"])
    return grouped.reset_index(drop=True)


def build_literature_review_markdown() -> str:
    return "\n".join(
        [
            "# 文献综述导航页",
            "",
            "这个页面把 16 篇指数效应文献按 `反方 / 中性 / 正方` 三组拆开，便于你直接写文献综述。",
            "",
            "推荐阅读顺序：",
            "1. 先看反方：建立“指数效应并非无争议事实”的问题意识。",
            "2. 再看中性：把争论推进到“制度背景与识别策略”层面。",
            "3. 最后看正方：为需求曲线、被动资金冲击和价格效应机制提供理论与实证支撑。",
            "",
            "推荐写法：",
            "- 第一段写反方：强调短期价格压力、效应减弱或消失。",
            "- 第二段写中性：强调不同指数制度和识别方法的重要性。",
            "- 第三段写正方：强调需求曲线向下倾斜、股票非完全替代和被动资金冲击。",
            "",
            "如果你现在要写论文正文，这个页面最适合配合 `docs/literature_review_author_year_cn.md` 一起使用。",
        ]
    )
