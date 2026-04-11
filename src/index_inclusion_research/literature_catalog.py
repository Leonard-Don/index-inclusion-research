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

CAMP_LABELS = {
    "创世之战": {
        "order": 1,
        "title": "创世之战",
        "subtitle": "1986 年的经典对决",
        "description": "同一年、同一批 S&P 500 数据，引出了“永久重估”与“短期价格压力”两条主线。",
    },
    "正方深化": {
        "order": 2,
        "title": "正方深化",
        "subtitle": "为什么溢价能长期存在",
        "description": "沿着 Shleifer 的方向继续往下做，强调长期保留、信息背书与更干净的资金冲击证据。",
    },
    "市场摩擦与效应重估": {
        "order": 3,
        "title": "市场摩擦与效应重估",
        "subtitle": "套利、交易成本与消失的效应",
        "description": "这一路文献不只关心“有没有效应”，更关心“为什么没有被套利抹平”以及“为什么近年在美股变弱”。",
    },
    "方法革命": {
        "order": 4,
        "title": "方法革命",
        "subtitle": "RDD 的方法论升级",
        "description": "用断点回归把传统 CAR 争论推进到更干净的识别层面，是当前项目 RDD 模块的理论锚点。",
    },
    "中国A股主战场": {
        "order": 5,
        "title": "中国 A 股主战场",
        "subtitle": "中国市场里的本土证据",
        "description": "把指数效应、长期反转与 RDD 识别放进中国市场制度环境里重新检验，是项目 A 股扩展最直接的学术来源。",
    },
}

DEEP_ANALYSIS = {
    "harris_gurel_1986": {
        "identification_target": "公告日前后短期异常收益与成交量冲击",
        "challenged_assumption": "观察到上涨并不等于已经识别出永久重估",
        "deep_contribution": "把指数效应首先解释成短期价格压力，并为后续所有永久性争论设定了基准对照。",
    },
    "shleifer_1986": {
        "identification_target": "指数纳入所代表的外生需求冲击",
        "challenged_assumption": "股票可以被视为完全可替代资产",
        "deep_contribution": "把指数效应从交易现象推进成资产定价问题，提出需求曲线向下倾斜这一理论根基。",
    },
    "lynch_mendenhall_1997": {
        "identification_target": "公告日与生效日分离后的价格反应路径",
        "challenged_assumption": "价格要么完全回吐，要么完全永久化",
        "deep_contribution": "证明短期价格压力和部分持久效应可以同时存在，是连接两大经典解释的桥梁文献。",
    },
    "kaul_mehrotra_morck_2000": {
        "identification_target": "不伴随信息变化的纯权重调整冲击",
        "challenged_assumption": "指数效应主要来自信息或样本选择",
        "deep_contribution": "用更干净的权重变动说明纯资金冲击本身也能带来不易回吐的价格变化。",
    },
    "denis_et_al_2003": {
        "identification_target": "纳入前后分析师预期与真实盈利改善",
        "challenged_assumption": "S&P 500 纳入是无信息事件",
        "deep_contribution": "把机制争论从纯需求曲线推进到信息背书，提醒研究者不能把所有上涨都解释成被动买盘。",
    },
    "wurgler_zhuravskaya_2002": {
        "identification_target": "股票可替代性与需求曲线斜率之间的关系",
        "challenged_assumption": "若存在错定价，套利者会立刻把它抹平",
        "deep_contribution": "补上了需求曲线机制的套利约束基础，解释为什么偏离可能持续存在。",
    },
    "madhavan_2003": {
        "identification_target": "Russell 重构中的收益冲击、流动性与交易成本",
        "challenged_assumption": "指数效应只能是纯暂时或纯永久二选一",
        "deep_contribution": "说明现实市场里短期价格压力与更长期的流动性变化往往叠加出现。",
    },
    "petajisto_2011": {
        "identification_target": "指数溢价、价格弹性与指数换手成本",
        "challenged_assumption": "指数效应只是学术上的价格异象，与资产管理实践无关",
        "deep_contribution": "把指数效应转译成被动基金承担的隐性交易成本，连接学术争论与产业含义。",
    },
    "kasch_sarkar_2011": {
        "identification_target": "纳入前强势表现与纳入后价值/共动变化",
        "challenged_assumption": "纳入样本可被视为近似随机，永久效应可直接归因于指数成员身份",
        "deep_contribution": "把很多永久效应重新解释为纳入前盈利、规模和动量表现的延续，强调样本选择偏差。",
    },
    "ahn_patatoukas_2022": {
        "identification_target": "指数化对套利能力与价格发现速度的影响",
        "challenged_assumption": "指数化天然会恶化市场定价效率",
        "deep_contribution": "把争论提升到市场质量层面，说明指数化在部分样本上甚至可能改善价格发现。",
    },
    "coakley_et_al_2022": {
        "identification_target": "期权市场隐含 beta 所反映的前瞻共动变化",
        "challenged_assumption": "正式纳入时点仍是效应的主要信息到达时刻",
        "deep_contribution": "说明现代市场中许多指数效应早在公告前就被预期和交易，正式事件日剩余空间有限。",
    },
    "greenwood_sammon_2022": {
        "identification_target": "S&P 500 指数效应的年代变化",
        "challenged_assumption": "经典美股指数效应在不同时代都同样强",
        "deep_contribution": "把问题改写成时变效应：机制未必消失，但可见 alpha 已被更成熟的套利显著压缩。",
    },
    "chang_hong_liskovich_2014": {
        "identification_target": "Russell 断点附近纯指数化冲击的价格效应",
        "challenged_assumption": "普通 CAR 足以干净识别指数效应",
        "deep_contribution": "用 RDD 把争论从现象描述推进到更强识别，是后续中国市场 RDD 文献的直接方法祖先。",
    },
    "chu_et_al_2021": {
        "identification_target": "CSI 300 调入调出的长期持有期表现",
        "challenged_assumption": "中国市场会复制美股那套标准长期路径",
        "deep_contribution": "揭示中国市场长期结果的反常性和制度异质性，强调本土投资者结构与国企特征的重要性。",
    },
    "yao_zhang_li_hs300": {
        "identification_target": "沪深300 边界样本的调入调出效应",
        "challenged_assumption": "传统事件研究足以在中国市场给出稳定结论",
        "deep_contribution": "把中国市场指数效应的争论重新锚定到识别方法本身，强调 RD 与 DID 的必要性。",
    },
    "yao_zhou_chen_2022": {
        "identification_target": "CSI300 边界样本在国际期刊框架下的 RD 与 DID 证据",
        "challenged_assumption": "中国市场证据只能停留在现象描述，难以进入更一般的指数效应争论",
        "deep_contribution": "将中国市场的不对称指数效应放进国际准实验识别语言中，增强了可比性和外部说服力。",
    },
}


@dataclass(frozen=True)
class LiteraturePaper:
    paper_id: str
    stance: str
    camp: str
    title: str
    authors: str
    year_label: str
    market_focus: str
    method_focus: str
    project_module: str
    relevance_note: str
    core_logic: str
    one_line_role: str
    practical_use: str
    pdf_path: Path

    @property
    def exists(self) -> bool:
        return self.pdf_path.exists()

    @property
    def camp_order(self) -> int:
        return int(CAMP_LABELS[self.camp]["order"])


PAPER_LIBRARY: tuple[LiteraturePaper, ...] = (
    LiteraturePaper(
        paper_id="harris_gurel_1986",
        stance="反方",
        camp="创世之战",
        title="Price and Volume Effects Associated with Changes in the S&P 500 List: New Evidence for the Existence of Price Pressures",
        authors="Lawrence Harris; Eitan Gurel",
        year_label="1986",
        market_focus="美国 / S&P 500",
        method_focus="事件研究, 价格压力",
        project_module="短期价格压力",
        relevance_note="短期价格压力路径的核心经典文献。",
        core_logic="被动资金集中建仓打破短期流动性平衡，纳入效应更像是临时价格压力，而不是永久性重估。",
        one_line_role="短期价格压力假说的开山作。",
        practical_use="适用于解释短窗口 CAR、成交量放大和后续回吐的经典价格压力证据。",
        pdf_path=PDF_ROOT / "con" / "1986_Harris_Gurel_Price_and_Volume_Effects_SP500.pdf",
    ),
    LiteraturePaper(
        paper_id="shleifer_1986",
        stance="正方",
        camp="创世之战",
        title="Do Demand Curves for Stocks Slope Down?",
        authors="Andrei Shleifer",
        year_label="1986",
        market_focus="美国 / S&P 500",
        method_focus="需求曲线, 非完全替代",
        project_module="需求曲线效应",
        relevance_note="需求曲线向下倾斜机制的理论核心文献。",
        core_logic="股票不是完全可替代资产，被动资金的刚性买入会把股价抬上去，因此需求曲线向下倾斜。",
        one_line_role="需求曲线向下倾斜的开山作。",
        practical_use="适合作为论证指数纳入可能带来部分持久重估的理论起点。",
        pdf_path=PDF_ROOT / "pro" / "1986_Shleifer_Do_Demand_Curves_for_Stocks_Slope_Down.pdf",
    ),
    LiteraturePaper(
        paper_id="lynch_mendenhall_1997",
        stance="正方",
        camp="正方深化",
        title="New Evidence on Stock Price Effects Associated with Changes in the S&P 500 Index",
        authors="Anthony W. Lynch; Richard R. Mendenhall",
        year_label="1997",
        market_focus="美国 / S&P 500",
        method_focus="公告与生效分离, 事件研究",
        project_module="短期价格压力",
        relevance_note="适合放在早期经典正向实证证据部分。",
        core_logic="在提前公告、随后生效的新制度下，价格在公告后显著反应，但只被部分反转，说明既不是瞬时吃掉，也不是完全回吐。",
        one_line_role="公告与生效分离后，效应只部分反转的关键桥梁文献。",
        practical_use="适用于比较公告日与生效日效应，并讨论“部分保留”这一现象。",
        pdf_path=PDF_ROOT / "pro" / "1997_Lynch_Mendenhall_New_Evidence_SP500_Index.pdf",
    ),
    LiteraturePaper(
        paper_id="kaul_mehrotra_morck_2000",
        stance="正方",
        camp="正方深化",
        title="Demand Curves for Stocks Do Slope Down: New Evidence from an Index Weights Adjustment",
        authors="Aditya Kaul; Vikas Mehrotra; Randall Morck",
        year_label="2000",
        market_focus="加拿大 / TSE 300",
        method_focus="指数权重调整, 需求曲线",
        project_module="需求曲线效应",
        relevance_note="与 Shleifer 一起支撑需求曲线向下倾斜。",
        core_logic="即使没有成分股纳入/剔除，只要指数权重发生机械调整，价格依然会出现难以回吐的变化，更接近纯资金冲击导致的长期重估。",
        one_line_role="用更干净的权重冲击实锤需求曲线向下倾斜。",
        practical_use="适用于说明即使不发生基本面变化，纯权重冲击也可能推高价格。",
        pdf_path=PDF_ROOT / "pro" / "2000_Kaul_Mehrotra_Morck_Demand_Curves_for_Stocks_Do_Slope_Down.pdf",
    ),
    LiteraturePaper(
        paper_id="denis_et_al_2003",
        stance="正方",
        camp="正方深化",
        title="S&P 500 Index Additions and Earnings Expectations",
        authors="Diane K. Denis; John J. McConnell; Alexei V. Ovtchinnikov; Yun Yu",
        year_label="2003",
        market_focus="美国 / S&P 500",
        method_focus="收益预期, 信息效应",
        project_module="短期价格压力",
        relevance_note="适合补充“纳入指数可能包含信息背书”这一机制。",
        core_logic="指数纳入不一定是无信息事件，分析师预期和后续业绩改善说明纳入可能包含质量背书和信息效应。",
        one_line_role="把“纯资金冲击”扩展到“信息背书”的关键文献。",
        practical_use="适用于说明公告日上涨未必完全由被动资金驱动，信息机制同样可能发挥作用。",
        pdf_path=PDF_ROOT / "pro" / "Denis_McConnell_Ovtchinnikov_Yu_2003.pdf",
    ),
    LiteraturePaper(
        paper_id="wurgler_zhuravskaya_2002",
        stance="中性",
        camp="市场摩擦与效应重估",
        title="Does Arbitrage Flatten Demand Curves for Stocks?",
        authors="Jeffrey Wurgler; Ekaterina Zhuravskaya",
        year_label="2002",
        market_focus="跨市场 / 机制",
        method_focus="需求曲线, 套利, 市场分割",
        project_module="需求曲线效应",
        relevance_note="适合做机制桥梁，把事件效应和需求曲线讨论连起来。",
        core_logic="套利者找不到足够完美的替代资产时，错误定价不会立刻被抹平，因此需求曲线即便向下倾斜也可能持续存在。",
        one_line_role="解释为什么价格压力不会秒回归的套利约束文献。",
        practical_use="适用于在机制部分连接“供需冲击”与“价格偏差为何持续”这两层解释。",
        pdf_path=PDF_ROOT / "mid" / "Wurgler_Zhuravskaya_2002_working_paper.pdf",
    ),
    LiteraturePaper(
        paper_id="madhavan_2003",
        stance="中性",
        camp="市场摩擦与效应重估",
        title="The Russell Reconstitution Effect",
        authors="Ananth Madhavan",
        year_label="2003",
        market_focus="美国 / Russell",
        method_focus="重构制度, 交易微观结构",
        project_module="沪深300论文复现",
        relevance_note="强调不同指数制度下效应与交易冲击的差异。",
        core_logic="Russell 重构既有显著交易冲击和流动性摩擦，也可能伴随成员身份带来的更长期影响，现实里两种机制会并存。",
        one_line_role="说明短期价格压力与永久影响可以同时存在的混合型文献。",
        practical_use="适用于说明现实市场中短期价格压力与长期影响可以同时存在。",
        pdf_path=PDF_ROOT / "mid" / "2003_Madhavan_The_Russell_Reconstitution_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="petajisto_2011",
        stance="反方",
        camp="市场摩擦与效应重估",
        title="The index premium and its hidden cost for index funds",
        authors="Antti Petajisto",
        year_label="2011",
        market_focus="美国 / S&P 500, Russell 2000",
        method_focus="指数溢价, 价格弹性",
        project_module="短期价格压力",
        relevance_note="适合连接被动投资、指数溢价与隐藏成本讨论。",
        core_logic="指数溢价本质上是被动资金换仓时支付的隐性成本，这一成本会被提前布局的套利者截取。",
        one_line_role="把指数溢价量化成指数基金隐性成本的关键文献。",
        practical_use="适用于把指数溢价讨论转化为被动基金隐性交易成本的产业语言。",
        pdf_path=PDF_ROOT / "con" / "Petajisto_2011_index_premium_hidden_cost.pdf",
    ),
    LiteraturePaper(
        paper_id="kasch_sarkar_2011",
        stance="反方",
        camp="市场摩擦与效应重估",
        title="Is There an S&P 500 Index Effect?",
        authors="Maria Kasch; Asani Sarkar",
        year_label="2011",
        market_focus="美国 / S&P 500",
        method_focus="指数效应再检验",
        project_module="短期价格压力",
        relevance_note="适合支撑“经典指数效应是否仍显著存在”的质疑。",
        core_logic="很多纳入股本来就是涨得更猛、业绩更好、估值更高的公司，若剔除这些预先特征，永久指数效应会明显减弱。",
        one_line_role="把很多“永久效应”重新解释成动量与选择偏差的代表文献。",
        practical_use="适用于讨论事件研究可能把强势股动量误识别为指数效应的问题。",
        pdf_path=PDF_ROOT / "con" / "2011_Kasch_Sarkar_Is_There_an_SP500_Index_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="ahn_patatoukas_2022",
        stance="中性",
        camp="市场摩擦与效应重估",
        title="Identifying the Effect of Stock Indexing: Impetus or Impediment to Arbitrage and Price Discovery?",
        authors="Byung Hyun Ahn; Panos N. Patatoukas",
        year_label="2022",
        market_focus="美国",
        method_focus="识别策略, 套利, 价格发现",
        project_module="沪深300论文复现",
        relevance_note="适合支撑“识别设计比简单事件研究更重要”的方法论段落。",
        core_logic="指数化不一定破坏定价效率，在某些更受套利约束的股票上反而可能提升价格发现速度。",
        one_line_role="把争论从“涨不涨”推进到“指数化如何改变价格发现”的文献。",
        practical_use="适用于从更高层次讨论指数化是否影响市场效率，而不局限于纳入日涨跌。",
        pdf_path=PDF_ROOT / "mid" / "2022_Ahn_Patatoukas_Identifying_the_Effect_of_Stock_Indexing.pdf",
    ),
    LiteraturePaper(
        paper_id="coakley_et_al_2022",
        stance="反方",
        camp="市场摩擦与效应重估",
        title="The S&P 500 Index inclusion effect: Evidence from the options market",
        authors="Jerry Coakley; George Dotsis; Apostolos Kourtis; Dimitris Psychoyios",
        year_label="2022",
        market_focus="美国 / S&P 500",
        method_focus="期权市场, 前瞻 beta",
        project_module="短期价格压力",
        relevance_note="更适合写进“现代市场中传统纳入效应减弱”的部分。",
        core_logic="利用期权市场的前瞻性指标可以看到，很多效应在正式纳入前就被市场提前消化，纳入时点本身未必还剩多少 Alpha。",
        one_line_role="用期权市场证据支持现代市场中效应弱化的文献。",
        practical_use="适用于说明收益更可能来自预期差，而非生效日当天的公开冲击。",
        pdf_path=PDF_ROOT / "con" / "2022_Coakley_Saffi_Wang_SP500_Index_Inclusion_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="greenwood_sammon_2022",
        stance="反方",
        camp="市场摩擦与效应重估",
        title="The Disappearing Index Effect",
        authors="Robin Greenwood; Marco Sammon",
        year_label="2022",
        market_focus="美国 / S&P 500",
        method_focus="长期时间趋势",
        project_module="短期价格压力",
        relevance_note="最适合放在“指数效应正在消失”这句的核心引用位置。",
        core_logic="随着套利者和事件驱动资金越来越成熟，美股经典指数纳入效应已经从高点大幅衰减。",
        one_line_role="给“美股指数效应正在消失”定调的最新前沿文献。",
        practical_use="适用于讨论策略拥挤、套利压缩与 Alpha 衰减。",
        pdf_path=PDF_ROOT / "con" / "2022_Greenwood_Sammon_The_Disappearing_Index_Effect.pdf",
    ),
    LiteraturePaper(
        paper_id="chang_hong_liskovich_2014",
        stance="正方",
        camp="方法革命",
        title="Regression Discontinuity and the Price Effects of Stock Market Indexing",
        authors="Yen-Cheng Chang; Harrison Hong; Inessa Liskovich",
        year_label="2014",
        market_focus="美国 / Russell",
        method_focus="断点回归, 指数化价格效应",
        project_module="沪深300论文复现",
        relevance_note="适合把 RDD 与指数效应机制结合起来写。",
        core_logic="利用 Russell 指数按市值排名的天然门槛构造断点回归，剔除大部分基本面噪音，更干净地识别纯指数化冲击。",
        one_line_role="RDD 识别指数效应的方法论开山鼻祖。",
        practical_use="适合作为断点回归识别指数效应的核心方法论依据。",
        pdf_path=PDF_ROOT / "pro" / "2014_Chang_Hong_Liskovich_Price_Effects_of_Stock_Market_Indexing.pdf",
    ),
    LiteraturePaper(
        paper_id="chu_et_al_2021",
        stance="正方",
        camp="中国A股主战场",
        title="Long-term impacts of index reconstitutions: Evidence from the CSI 300 additions and deletions",
        authors="Gang Chu; John W. Goodell; Xiao Li; Yongjie Zhang",
        year_label="2021",
        market_focus="中国 / CSI300",
        method_focus="长期效应, 调入调出",
        project_module="沪深300论文复现",
        relevance_note="适合补充中国市场长期指数重构影响的正向证据。",
        core_logic="中国市场里调入和调出的长期路径并不遵循美股那套标准答案，删除股长期反而可能更强，背后带有鲜明的本土制度与投资者结构特征。",
        one_line_role="把指数效应问题重新放回 A 股制度环境里的长期异象文献。",
        practical_use="适用于说明 A 股市场不能简单照搬美股指数效应叙事。",
        pdf_path=PDF_ROOT / "pro" / "1-s2.0-S0927538X2100158X-main.pdf",
    ),
    LiteraturePaper(
        paper_id="yao_zhang_li_hs300",
        stance="正方",
        camp="中国A股主战场",
        title="指数效应存在吗？——来自“沪深300”断点回归的证据",
        authors="姚东旻; 张日升; 李嘉晟",
        year_label="待补",
        market_focus="中国 / 沪深300",
        method_focus="断点回归, DID",
        project_module="沪深300论文复现",
        relevance_note="中国市场正向证据，最贴合当前项目的 A 股扩展模块。",
        core_logic="把 RDD 识别直接落到沪深300，给出中国市场调入效应显著、调出效应不对称的本土证据。",
        one_line_role="把中国市场 RDD 识别思路转化为中文实证语言的核心论文。",
        practical_use="适合作为中国样本 RDD 识别框架的中文核心依据。",
        pdf_path=PDF_ROOT / "pro" / "3439A97D91C0913CDC56FC9521E_597DEABF_14507A.pdf",
    ),
    LiteraturePaper(
        paper_id="yao_zhou_chen_2022",
        stance="反方",
        camp="中国A股主战场",
        title="Price effects in the Chinese stock market: Evidence from the China securities index (CSI300) based on regression discontinuity",
        authors="Dongmin Yao; Shiyu Zhou; Yijing Chen",
        year_label="2022",
        market_focus="中国 / CSI300",
        method_focus="断点回归, DID",
        project_module="沪深300论文复现",
        relevance_note="适合放在“指数效应是否稳健存在仍有争议”的反方补充位置。",
        core_logic="在国际发表版中把 CSI300 的断点回归证据推向更正式的学术场域，也提示中国市场证据需要和更大的指数效应争论一起理解。",
        one_line_role="为中国市场 RDD 识别提供国际发表层面的学术背书。",
        practical_use="适用于说明中国市场 RDD 识别已获得国际期刊层面的实证支持。",
        pdf_path=PDF_ROOT / "con" / "1-s2.0-S1544612321004244-main.pdf",
    ),
)


def _compact_author_label(authors: str) -> str:
    parts = [part.strip() for part in authors.split(";") if part.strip()]
    if not parts:
        return authors

    def family_name(name: str) -> str:
        tokens = name.split()
        return tokens[-1] if len(tokens) > 1 else name

    families = [family_name(part) for part in parts]
    if len(families) == 1:
        return families[0]
    if len(families) == 2:
        return f"{families[0]}、{families[1]}"
    return f"{families[0]} 等"


def _paper_citation(paper: LiteraturePaper) -> str:
    compact_authors = _compact_author_label(paper.authors)
    year = paper.year_label
    return f"{compact_authors}（{year}）"


def _year_sort_value(year_label: str) -> int:
    digits = "".join(char for char in str(year_label) if char.isdigit())
    if digits:
        return int(digits)
    return 9999


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
        row.update(DEEP_ANALYSIS.get(paper.paper_id, {}))
        row["pdf_path"] = str(paper.pdf_path)
        row["pdf_exists"] = paper.exists
        row["camp_order"] = paper.camp_order
        row["year_order"] = _year_sort_value(paper.year_label)
        rows.append(row)
    return (
        pd.DataFrame(rows)
        .sort_values(["camp_order", "year_order", "title"], kind="stable")
        .reset_index(drop=True)
    )


def build_literature_dashboard_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for paper in PAPER_LIBRARY:
        open_link = f'<a href="/paper/{paper.paper_id}" target="_blank">文献讲义</a>' if paper.exists else "PDF 不存在"
        rows.append(
            {
                "阵营": CAMP_LABELS[paper.camp]["title"],
                "立场": paper.stance,
                "代表文献": _paper_citation(paper),
                "年份": paper.year_label,
                "市场 / 指数": paper.market_focus,
                "方法 / 关键词": paper.method_focus,
                "识别对象": DEEP_ANALYSIS.get(paper.paper_id, {}).get("identification_target", ""),
                "挑战的假设": DEEP_ANALYSIS.get(paper.paper_id, {}).get("challenged_assumption", ""),
                "一句话定位": paper.one_line_role,
                "争论推进": DEEP_ANALYSIS.get(paper.paper_id, {}).get("deep_contribution", ""),
                "项目模块": paper.project_module,
                "研究中的作用": paper.practical_use,
                "PDF": open_link,
            }
        )
    frame = pd.DataFrame(rows)
    frame["阵营顺序"] = frame["阵营"].map(
        lambda value: next(config["order"] for config in CAMP_LABELS.values() if config["title"] == value)
    )
    frame["年份顺序"] = frame["年份"].map(_year_sort_value)
    return (
        frame.sort_values(["阵营顺序", "年份顺序", "代表文献"], kind="stable")
        .drop(columns=["阵营顺序", "年份顺序"])
        .reset_index(drop=True)
    )


def build_literature_summary_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    return (
        catalog.groupby(["stance", "project_module"], dropna=False)
        .agg(文献数量=("paper_id", "size"))
        .reset_index()
        .rename(columns={"stance": "立场", "project_module": "项目模块"})
    )


def build_camp_summary_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    counts = catalog.groupby("camp", dropna=False).agg(文献数量=("paper_id", "size")).reset_index()
    rows: list[dict[str, object]] = []
    for _, row in counts.iterrows():
        camp = str(row["camp"])
        config = CAMP_LABELS[camp]
        rows.append(
            {
                "阵营": config["title"],
                "副标题": config["subtitle"],
                "核心问题": config["description"],
                "文献数量": int(row["文献数量"]),
            }
        )
    frame = pd.DataFrame(rows)
    frame["阵营顺序"] = frame["阵营"].map(
        lambda value: next(config["order"] for config in CAMP_LABELS.values() if config["title"] == value)
    )
    return frame.sort_values("阵营顺序").drop(columns=["阵营顺序"]).reset_index(drop=True)


def build_literature_evolution_frame() -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    frame = catalog[
        [
            "camp",
            "stance",
            "authors",
            "year_label",
            "market_focus",
            "method_focus",
            "identification_target",
            "challenged_assumption",
            "one_line_role",
            "deep_contribution",
            "project_module",
            "practical_use",
            "camp_order",
            "year_order",
        ]
    ].rename(
        columns={
            "camp": "阵营",
            "stance": "立场",
            "authors": "作者",
            "year_label": "年份",
            "market_focus": "市场 / 指数",
            "method_focus": "方法 / 关键词",
            "identification_target": "识别对象",
            "challenged_assumption": "挑战的假设",
            "one_line_role": "一句话定位",
            "deep_contribution": "争论推进",
            "project_module": "项目模块",
            "practical_use": "研究中的作用",
            "camp_order": "camp_order",
            "year_order": "year_order",
        }
    )
    frame["阵营"] = frame["阵营"].map(lambda value: CAMP_LABELS[value]["title"])
    frame["代表文献"] = frame.apply(
        lambda row: f"{_compact_author_label(str(row['作者']))}（{row['年份']}）",
        axis=1,
    )
    ordered = frame[
        [
            "阵营",
            "立场",
            "代表文献",
            "市场 / 指数",
            "方法 / 关键词",
            "识别对象",
            "挑战的假设",
            "一句话定位",
            "争论推进",
            "项目模块",
            "研究中的作用",
            "camp_order",
            "year_order",
        ]
    ]
    return (
        ordered.sort_values(["camp_order", "year_order", "代表文献"], kind="stable")
        .drop(columns=["camp_order", "year_order"])
        .reset_index(drop=True)
    )


def build_literature_meeting_frame() -> pd.DataFrame:
    rows = [
        {
            "讨论主题": "美股指数效应是否仍具显著性",
            "代表文献": "Greenwood and Sammon (2022); Petajisto (2011)",
            "核心表述": "美股指数红利已被事件驱动资金显著压缩，收益更依赖预期差与提前量，而非生效日当天的公开冲击。",
        },
        {
            "讨论主题": "短期价格压力",
            "代表文献": "Harris and Gurel (1986); Lynch and Mendenhall (1997)",
            "核心表述": "短窗口 CAR 与成交量放大更接近建仓冲击，但公告与生效之间仍可能保留部分持续效应。",
        },
        {
            "讨论主题": "需求曲线与长期保留",
            "代表文献": "Shleifer (1986); Kaul, Mehrotra and Morck (2000)",
            "核心表述": "即使不伴随基本面变化，纯指数权重冲击也可能带来不完全回吐的价格效应，这支持股票需求曲线并非水平。",
        },
        {
            "讨论主题": "价格偏差为何未被迅速套利抹平",
            "代表文献": "Wurgler and Zhuravskaya (2002); Madhavan (2003)",
            "核心表述": "现实市场中替代资产不完美、冲击集中且交易摩擦显著，因此价格偏离不会像教科书假设那样瞬时消失。",
        },
        {
            "讨论主题": "为何需要使用 RDD",
            "代表文献": "Chang, Hong and Liskovich (2014); Ahn and Patatoukas (2022)",
            "核心表述": "传统 CAR 争论常受内生性与样本选择影响，断点回归能够更干净地隔离指数化本身的效应。",
        },
        {
            "讨论主题": "为何中国市场仍值得持续研究",
            "代表文献": "Chu et al. (2021); 姚东旻等; Yao, Zhou and Chen (2022)",
            "核心表述": "中国市场的做空约束、散户结构与指数制度使得指数效应呈现出更强的不对称性与更鲜明的本土特征。",
        },
    ]
    return pd.DataFrame(rows)


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


def build_project_track_frame(project_module: str) -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    grouped = catalog.loc[catalog["project_module"] == project_module].copy()
    grouped = grouped[
        [
            "camp",
            "stance",
            "authors",
            "year_label",
            "identification_target",
            "challenged_assumption",
            "one_line_role",
            "practical_use",
            "paper_id",
            "pdf_exists",
            "camp_order",
            "year_order",
        ]
    ].rename(
        columns={
            "camp": "阵营",
            "stance": "立场",
            "authors": "作者",
            "year_label": "年份",
            "identification_target": "识别对象",
            "challenged_assumption": "挑战的假设",
            "one_line_role": "一句话定位",
            "practical_use": "在本项目中的作用",
            "paper_id": "paper_id",
            "pdf_exists": "pdf_exists",
            "camp_order": "camp_order",
            "year_order": "year_order",
        }
    )
    grouped["阵营"] = grouped["阵营"].map(lambda value: CAMP_LABELS[value]["title"])
    grouped["PDF"] = grouped.apply(
        lambda row: f'<a href="/paper/{row["paper_id"]}" target="_blank">文献讲义</a>' if bool(row["pdf_exists"]) else "PDF 不存在",
        axis=1,
    )
    grouped["代表文献"] = grouped.apply(
        lambda row: f"{_compact_author_label(str(row['作者']))}（{row['年份']}）",
        axis=1,
    )
    grouped = grouped[
        ["阵营", "立场", "代表文献", "识别对象", "挑战的假设", "一句话定位", "在本项目中的作用", "PDF", "camp_order", "year_order"]
    ].sort_values(["camp_order", "year_order", "代表文献"], kind="stable")
    return grouped.drop(columns=["camp_order", "year_order"]).reset_index(drop=True)


def build_project_track_markdown(project_module: str) -> str:
    config = TRACK_LABELS[project_module]
    catalog = build_literature_catalog_frame()
    track = catalog.loc[catalog["project_module"] == project_module].copy()
    stance_counts = track["stance"].value_counts().to_dict()
    camp_counts = track["camp"].value_counts().to_dict()
    camp_text = "、".join(f"{camp} {count} 篇" for camp, count in sorted(camp_counts.items(), key=lambda item: CAMP_LABELS[item[0]]["order"]))
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


def build_project_track_support_records(project_module: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for paper in PAPER_LIBRARY:
        if paper.project_module != project_module:
            continue
        records.append(
            {
                "citation": _paper_citation(paper),
                "camp": CAMP_LABELS[paper.camp]["title"],
                "year_label": paper.year_label,
                "year_order": _year_sort_value(paper.year_label),
                "stance": paper.stance,
                "market_focus": paper.market_focus,
                "method_focus": paper.method_focus,
                "identification_target": DEEP_ANALYSIS.get(paper.paper_id, {}).get("identification_target", ""),
                "challenged_assumption": DEEP_ANALYSIS.get(paper.paper_id, {}).get("challenged_assumption", ""),
                "deep_contribution": DEEP_ANALYSIS.get(paper.paper_id, {}).get("deep_contribution", ""),
                "one_line_role": paper.one_line_role,
                "practical_use": paper.practical_use,
                "pdf_href": f"/paper/{paper.paper_id}" if paper.exists else "",
            }
        )
    records.sort(
        key=lambda row: (
            next(config["order"] for config in CAMP_LABELS.values() if config["title"] == row["camp"]),
            int(row["year_order"]),
            str(row["citation"]),
        )
    )
    return records


def build_grouped_literature_frame(stance: str) -> pd.DataFrame:
    catalog = build_literature_catalog_frame()
    grouped = catalog.loc[catalog["stance"] == stance].copy()
    grouped = grouped[
        [
            "camp",
            "authors",
            "year_label",
            "market_focus",
            "identification_target",
            "challenged_assumption",
            "one_line_role",
            "practical_use",
            "paper_id",
            "pdf_exists",
            "camp_order",
            "year_order",
        ]
    ].rename(
        columns={
            "camp": "阵营",
            "authors": "作者",
            "year_label": "年份",
            "market_focus": "市场 / 指数",
            "identification_target": "识别对象",
            "challenged_assumption": "挑战的假设",
            "one_line_role": "一句话定位",
            "practical_use": "研究中的作用",
            "paper_id": "paper_id",
            "pdf_exists": "pdf_exists",
            "camp_order": "camp_order",
            "year_order": "year_order",
        }
    )
    grouped["阵营"] = grouped["阵营"].map(lambda value: CAMP_LABELS[value]["title"])
    grouped["PDF"] = grouped.apply(
        lambda row: f'<a href="/paper/{row["paper_id"]}" target="_blank">文献讲义</a>' if bool(row["pdf_exists"]) else "PDF 不存在",
        axis=1,
    )
    grouped["代表文献"] = grouped.apply(
        lambda row: f"{_compact_author_label(str(row['作者']))}（{row['年份']}）",
        axis=1,
    )
    grouped = grouped[
        ["阵营", "代表文献", "市场 / 指数", "识别对象", "挑战的假设", "一句话定位", "研究中的作用", "PDF", "camp_order", "year_order"]
    ].sort_values(["camp_order", "year_order", "代表文献"], kind="stable")
    return grouped.drop(columns=["camp_order", "year_order"]).reset_index(drop=True)


def build_literature_framework_markdown() -> str:
    lines = [
        "# 指数效应研究的五大阵营",
        "",
        "这 16 篇文献现在不只是一份 PDF 清单，而是一条完整的研究史和策略框架。",
        "",
        "这一框架可按以下顺序展开：",
        "1. 先用 `创世之战` 立起争论：指数纳入后的上涨，到底是永久重估还是短期价格压力？",
        "2. 再用 `正方深化` 解释为什么效应可能部分保留：需求曲线、权重冲击、信息背书都能提供证据。",
        "3. 接着用 `市场摩擦与效应重估` 说明为什么偏差没有瞬间消失，以及为什么在现代美股里效应显著变弱。",
        "4. 然后用 `方法革命` 说明为什么今天必须上 RDD，而不能只靠普通 CAR 争论。",
        "5. 最后落到 `中国 A 股主战场`：海外效应减弱，不代表 A 股没有，制度环境和投资者结构会重新塑造结论。",
        "",
        "这一框架可进一步概括为以下三点：",
        "- 美股老 Alpha 已经被吃薄，但方法论和机制讨论还很值钱。",
        "- 中国市场因为制度差异，依然值得用更强识别方法继续做。",
        "- 三条研究主线对应了这条文献演进链在实证层面的落地。",
    ]
    return "\n".join(lines)


def build_literature_review_markdown() -> str:
    return "\n".join(
        [
            "# 文献综述导航页",
            "",
            "这个页面把 16 篇指数效应文献按 `反方 / 中性 / 正方` 三组拆开，便于组织传统文献综述。",
            "",
            "若需展示研究史、方法演进或策略脉络，可配合“五大阵营”页面一并阅读。",
            "",
            "建议阅读顺序：",
            "1. 先看反方：建立“指数效应并非无争议事实”的问题意识。",
            "2. 再看中性：把争论推进到“制度背景与识别策略”层面。",
            "3. 最后看正方：为需求曲线、被动资金冲击和价格效应机制提供理论与实证支撑。",
            "",
            "建议写法：",
            "- 第一段写反方：强调短期价格压力、效应减弱或消失。",
            "- 第二段写中性：强调不同指数制度和识别方法的重要性。",
            "- 第三段写正方：强调需求曲线向下倾斜、股票非完全替代和被动资金冲击。",
            "",
            "该页面可与 `docs/literature_review_author_year_cn.md` 配合使用，以形成更完整的文献综述正文。",
        ]
    )
