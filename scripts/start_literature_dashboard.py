from __future__ import annotations

import os
import sys
from pathlib import Path

from flask import Flask, abort, redirect, render_template_string, request, send_file, url_for

os.environ.setdefault("MPLBACKEND", "Agg")

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SRC = ROOT / "src"
for path in [SCRIPTS, SRC]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from start_demand_curve_track import run_analysis as run_demand_curve_track
from start_identification_china_track import run_analysis as run_identification_china_track
from start_price_pressure_track import run_analysis as run_price_pressure_track
from index_inclusion_research.literature_catalog import (
    build_camp_summary_frame,
    build_literature_catalog_frame,
    build_grouped_literature_frame,
    build_literature_dashboard_frame,
    build_literature_evolution_frame,
    build_literature_framework_markdown,
    build_literature_meeting_frame,
    build_literature_review_markdown,
    build_literature_summary_frame,
    build_literature_summary_markdown,
    build_project_track_frame,
    build_project_track_markdown,
    build_project_track_support_records,
    get_literature_paper,
)
from index_inclusion_research.supplementary import (
    build_case_playbook_frame,
    build_event_clock_frame,
    build_impact_formula_frame,
    build_mechanism_chain_frame,
    build_supplementary_summary_markdown,
    estimate_impact_scenarios,
)


ANALYSES = {
    "price_pressure_track": {
        "title": "短期价格压力与效应减弱",
        "subtitle": "Price Pressure & Disappearing Effect",
        "description_zh": "以反方文献和早期事件研究证据为底，检验短窗口 CAR、成交量冲击和效应减弱问题",
        "project_module": "短期价格压力",
        "runner": run_price_pressure_track,
    },
    "demand_curve_track": {
        "title": "需求曲线与长期保留",
        "subtitle": "Demand Curves & Long-run Retention",
        "description_zh": "以正方机制文献为底，检验价格是否只部分回吐以及需求曲线是否向下倾斜",
        "project_module": "需求曲线效应",
        "runner": run_demand_curve_track,
    },
    "identification_china_track": {
        "title": "制度识别与中国市场证据",
        "subtitle": "Identification & China Evidence",
        "description_zh": "以中性文献和中国市场证据为底，整合匹配对照组、DID 风格分析和 RDD 扩展",
        "project_module": "沪深300论文复现",
        "runner": run_identification_china_track,
    },
}

LIBRARY_CARD = {
    "title": "16 篇文献库",
    "subtitle": "Literature Library",
    "description_zh": "反方、中性、正方三组文献与项目模块映射",
}

REVIEW_CARD = {
    "title": "文献综述",
    "subtitle": "Review Navigator",
    "description_zh": "按反方、中性、正方三组查看 16 篇文献",
}

FRAMEWORK_CARD = {
    "title": "研究框架",
    "subtitle": "Five Camps",
    "description_zh": "按五大阵营查看 16 篇文献的演进脉络、项目映射与研究表达",
}

SUPPLEMENT_CARD = {
    "title": "机制与执行补充",
    "subtitle": "Mechanics & Execution",
    "description_zh": "事件时钟、机制链、冲击估算与表达框架，不进入文献库，仅作补充层",
}

PROJECT_MODULE_DISPLAY = {
    config["project_module"]: config["title"] for config in ANALYSES.values()
}

RUN_CACHE: dict[str, dict[str, object]] = {}

TABLE_LABELS = {
    "event_study_summary": "事件研究汇总表",
    "mechanism_summary": "机制变量汇总表",
    "retention_summary": "保留率汇总表",
    "did_summary": "DID 汇总表",
    "event_counts": "真实事件样本表",
    "panel_coverage": "事件窗口覆盖表",
    "regression_coefficients": "回归系数表",
    "regression_models": "模型统计表",
    "regression_dataset": "回归样本表",
    "match_diagnostics": "匹配诊断表",
    "rdd_summary": "RDD 汇总表",
    "event_level_with_running": "事件层运行变量样本表",
    "sample_scope": "样本范围总表",
    "data_sources": "数据来源与口径表",
    "identification_scope": "识别范围说明表",
    "long_window_event_study_summary": "长窗口事件研究汇总表",
}

COLUMN_LABELS = {
    "market": "市场",
    "event_phase": "事件阶段",
    "inclusion": "是否纳入",
    "window": "窗口",
    "window_slug": "窗口代码",
    "n_events": "事件数",
    "n_obs": "样本量",
    "mean_car": "平均 CAR",
    "std_car": "CAR 标准差",
    "t_stat": "t 值",
    "p_value": "p 值",
    "mean_turnover_change": "平均换手率变化",
    "mean_volume_change": "平均成交量变化",
    "mean_volatility_change": "平均波动率变化",
    "short_mean_car": "短窗口平均 CAR",
    "long_mean_car": "长窗口平均 CAR",
    "car_reversal": "CAR 回吐幅度",
    "retention_ratio": "保留率",
    "metric": "指标",
    "treated_post_minus_pre": "处理组后减前",
    "control_post_minus_pre": "对照组后减前",
    "did_estimate": "DID 估计值",
    "n_treated": "处理组数量",
    "n_control": "对照组数量",
    "specification": "回归规格",
    "dependent_variable": "被解释变量",
    "coefficient": "系数",
    "std_error": "标准误",
    "r_squared": "R²",
    "adj_r_squared": "调整后 R²",
    "outcome": "结果变量",
    "bandwidth": "带宽",
    "n_left": "左侧样本数",
    "n_right": "右侧样本数",
    "tau": "断点效应",
    "event_id": "事件 ID",
    "index_name": "指数名称",
    "event_ticker": "股票代码",
    "event_type": "事件类型",
    "event_date": "事件日期",
    "sector": "行业",
    "log_mkt_cap": "对数市值",
    "pre_event_return": "事件前收益",
    "turnover_change": "换手率变化",
    "volume_change": "成交量变化",
    "volatility_change": "波动率变化",
    "running_variable": "运行变量",
    "cutoff": "断点阈值",
    "distance_to_cutoff": "距断点距离",
    "batch_id": "批次 ID",
    "comparison_id": "比较组 ID",
    "parameter": "参数",
    "term": "变量",
    "car_m1_p1": "CAR[-1,+1]",
    "car_m3_p3": "CAR[-3,+3]",
    "car_m5_p5": "CAR[-5,+5]",
    "car_p0_p5": "CAR[0,+5]",
    "car_p0_p20": "CAR[0,+20]",
    "car_p0_p60": "CAR[0,+60]",
    "car_p0_p120": "CAR[0,+120]",
    "announce": "公告日",
    "effective": "生效日",
    "CN": "中国 A 股",
    "US": "美国",
    "数据集": "数据集",
    "文件": "文件",
    "来源": "来源",
    "市场范围": "市场范围",
    "起始日期": "起始日期",
    "结束日期": "结束日期",
    "行数": "行数",
    "股票数": "股票数",
    "事件数": "事件数",
    "备注": "备注",
    "样本层": "样本层",
    "事件相位窗口数": "事件相位窗口数",
    "观测值": "观测值",
    "说明": "说明",
    "分析层": "分析层",
    "样本基础": "样本基础",
    "主要输出": "主要输出",
    "证据状态": "证据状态",
    "当前口径": "当前口径",
}

VALUE_LABELS = {
    "announce": "公告日",
    "effective": "生效日",
    "CN": "中国 A 股",
    "US": "美国",
    "abnormal_return": "异常收益",
    "turnover": "换手率",
    "log_volume": "对数成交量",
    "main_car": "主回归 CAR",
    "turnover_mechanism": "换手率机制",
    "volume_mechanism": "成交量机制",
    "volatility_mechanism": "波动率机制",
    "const": "常数项",
    "inclusion": "是否纳入",
    "log_mkt_cap": "对数市值",
    "pre_event_return": "事件前收益",
    "car_m1_p1": "CAR[-1,+1]",
    "car_m3_p3": "CAR[-3,+3]",
    "car_m5_p5": "CAR[-5,+5]",
    "car_p0_p5": "CAR[0,+5]",
    "car_p0_p20": "CAR[0,+20]",
    "car_p0_p60": "CAR[0,+60]",
    "car_p0_p120": "CAR[0,+120]",
    "turnover_change": "换手率变化",
    "volume_change": "成交量变化",
    "volatility_change": "波动率变化",
}

PAPER_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{% if current and current.page_title %}{{ current.page_title }}{% elif current and current.title %}{{ current.title }}｜指数纳入效应研究界面{% else %}单篇文献讲义｜指数纳入效应研究界面{% endif %}</title>
  <style>
    :root {
      --bg: #f7f1e8;
      --paper: #fffaf2;
      --ink: #1f2a37;
      --muted: #5b6773;
      --line: rgba(31, 42, 55, 0.12);
      --accent: #005f73;
      --accent-2: #ae2012;
      --shadow: 0 24px 60px rgba(42, 36, 26, 0.08);
      --radius: 24px;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      padding: 0;
      max-width: 100%;
      overflow-x: hidden;
    }
    body {
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(0,95,115,0.08), transparent 26%),
        radial-gradient(circle at top right, rgba(174,32,18,0.08), transparent 24%),
        linear-gradient(180deg, #f9f4ed 0%, var(--bg) 100%);
    }
    a {
      color: var(--accent);
      text-decoration: none;
    }
    .page {
      width: min(1180px, 100%);
      margin: 28px auto 64px;
      padding: 0 16px;
    }
    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      margin-bottom: 18px;
      color: var(--muted);
      font-size: 14px;
      flex-wrap: wrap;
    }
    .topbar-nav {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
    }
    .hero {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 34px 36px 28px;
    }
    .hero-kicker {
      color: var(--accent-2);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      margin-bottom: 10px;
      font-weight: 700;
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(34px, 4vw, 54px);
      line-height: 1.02;
      letter-spacing: -0.03em;
    }
    .hero-subtitle {
      margin-top: 12px;
      font-size: 16px;
      line-height: 1.7;
      color: var(--muted);
    }
    .hero-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 20px;
    }
    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 10px 15px;
      border-radius: 999px;
      border: 1px solid var(--accent);
      font-size: 14px;
      font-weight: 700;
    }
    .btn.primary {
      background: var(--accent);
      color: white;
    }
    .btn.secondary {
      background: transparent;
      color: var(--accent);
    }
    .hero-summary {
      margin-top: 22px;
      padding-top: 18px;
      border-top: 1px solid rgba(31, 42, 55, 0.08);
      color: var(--muted);
      display: grid;
      gap: 10px;
      font-size: 15px;
      line-height: 1.75;
    }
    .hero-summary p {
      margin: 0;
      max-width: 72ch;
    }
    .hero-summary .summary-title {
      color: #1d2a37;
      font-size: 17px;
      font-weight: 700;
      line-height: 1.6;
    }
    .section {
      margin-top: 22px;
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 24px;
    }
    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 18px;
      align-items: end;
      margin-bottom: 16px;
      flex-wrap: wrap;
    }
    .section-kicker {
      color: #7a6d5a;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .section h2 {
      margin: 0;
      font-size: 28px;
      letter-spacing: -0.02em;
    }
    .section-intro {
      color: var(--muted);
      font-size: 15px;
      line-height: 1.7;
      max-width: 68ch;
    }
    .insight-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }
    .insight-card {
      padding: 18px;
      border-radius: 18px;
      border: 1px solid rgba(31, 42, 55, 0.08);
      background: linear-gradient(180deg, rgba(255,255,255,0.76), rgba(255,255,255,0.5));
    }
    .insight-card .kicker {
      color: #7b6344;
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 8px;
      font-weight: 700;
    }
    .insight-card h3 {
      margin: 0 0 8px;
      font-size: 19px;
      line-height: 1.35;
      color: #1d2a37;
    }
    .insight-meta {
      color: #7a6d5a;
      font-size: 13px;
      margin-bottom: 8px;
    }
    .insight-card p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.75;
    }
    .table-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }
    .table-card h3 {
      margin: 0 0 12px;
      font-size: 18px;
    }
    .table-wrap {
      width: 100%;
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: white;
    }
    table.dataframe {
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      font-size: 15px;
    }
    table.dataframe th, table.dataframe td {
      padding: 12px 14px;
      border-bottom: 1px solid #efe3d2;
      text-align: left;
      white-space: normal;
      word-break: break-word;
      overflow-wrap: anywhere;
      vertical-align: top;
      min-width: 92px;
    }
    table.dataframe thead th {
      background: #fbf7f1;
      position: sticky;
      top: 0;
      z-index: 1;
    }
    .note {
      margin-top: 14px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }
    .timeline-track {
      position: relative;
      margin-bottom: 22px;
    }
    .timeline-track::before {
      content: "";
      position: absolute;
      left: 12%;
      right: 12%;
      top: 34px;
      height: 2px;
      background: linear-gradient(90deg, rgba(0,95,115,0.18), rgba(174,32,18,0.22));
      z-index: 0;
    }
    .sequence-grid {
      position: relative;
      z-index: 1;
      display: grid;
      grid-template-columns: 1fr 1.15fr 1fr;
      gap: 16px;
    }
    .timeline-label-row {
      position: relative;
      z-index: 1;
      display: grid;
      grid-template-columns: 1fr 1.15fr 1fr;
      gap: 16px;
      margin-bottom: 14px;
    }
    .timeline-label {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 34px;
      padding: 6px 12px;
      border-radius: 999px;
      border: 1px solid rgba(31, 42, 55, 0.10);
      background: rgba(255, 255, 255, 0.84);
      color: #5f4d37;
      font-size: 12px;
      line-height: 1.2;
      font-weight: 700;
      text-align: center;
    }
    .timeline-label.current {
      color: var(--accent-2);
      border-color: rgba(174,32,18,0.18);
      background: rgba(174,32,18,0.06);
    }
    .sequence-card,
    .recommend-card {
      padding: 18px;
      border-radius: 18px;
      border: 1px solid rgba(31, 42, 55, 0.08);
      background: linear-gradient(180deg, rgba(255,255,255,0.78), rgba(255,255,255,0.54));
    }
    .sequence-card {
      position: relative;
      padding-top: 34px;
    }
    .sequence-card.current {
      border-color: rgba(0, 95, 115, 0.24);
      background: linear-gradient(180deg, rgba(0,95,115,0.08), rgba(255,255,255,0.78));
    }
    .sequence-card::before {
      content: "";
      position: absolute;
      top: 18px;
      left: 18px;
      width: 14px;
      height: 14px;
      border-radius: 999px;
      background: white;
      border: 3px solid rgba(0,95,115,0.38);
      box-shadow: 0 0 0 6px rgba(0,95,115,0.08);
    }
    .sequence-card.current::before {
      border-color: var(--accent-2);
      box-shadow: 0 0 0 7px rgba(174,32,18,0.10);
    }
    .sequence-card.current::after {
      content: "当前文献";
      position: absolute;
      top: 14px;
      right: 16px;
      font-size: 11px;
      letter-spacing: 0.08em;
      color: var(--accent-2);
      font-weight: 700;
      text-transform: uppercase;
    }
    .mini-kicker {
      color: #7b6344;
      font-size: 11px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 8px;
      font-weight: 700;
    }
    .sequence-card h3,
    .recommend-card h3 {
      margin: 0 0 8px;
      font-size: 18px;
      line-height: 1.4;
    }
    .mini-meta {
      color: #7a6d5a;
      font-size: 13px;
      margin-bottom: 8px;
      line-height: 1.6;
    }
    .pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 10px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      padding: 5px 10px;
      border-radius: 999px;
      border: 1px solid rgba(31, 42, 55, 0.10);
      background: rgba(255,255,255,0.82);
      color: #634b2f;
      font-size: 12px;
      line-height: 1;
      font-weight: 700;
      white-space: nowrap;
    }
    .pill.year {
      color: var(--accent);
      border-color: rgba(0,95,115,0.16);
      background: rgba(0,95,115,0.06);
    }
    .pill.camp {
      color: var(--accent-2);
      border-color: rgba(174,32,18,0.16);
      background: rgba(174,32,18,0.05);
    }
    .sequence-card p,
    .recommend-card p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.72;
    }
    .sequence-card a,
    .recommend-card a {
      display: inline-flex;
      margin-top: 12px;
      font-weight: 700;
    }
    .recommend-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }
    .evolution-nav {
      margin-top: 18px;
      padding-top: 18px;
      border-top: 1px solid rgba(31, 42, 55, 0.08);
    }
    .evolution-nav h3 {
      margin: 0 0 8px;
      font-size: 18px;
    }
    .evolution-nav-copy {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.72;
      margin-bottom: 14px;
      max-width: 72ch;
    }
    .evolution-switcher {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 16px;
    }
    .evolution-switcher button {
      appearance: none;
      border: 1px solid rgba(31, 42, 55, 0.12);
      background: rgba(255,255,255,0.78);
      color: #5f4d37;
      padding: 8px 14px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }
    .evolution-switcher button.active {
      background: rgba(0,95,115,0.09);
      color: var(--accent);
      border-color: rgba(0,95,115,0.22);
    }
    .evolution-view {
      display: none;
    }
    .evolution-view.active {
      display: block;
    }
    .evolution-group {
      margin-top: 18px;
    }
    .evolution-group:first-of-type {
      margin-top: 0;
    }
    .evolution-group-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }
    .evolution-group-head h4 {
      margin: 0;
      font-size: 17px;
      line-height: 1.4;
      color: #1d2a37;
    }
    .evolution-group-head .group-meta {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }
    .evolution-nav-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .evolution-link {
      display: block;
      padding: 14px;
      border-radius: 16px;
      border: 1px solid rgba(31, 42, 55, 0.08);
      background: linear-gradient(180deg, rgba(255,255,255,0.86), rgba(255,255,255,0.58));
      color: inherit;
      text-decoration: none;
      min-height: 138px;
    }
    .evolution-link.current {
      border-color: rgba(174,32,18,0.20);
      background: linear-gradient(180deg, rgba(174,32,18,0.07), rgba(255,255,255,0.76));
      box-shadow: inset 0 0 0 1px rgba(174,32,18,0.05);
    }
    .evolution-link .eyebrow {
      color: #7b6344;
      font-size: 11px;
      line-height: 1.4;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }
    .evolution-link h4 {
      margin: 0 0 10px;
      font-size: 16px;
      line-height: 1.4;
      color: #1d2a37;
    }
    .evolution-link .mini-meta {
      margin-bottom: 10px;
    }
    .evolution-link p {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    @media (max-width: 900px) {
      .timeline-track::before {
        display: none;
      }
      .page {
        margin: 18px auto 44px;
        padding: 0 12px;
      }
      .hero,
      .section {
        border-radius: 18px;
        padding: 20px;
      }
      .sequence-grid,
      .recommend-grid,
      .evolution-nav-grid,
      .insight-grid,
      .table-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="topbar">
      <div>单篇文献讲义</div>
      <div class="topbar-nav">
        <a href="{{ url_for('show_library') }}">返回文献库</a>
        <a href="{{ url_for('show_review') }}">文献综述</a>
        <a href="{{ url_for('show_framework') }}">研究框架</a>
        <a href="{{ url_for('home') }}">返回首页</a>
      </div>
    </div>

    <section class="hero">
      <div class="hero-kicker">{{ current.id }}</div>
      <h1>{{ current.title }}</h1>
      <div class="hero-subtitle">
        <div>{{ current.description }}</div>
        {% if current.subtitle %}
        <div>{{ current.subtitle }}</div>
        {% endif %}
      </div>
      <div class="hero-actions">
        {% for action in current.primary_actions or [] %}
        <a class="btn {% if loop.first %}primary{% else %}secondary{% endif %}" href="{{ action.href }}" {% if action.target %}target="{{ action.target }}"{% endif %}>{{ action.label }}</a>
        {% endfor %}
        <a class="btn secondary" href="{{ url_for('show_library') }}">返回文献库</a>
      </div>
      {% if current.summary_paragraphs %}
      <div class="hero-summary">
        {% for paragraph in current.summary_paragraphs %}
        <p class="{% if loop.first %}summary-title{% endif %}">{{ paragraph }}</p>
        {% endfor %}
      </div>
      {% elif current.summary_text %}
      <div class="hero-summary"><p>{{ current.summary_text }}</p></div>
      {% endif %}
    </section>

    {% if current.summary_cards %}
    <section class="section">
      <div class="section-head">
        <div>
          <div class="section-kicker">核心解读</div>
          <h2>这篇论文真正贡献了什么</h2>
        </div>
        <div class="section-intro">阅读顺序建议是：先看识别对象，再看它挑战了哪条旧假设，最后看这篇证据在整个指数效应争论中把问题往前推进了哪一步。</div>
      </div>
      <div class="insight-grid">
        {% for card in current.summary_cards %}
        <article class="insight-card">
          <div class="kicker">{{ card.kicker }}</div>
          <h3>{{ card.title }}</h3>
          {% if card.meta %}<div class="insight-meta">{{ card.meta }}</div>{% endif %}
          <p>{{ card["copy"] }}</p>
        </article>
        {% endfor %}
      </div>
    </section>
    {% endif %}

    {% if current.sequence_cards or current.recommended_cards %}
    <section class="section">
      <div class="section-head">
        <div>
          <div class="section-kicker">研究路径</div>
          <h2>这篇论文在 16 篇链条中的位置</h2>
        </div>
        <div class="section-intro">先看前后相邻文献把握它在整条争论中的位置，再根据推荐阅读判断下一步是继续追机制、追识别，还是回到中国市场语境。</div>
      </div>
      {% if current.sequence_cards %}
      <div class="timeline-track">
        <div class="timeline-label-row">
          {% for card in current.sequence_cards %}
          <div class="timeline-label {% if card.is_current %}current{% endif %}">{{ card.track_label }}</div>
          {% endfor %}
        </div>
        <div class="sequence-grid">
          {% for card in current.sequence_cards %}
          <article class="sequence-card {% if card.is_current %}current{% endif %}">
            <div class="mini-kicker">{{ card.kicker }}</div>
            <h3>{{ card.title }}</h3>
            {% if card.year_label or card.camp %}
            <div class="pill-row">
              {% if card.year_label %}<span class="pill year">{{ card.year_label }}</span>{% endif %}
              {% if card.camp %}<span class="pill camp">{{ card.camp }}</span>{% endif %}
            </div>
            {% endif %}
            {% if card.meta %}<div class="mini-meta">{{ card.meta }}</div>{% endif %}
            <p>{{ card["copy"] }}</p>
            {% if card.href %}
            <a href="{{ card.href }}">进入文献讲义</a>
            {% endif %}
          </article>
          {% endfor %}
        </div>
      </div>
      {% endif %}
      {% if current.recommended_cards %}
      <div class="recommend-grid">
        {% for card in current.recommended_cards %}
        <article class="recommend-card">
          <div class="mini-kicker">{{ card.kicker }}</div>
          <h3>{{ card.title }}</h3>
          {% if card.year_label or card.camp %}
          <div class="pill-row">
            {% if card.year_label %}<span class="pill year">{{ card.year_label }}</span>{% endif %}
            {% if card.camp %}<span class="pill camp">{{ card.camp }}</span>{% endif %}
          </div>
          {% endif %}
          {% if card.meta %}<div class="mini-meta">{{ card.meta }}</div>{% endif %}
          <p>{{ card["copy"] }}</p>
          {% if card.href %}
          <a href="{{ card.href }}">推荐继续阅读</a>
          {% endif %}
        </article>
        {% endfor %}
      </div>
      {% endif %}
      {% if current.evolution_nav_cards %}
      <div class="evolution-nav">
        <h3>完整文献演进导航</h3>
        <div class="evolution-nav-copy">以下 16 篇文献按阵营和时间排序。当前页可作为入口页使用，沿着这张导航可以直接跳转到任意一篇文献讲义。</div>
        <div class="evolution-switcher">
          <button class="active" type="button" data-view-target="camp">按阵营</button>
          <button type="button" data-view-target="track">按主线</button>
          <button type="button" data-view-target="stance">按立场</button>
        </div>
        {% for view in current.evolution_nav_views %}
        <div class="evolution-view {% if loop.first %}active{% endif %}" data-view-name="{{ view.id }}">
          {% for group in view.groups %}
          <section class="evolution-group">
            <div class="evolution-group-head">
              <h4>{{ group.title }}</h4>
              {% if group.meta %}<div class="group-meta">{{ group.meta }}</div>{% endif %}
            </div>
            <div class="evolution-nav-grid">
              {% for card in group.cards %}
              <a class="evolution-link {% if card.is_current %}current{% endif %}" href="{{ card.href }}">
                <div class="eyebrow">{{ card.kicker }}</div>
                <h4>{{ card.title }}</h4>
                <div class="pill-row">
                  <span class="pill year">{{ card.year_label }}</span>
                  <span class="pill camp">{{ card.camp }}</span>
                </div>
                <div class="mini-meta">{{ card.track_label }}</div>
                <p>{{ card["copy"] }}</p>
              </a>
              {% endfor %}
            </div>
          </section>
          {% endfor %}
        </div>
        {% endfor %}
      </div>
      {% endif %}
    </section>
    {% endif %}

    <section class="section">
      <div class="section-head">
        <div>
          <div class="section-kicker">结构化信息</div>
          <h2>论文信息与深度解读</h2>
        </div>
        <div class="section-intro">这里把适合展示与答辩引用的关键信息压缩成两张表：左边是论文基本信息，右边是当前项目中的深度解读。</div>
      </div>
      <div class="table-grid">
        {% for label, html_table in current.rendered_tables %}
        <div class="table-card">
          <h3>{{ label }}</h3>
          <div class="table-wrap">{{ html_table|safe }}</div>
        </div>
        {% endfor %}
      </div>
      <div class="note">如果需要进入原文，请使用上方“打开原文 PDF”按钮；当前页面的目的，是先把这篇论文在整条研究链中的位置讲清楚，而不是直接跳进扫描版原文。</div>
    </section>
  </div>
  <script>
    const viewButtons = document.querySelectorAll('[data-view-target]');
    const viewPanels = document.querySelectorAll('[data-view-name]');
    viewButtons.forEach((button) => {
      button.addEventListener('click', () => {
        const target = button.getAttribute('data-view-target');
        viewButtons.forEach((item) => item.classList.toggle('active', item === button));
        viewPanels.forEach((panel) => {
          panel.classList.toggle('active', panel.getAttribute('data-view-name') === target);
        });
      });
    });
  </script>
</body>
</html>
"""

HOME_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>指数纳入研究展板</title>
  <style>
    :root {
      --bg: #f5efe5;
      --bg-2: #fbf7f1;
      --paper: rgba(255, 251, 244, 0.78);
      --ink: #18212b;
      --muted: #5f6b78;
      --line: rgba(24, 33, 43, 0.12);
      --accent: #0f5c6e;
      --accent-2: #a63b28;
      --accent-3: #b88a2d;
      --shadow: 0 28px 80px rgba(33, 27, 18, 0.10);
      --radius: 24px;
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "PingFang SC", "Hiragino Sans GB", "Source Han Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at 12% 12%, rgba(15,92,110,0.14), transparent 26%),
        radial-gradient(circle at 88% 10%, rgba(166,59,40,0.12), transparent 22%),
        radial-gradient(circle at 82% 44%, rgba(184,138,45,0.10), transparent 20%),
        linear-gradient(180deg, #f8f2e9 0%, #f5efe5 42%, #f0e7d9 100%);
      overflow-x: hidden;
    }
    a { color: inherit; text-decoration: none; }
    .shell {
      width: min(1440px, 100%);
      margin: 0 auto;
      padding: 0 20px 64px;
    }
    .topbar {
      position: sticky;
      top: 0;
      z-index: 20;
      padding: 14px 20px;
      backdrop-filter: blur(18px);
      background: rgba(245, 239, 229, 0.80);
      border-bottom: 1px solid rgba(24, 33, 43, 0.08);
    }
    .topbar-inner {
      width: min(1440px, 100%);
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .brand {
      display: flex;
      gap: 14px;
      align-items: baseline;
      min-width: 0;
    }
    .brand-mark {
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: 27px;
      letter-spacing: -0.04em;
      line-height: 1;
    }
    .brand-copy {
      color: var(--muted);
      font-size: 13px;
      letter-spacing: 0.03em;
      white-space: nowrap;
    }
    .topbar-controls {
      display: grid;
      gap: 8px;
      justify-items: end;
      flex: 1;
      min-width: 0;
    }
    .nav {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .nav-sections,
    .nav-modes {
      align-items: center;
    }
    .nav-inline-label {
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-right: 2px;
    }
    .nav a, .nav button {
      border: 1px solid rgba(24, 33, 43, 0.08);
      background: rgba(255,255,255,0.46);
      color: var(--ink);
      padding: 9px 13px;
      border-radius: 999px;
      font-size: 12px;
      cursor: pointer;
      transition: transform 180ms ease, background 180ms ease, border-color 180ms ease;
    }
    .nav a:hover, .nav button:hover {
      transform: translateY(-1px);
      background: rgba(255,255,255,0.85);
      border-color: rgba(24, 33, 43, 0.14);
    }
    .nav a.active, .nav button.active {
      background: rgba(0,95,115,0.10);
      color: var(--accent);
      border-color: rgba(0,95,115,0.22);
      box-shadow: inset 0 0 0 1px rgba(0,95,115,0.04);
    }
    .nav form { margin: 0; }
    .hero {
      min-height: min(620px, calc(100svh - 92px));
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(320px, 0.9fr);
      align-items: start;
      gap: 28px;
      padding: 24px 0 4px;
    }
    .hero-copy {
      padding: 18px 0 12px;
    }
    .eyebrow {
      color: var(--accent-2);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.14em;
      margin-bottom: 14px;
    }
    .hero h1 {
      margin: 0;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: clamp(36px, 4.3vw, 56px);
      line-height: 1.08;
      letter-spacing: -0.055em;
      max-width: 15ch;
    }
    .hero-lead {
      max-width: 56ch;
      margin-top: 16px;
      font-size: clamp(15px, 1.45vw, 18px);
      line-height: 1.78;
      color: var(--muted);
    }
    .hero-rail-label {
      margin-top: 22px;
      color: #8d7450;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.12em;
    }
    .hero-rail {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-top: 10px;
      padding-top: 16px;
      border-top: 1px solid rgba(24,33,43,0.12);
    }
    .hero-step {
      padding-right: 12px;
    }
    .hero-step .index {
      display: block;
      color: #8d7450;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: 22px;
      line-height: 1;
      margin-bottom: 10px;
    }
    .hero-step strong {
      display: block;
      font-size: 15px;
      margin-bottom: 8px;
    }
    .hero-step span {
      display: block;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }
    .hero-notes {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      margin-top: 24px;
      max-width: 760px;
    }
    .hero-note {
      padding: 16px 18px;
      border-top: 1px solid rgba(24,33,43,0.12);
      background: linear-gradient(180deg, rgba(255,255,255,0.44), rgba(255,255,255,0.14));
    }
    .hero-note strong {
      display: block;
      font-size: 14px;
      margin-bottom: 8px;
    }
    .hero-note span {
      display: block;
      color: var(--muted);
      line-height: 1.6;
      font-size: 14px;
    }
    .hero-side {
      position: relative;
      min-height: 360px;
      border-radius: 34px;
      overflow: hidden;
      background:
        linear-gradient(160deg, rgba(15,92,110,0.92), rgba(10,58,76,0.94) 36%, rgba(24,33,43,0.98) 100%);
      box-shadow: var(--shadow);
      border: 1px solid rgba(255,255,255,0.18);
    }
    .hero-side::before {
      content: "";
      position: absolute;
      inset: 0;
      background:
        radial-gradient(circle at 16% 20%, rgba(255,255,255,0.18), transparent 24%),
        radial-gradient(circle at 70% 18%, rgba(255,255,255,0.14), transparent 18%),
        radial-gradient(circle at 74% 72%, rgba(184,138,45,0.30), transparent 24%),
        linear-gradient(135deg, transparent 0%, rgba(255,255,255,0.04) 100%);
      pointer-events: none;
    }
    .hero-side-inner {
      position: relative;
      height: 100%;
      padding: 24px;
      display: grid;
      gap: 18px;
      align-content: start;
    }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .kpi {
      padding: 18px;
      border-radius: 20px;
      background: rgba(255,255,255,0.10);
      border: 1px solid rgba(255,255,255,0.14);
      backdrop-filter: blur(8px);
    }
    .kpi .value {
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: clamp(28px, 4vw, 44px);
      line-height: 1;
      color: white;
      letter-spacing: -0.04em;
    }
    .kpi .label {
      margin-top: 8px;
      font-size: 13px;
      color: rgba(255,255,255,0.72);
      line-height: 1.5;
    }
    .hero-summary {
      padding: 20px 22px;
      border-radius: 22px;
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.14);
      color: rgba(255,255,255,0.82);
      line-height: 1.7;
      font-size: 15px;
    }
    .section {
      padding-top: 58px;
      scroll-margin-top: 92px;
    }
    .section-head {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(260px, 360px);
      gap: 24px;
      align-items: end;
      margin-bottom: 26px;
    }
    .section-kicker {
      color: var(--accent-2);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.12em;
      margin-bottom: 10px;
    }
    .section h2 {
      margin: 0;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: clamp(32px, 4vw, 54px);
      line-height: 1.02;
      letter-spacing: -0.05em;
    }
    .section-intro, .section-side {
      color: var(--muted);
      line-height: 1.75;
      font-size: 16px;
    }
    .section-side {
      padding-top: 12px;
      border-top: 1px solid rgba(24,33,43,0.10);
    }
    .abstract-panel {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(280px, 0.85fr);
      gap: 22px;
      padding: 22px 0 24px;
      border-top: 1px solid rgba(24,33,43,0.12);
      border-bottom: 1px solid rgba(24,33,43,0.12);
    }
    .abstract-lead {
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: clamp(22px, 2.1vw, 30px);
      line-height: 1.5;
      color: #203040;
      letter-spacing: -0.02em;
    }
    .abstract-points {
      display: grid;
      gap: 12px;
    }
    .abstract-point {
      padding-top: 12px;
      border-top: 1px solid rgba(24,33,43,0.08);
      color: var(--muted);
      font-size: 14px;
      line-height: 1.72;
    }
    .abstract-point strong {
      display: block;
      margin-bottom: 6px;
      color: #243545;
      font-size: 13px;
      letter-spacing: 0.08em;
    }
    .highlight-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 0;
      border-top: 1px solid rgba(24,33,43,0.12);
      border-bottom: 1px solid rgba(24,33,43,0.12);
      background: linear-gradient(180deg, rgba(255,255,255,0.34), rgba(255,255,255,0.14));
    }
    .highlight {
      min-height: 190px;
      padding: 24px 22px;
      display: grid;
      align-content: space-between;
    }
    .highlight + .highlight {
      border-left: 1px solid rgba(24,33,43,0.10);
    }
    .highlight .label {
      color: var(--accent);
      font-size: 12px;
      letter-spacing: 0.10em;
      font-weight: 700;
      margin-bottom: 12px;
    }
    .highlight .headline {
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: 28px;
      line-height: 1.08;
      letter-spacing: -0.04em;
      margin-bottom: 12px;
    }
    .highlight .copy {
      color: var(--muted);
      line-height: 1.68;
      font-size: 15px;
    }
    .track {
      display: grid;
      gap: 26px;
      padding: 32px 0 10px;
      border-top: 1px solid rgba(24,33,43,0.10);
    }
    .track-meta {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1.1fr);
      gap: 22px;
      align-items: start;
      padding-bottom: 16px;
      border-bottom: 1px solid rgba(24,33,43,0.08);
    }
    .track-title {
      margin: 0;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: clamp(26px, 3vw, 42px);
      line-height: 1.02;
      letter-spacing: -0.045em;
    }
    .track-index {
      display: block;
      margin-bottom: 8px;
      color: #8d7450;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.12em;
    }
    .track-subtitle {
      margin-top: 6px;
      color: #816f5b;
      font-size: 11px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }
    .track-summary {
      color: var(--muted);
      line-height: 1.72;
      font-size: 15px;
      max-width: 64ch;
      white-space: pre-wrap;
    }
    .track-takeaway {
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px solid rgba(24,33,43,0.08);
      color: #223546;
      font-size: 15px;
      line-height: 1.72;
    }
    .track-takeaway strong {
      color: #8d7450;
      font-size: 12px;
      letter-spacing: 0.12em;
      margin-right: 10px;
    }
    .support-band {
      display: grid;
      gap: 14px;
      margin-top: 22px;
    }
    .support-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 14px;
      padding-bottom: 8px;
      border-bottom: 1px solid rgba(24,33,43,0.08);
    }
    .support-head h4 {
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .support-head p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
      max-width: 54ch;
      text-align: right;
    }
    .support-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      gap: 16px;
    }
    .support-card {
      padding: 18px 18px 16px;
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(255,255,255,0.62), rgba(255,255,255,0.32));
      border: 1px solid rgba(24,33,43,0.08);
      display: grid;
      gap: 10px;
      align-content: start;
    }
    .support-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 12px;
    }
    .support-pill {
      padding: 5px 9px;
      border-radius: 999px;
      background: rgba(15,92,110,0.08);
      color: #215164;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.06em;
    }
    .support-citation {
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: 20px;
      line-height: 1.2;
      color: #1d2a37;
    }
    .support-role {
      font-size: 15px;
      line-height: 1.7;
      color: #243545;
    }
    .support-deep-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 14px;
    }
    .support-deep {
      padding: 12px 13px;
      border-radius: 14px;
      background: rgba(255,255,255,0.6);
      border: 1px solid rgba(24,33,43,0.06);
    }
    .support-deep.full {
      grid-column: 1 / -1;
    }
    .support-deep-label {
      color: #7b6344;
      font-size: 12px;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
      font-weight: 700;
    }
    .support-deep-copy {
      color: #2a3948;
      font-size: 14px;
      line-height: 1.72;
    }
    .support-detail {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 16px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }
    .support-detail div strong {
      color: #7b6344;
      font-size: 12px;
      letter-spacing: 0.08em;
      margin-right: 8px;
    }
    .support-link {
      display: inline-block;
      margin-top: 2px;
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
    }
    .result-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      margin-top: 18px;
      min-width: 0;
    }
    .insight-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin-top: 18px;
    }
    .insight-card {
      padding: 16px 16px 15px;
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(255,255,255,0.68), rgba(255,255,255,0.34));
      border: 1px solid rgba(24,33,43,0.08);
    }
    .insight-label {
      color: #7b6344;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
    }
    .insight-value {
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: 28px;
      line-height: 1.05;
      color: #1d2a37;
      margin-bottom: 8px;
      letter-spacing: -0.03em;
    }
    .insight-copy {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    .result-card {
      padding: 18px;
      border-radius: 22px;
      background: linear-gradient(180deg, rgba(255,255,255,0.56), rgba(255,255,255,0.26));
      border: 1px solid rgba(24,33,43,0.08);
      min-width: 0;
    }
    .result-card.wide {
      grid-column: 1 / -1;
    }
    .result-card .table-label {
      margin-bottom: 12px;
    }
    .result-figure img {
      width: 100%;
      display: block;
      border-radius: 18px;
      border: 1px solid rgba(24,33,43,0.08);
      background: white;
    }
    .result-figure .figure-caption {
      padding: 12px 2px 0;
    }
    .track-panels {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(320px, 0.85fr);
      gap: 24px;
      align-items: start;
      min-width: 0;
    }
    .surface {
      background: var(--paper);
      border: 1px solid rgba(24,33,43,0.08);
      border-radius: 24px;
      box-shadow: var(--shadow);
      overflow: hidden;
      min-width: 0;
    }
    .surface-pad {
      padding: 22px;
    }
    .surface-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 14px;
    }
    .surface-title {
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .surface-note {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    .stat-list {
      display: grid;
      gap: 12px;
    }
    .stat-row {
      display: grid;
      grid-template-columns: 140px 1fr;
      gap: 14px;
      padding-top: 12px;
      border-top: 1px solid rgba(24,33,43,0.08);
    }
    .stat-row:first-child { border-top: 0; padding-top: 0; }
    .stat-name {
      font-size: 12px;
      color: #7b6b58;
      letter-spacing: 0.10em;
      font-weight: 700;
    }
    .stat-copy {
      color: var(--muted);
      line-height: 1.7;
      font-size: 15px;
    }
    .figure-stack {
      display: grid;
      gap: 18px;
    }
    .figure-feature {
      background: rgba(255,255,255,0.62);
      border-bottom: 1px solid rgba(24,33,43,0.08);
    }
    .figure-feature img {
      width: 100%;
      display: block;
      aspect-ratio: 16 / 10;
      object-fit: cover;
      background: white;
    }
    .figure-caption {
      padding: 14px 18px 18px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }
    .surface-empty {
      min-height: 320px;
      display: grid;
      place-items: center;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.7;
      background: rgba(255,255,255,0.42);
    }
    .thumb-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      padding: 18px;
    }
    .thumb img {
      width: 100%;
      display: block;
      border-radius: 16px;
      border: 1px solid rgba(24,33,43,0.08);
      background: white;
    }
    .thumb div {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .table-shell {
      margin-top: 12px;
      border-top: 1px solid rgba(24,33,43,0.08);
      padding-top: 16px;
    }
    .table-block + .table-block { margin-top: 18px; }
    .table-label {
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 10px;
      color: #203040;
    }
    .table-wrap {
      overflow: auto;
      width: 100%;
      max-width: 100%;
      min-width: 0;
      background: rgba(255,255,255,0.74);
      border: 1px solid rgba(24,33,43,0.08);
      border-radius: 18px;
    }
    table.dataframe {
      width: max-content;
      min-width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    table.dataframe.compact-table {
      width: 100%;
      table-layout: fixed;
    }
    table.dataframe th, table.dataframe td {
      padding: 12px 14px;
      text-align: left;
      border-bottom: 1px solid rgba(24,33,43,0.08);
      vertical-align: top;
      white-space: normal;
      overflow-wrap: anywhere;
    }
    table.dataframe th {
      position: sticky;
      top: 0;
      background: #f8f2e9;
      z-index: 1;
    }
    table.dataframe.compact-table th,
    table.dataframe.compact-table td {
      font-size: 12.5px;
      line-height: 1.58;
      padding: 10px 11px;
    }
    .library-panels {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 18px;
      align-items: start;
      min-width: 0;
    }
    .section-summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
      margin-top: 18px;
      margin-bottom: 18px;
      min-width: 0;
    }
    .summary-card {
      padding: 18px 18px 16px;
      border-radius: 22px;
      background: linear-gradient(180deg, rgba(255,255,255,0.60), rgba(255,255,255,0.28));
      border: 1px solid rgba(24,33,43,0.08);
      min-width: 0;
      display: grid;
      gap: 10px;
      align-content: start;
    }
    .summary-kicker {
      color: #7b6344;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .summary-title {
      margin: 0;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: 24px;
      line-height: 1.16;
      color: #1d2a37;
    }
    .summary-meta {
      color: #215164;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.06em;
    }
    .summary-copy {
      color: #243545;
      font-size: 14px;
      line-height: 1.72;
    }
    .summary-foot {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
      border-top: 1px solid rgba(24,33,43,0.08);
      padding-top: 10px;
    }
    .cta-strip {
      margin-top: 36px;
      padding: 26px 28px;
      border-radius: 28px;
      color: white;
      background: linear-gradient(135deg, #0f5c6e 0%, #143544 50%, #a63b28 100%);
      box-shadow: var(--shadow);
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 20px;
      align-items: center;
    }
    .cta-strip h3 {
      margin: 0 0 10px;
      font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "Iowan Old Style", serif;
      font-size: clamp(28px, 3vw, 42px);
      line-height: 1.08;
      letter-spacing: -0.04em;
    }
    .cta-strip p {
      margin: 0;
      color: rgba(255,255,255,0.82);
      line-height: 1.7;
      font-size: 15px;
    }
    .cta-strip button {
      border: 1px solid rgba(255,255,255,0.18);
      background: rgba(255,255,255,0.12);
      color: white;
      padding: 14px 18px;
      border-radius: 999px;
      font-size: 14px;
      cursor: pointer;
    }
    @media (max-width: 1120px) {
      .hero, .track-meta, .track-panels, .section-head, .library-panels, .cta-strip, .abstract-panel, .support-grid, .result-grid, .insight-strip {
        grid-template-columns: 1fr;
      }
      .hero { min-height: auto; }
      .hero-side { min-height: 420px; }
      .highlight-grid { grid-template-columns: 1fr; }
      .highlight + .highlight { border-left: 0; border-top: 1px solid rgba(24,33,43,0.10); }
      .support-head { display: grid; }
      .support-head p { text-align: left; max-width: none; }
    }
    @media (max-width: 760px) {
      .shell { padding: 0 14px 44px; }
      .topbar { padding: 12px 14px; }
      .topbar-inner { display: grid; gap: 12px; }
      .brand { display: block; }
      .brand-copy { margin-top: 8px; white-space: normal; }
      .topbar-controls { justify-items: stretch; }
      .nav { justify-content: flex-start; }
      .hero-note, .kpi, .surface-pad { padding: 16px; }
      .hero-rail, .hero-notes, .kpi-grid, .thumb-grid { grid-template-columns: 1fr; }
      .support-detail { grid-template-columns: 1fr; }
      .support-deep-grid { grid-template-columns: 1fr; }
      .stat-row { grid-template-columns: 1fr; gap: 6px; }
    }
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div class="brand">
        <div class="brand-mark">指数纳入效应研究展板</div>
        <div class="brand-copy">文献脉络 · 样本证据 · 识别设计</div>
      </div>
      <div class="topbar-controls">
        <div class="nav nav-sections">
          {% for item in nav_sections %}
          <a
            class="{% if loop.first %}active{% endif %}"
            data-section-link
            data-section-key="{{ item.anchor }}"
            href="#{{ item.anchor }}"
          >{{ item.label }}</a>
          {% endfor %}
        </div>
        <div class="nav nav-modes">
          <span class="nav-inline-label">展示模式</span>
          {% for item in mode_tabs %}
          <a
            data-mode-link
            data-base-href="{{ item.base_href }}"
            data-default-hash="{{ item.default_hash }}"
            data-allowed-hashes="{{ item.allowed_hashes|join(',') }}"
            class="{% if item.active %}active{% endif %}"
            href="{{ item.href }}"
            {% if item.active %}aria-current="page"{% endif %}
          >{{ item.label }}</a>
          {% endfor %}
          <form method="post" action="{{ url_for('refresh_dashboard', mode=mode) }}">
            <input type="hidden" name="anchor" value="overview" data-anchor-input>
            <button type="submit">刷新数据</button>
          </form>
        </div>
      </div>
    </div>
  </div>
  <div class="shell">
    <section class="hero" id="overview">
      <div class="hero-copy">
        <div class="eyebrow">研究总览</div>
        <h1>16 篇文献、真实样本与识别设计：指数纳入效应的综合证据。</h1>
        <div class="hero-lead">
          本页将文献演进、真实样本、三条研究主线、关键图表与核心表格整合为一套连续的研究展示材料，用于回答指数纳入效应是否存在、如何解释以及如何识别。
        </div>
        <div class="hero-rail-label">研究路径</div>
        <div class="hero-rail">
          {% for section in track_sections %}
          <div class="hero-step">
            <span class="index">{{ "%02d"|format(loop.index) }}</span>
            <strong>{{ section.title }}</strong>
            <span>{{ section.display_summary }}</span>
          </div>
          {% endfor %}
        </div>
        <div class="hero-notes">
          {% for note in overview_notes %}
          <div class="hero-note">
            <strong>{{ note.title }}</strong>
            <span>{{ note["copy"] }}</span>
          </div>
          {% endfor %}
        </div>
      </div>
      <div class="hero-side">
        <div class="hero-side-inner">
          <div class="kpi-grid">
            {% for metric in overview_metrics %}
            <div class="kpi">
              <div class="value">{{ metric.value }}</div>
              <div class="label">{{ metric.label }}</div>
            </div>
            {% endfor %}
          </div>
          <div class="hero-summary">{{ overview_summary }}</div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <div>
          <div class="section-kicker">核心发现</div>
          <h2>核心研究结论</h2>
          <div class="section-intro">这一组内容优先呈现当前样本中最值得讨论的事实与识别含义，便于在开场阶段先建立问题意识和主要结论。</div>
        </div>
        <div class="section-side">结论综合自当前真实样本、三条研究主线结果与 16 篇文献框架，作用是帮助听众迅速把握“现象是否存在、机制如何解释、识别是否充分”这三层问题。</div>
      </div>
      <div class="abstract-panel">
        <div class="abstract-lead">{{ abstract_lead }}</div>
        <div class="abstract-points">
          {% for point in abstract_points %}
          <div class="abstract-point">
            <strong>{{ point.title }}</strong>
            <span>{{ point["copy"] }}</span>
          </div>
          {% endfor %}
        </div>
      </div>
      <div class="highlight-grid">
        {% for item in highlights %}
        <div class="highlight">
          <div>
            <div class="label">{{ item.label }}</div>
            <div class="headline">{{ item.headline }}</div>
          </div>
          <div class="copy">{{ item["copy"] }}</div>
        </div>
        {% endfor %}
      </div>
    </section>

    <section class="section" id="design">
      <div class="section-head">
        <div>
          <div class="section-kicker">样本与设计</div>
          <h2>先交代样本结构，再进入结果解释。</h2>
          <div class="section-intro">这一部分集中说明真实样本覆盖、事件窗口口径与回归结果的总体轮廓，让后续三条主线建立在更完整的研究设计之上。</div>
        </div>
        <div class="section-side">{{ design_section.summary }}</div>
      </div>
      {% if design_section.summary_cards %}
      <div class="section-summary-grid">
        {% for card in design_section.summary_cards %}
        <div class="summary-card">
          <div class="summary-kicker">{{ card.kicker }}</div>
          <h3 class="summary-title">{{ card.title }}</h3>
          {% if card.meta %}
          <div class="summary-meta">{{ card.meta }}</div>
          {% endif %}
          <div class="summary-copy">{{ card["copy"] }}</div>
          {% if card["foot"] %}
          <div class="summary-foot">{{ card["foot"] }}</div>
          {% endif %}
        </div>
        {% endfor %}
      </div>
      {% endif %}
      {% if mode != "brief" %}
      <div class="library-panels">
        {% for figure in design_section.figures %}
        <div class="result-card {{ figure.layout_class }}">
          <div class="table-label">{{ figure.label }}</div>
          <div class="result-figure">
            <img src="{{ url_for('serve_result_file', subpath=figure.path) }}" alt="{{ figure.caption }}">
            <div class="figure-caption">{{ figure.caption }}</div>
          </div>
        </div>
        {% endfor %}
        {% for table in design_section.tables %}
        <div class="result-card {{ table.layout_class }}">
          <div class="table-label">{{ table.label }}</div>
          <div class="table-wrap">{{ table.html|safe }}</div>
        </div>
        {% endfor %}
      </div>
      {% else %}
      <div class="section-side">3 分钟汇报模式下，这里只保留样本设计摘要卡；完整图表和数据来源表可切换到展示版或完整材料查看。</div>
      {% endif %}
    </section>

    <section class="section" id="tracks">
      <div class="section-head">
        <div>
          <div class="section-kicker">研究主线</div>
          <h2>三条主线，对应三类核心问题。</h2>
          <div class="section-intro">每一条主线都围绕一个明确问题展开，分别对应短期价格压力、长期保留与需求曲线、以及制度识别与中国市场证据。</div>
        </div>
        <div class="section-side">这部分构成研究展示的实证核心，对应短期冲击、长期保留与识别设计三类关键问题。</div>
      </div>
      {% for section in track_sections %}
      <div class="track" id="{{ section.anchor }}">
        <div class="track-meta">
          <div>
            <span class="track-index">{{ "%02d"|format(loop.index) }}</span>
            <h3 class="track-title">{{ section.title }}</h3>
            <div class="track-subtitle">{{ section.subtitle }}</div>
          </div>
          <div>
            <div class="track-summary">{{ section.display_summary }}</div>
            <div class="track-takeaway"><strong>结论句</strong>{{ section.takeaway }}</div>
          </div>
        </div>
        {% if section.result_cards %}
        <div class="insight-strip">
          {% for item in section.result_cards %}
          <div class="insight-card">
            <div class="insight-label">{{ item.label }}</div>
            <div class="insight-value">{{ item.value }}</div>
            <div class="insight-copy">{{ item["copy"] }}</div>
          </div>
          {% endfor %}
        </div>
        {% endif %}
        <div class="track-panels">
          <div class="surface">
            <div class="figure-stack">
              {% if section.display_figures %}
              <div class="figure-feature">
                <img src="{{ url_for('serve_result_file', subpath=section.display_figures[0].path) }}" alt="{{ section.display_figures[0].caption }}">
                <div class="figure-caption">{{ section.display_figures[0].caption }}</div>
              </div>
              {% if section.display_figures|length > 1 %}
              <div class="thumb-grid">
                {% for figure in section.display_figures[1:5] %}
                <div class="thumb">
                  <img src="{{ url_for('serve_result_file', subpath=figure.path) }}" alt="{{ figure.caption }}">
                  <div>{{ figure.caption }}</div>
                </div>
                {% endfor %}
              </div>
              {% endif %}
              {% else %}
              <div class="surface-empty">当前这条主线暂无可展示图表。</div>
              {% endif %}
            </div>
          </div>
          <div class="surface">
            <div class="surface-pad">
              <div class="surface-head">
                <h4 class="surface-title">阅读提示</h4>
                <div class="surface-note">{{ section.badge }}</div>
              </div>
              <div class="stat-list">
                {% for note in section.notes %}
                <div class="stat-row">
                  <div class="stat-name">{{ note.name }}</div>
                  <div class="stat-copy">{{ note["copy"] }}</div>
                </div>
                {% endfor %}
              </div>
            </div>
          </div>
        </div>
        {% if section.display_tables and mode != "brief" %}
        <div class="result-grid">
          {% for table in section.display_tables %}
          <div class="result-card {{ table.layout_class }}">
            <div class="table-label">{{ table.label }}</div>
            <div class="table-wrap">{{ table.html|safe }}</div>
          </div>
          {% endfor %}
        </div>
        {% endif %}
        {% if section.display_support_papers and mode != "brief" %}
        <div class="support-band">
          <div class="support-head">
            <h4>支撑文献</h4>
            <p>这一组文献不是简单并列引用，而是为当前主线分别提供问题意识、机制解释与识别支撑。</p>
          </div>
          <div class="support-grid">
            {% for paper in section.display_support_papers %}
            <div class="support-card">
              <div class="support-meta">
                <span class="support-pill">{{ paper.camp }}</span>
                <span class="support-pill">{{ paper.stance }}</span>
                <span class="support-pill">{{ paper.market_focus }}</span>
              </div>
              <div class="support-citation">{{ paper.citation }}</div>
              <div class="support-role">{{ paper.one_line_role }}</div>
              <div class="support-deep-grid">
                <div class="support-deep">
                  <div class="support-deep-label">识别对象</div>
                  <div class="support-deep-copy">{{ paper.identification_target }}</div>
                </div>
                <div class="support-deep">
                  <div class="support-deep-label">挑战的假设</div>
                  <div class="support-deep-copy">{{ paper.challenged_assumption }}</div>
                </div>
                <div class="support-deep full">
                  <div class="support-deep-label">争论推进</div>
                  <div class="support-deep-copy">{{ paper.deep_contribution }}</div>
                </div>
              </div>
              <div class="support-detail">
                <div><strong>方法</strong>{{ paper.method_focus }}</div>
                <div><strong>作用</strong>{{ paper.practical_use }}</div>
              </div>
              {% if paper.pdf_href %}
              <a class="support-link" href="{{ paper.pdf_href }}" target="_blank">文献讲义</a>
              {% endif %}
            </div>
            {% endfor %}
          </div>
        </div>
        {% endif %}
      </div>
      {% endfor %}
    </section>

    {% if mode != "brief" %}
    <section class="section" id="framework">
      <div class="section-head">
        <div>
          <div class="section-kicker">文献框架</div>
          <h2>16 篇文献在同一条研究链条中的位置。</h2>
          <div class="section-intro">这里集中呈现五大阵营、演进总表和表达框架，重点不是逐篇罗列，而是说明每篇文献在争论史中承担的角色。</div>
        </div>
        <div class="section-side">{{ framework_section.display_summary }}</div>
      </div>
      {% if framework_section.summary_cards %}
      <div class="section-summary-grid">
        {% for card in framework_section.summary_cards %}
        <div class="summary-card">
          <div class="summary-kicker">{{ card.kicker }}</div>
          <h3 class="summary-title">{{ card.title }}</h3>
          {% if card.meta %}
          <div class="summary-meta">{{ card.meta }}</div>
          {% endif %}
          <div class="summary-copy">{{ card["copy"] }}</div>
          {% if card["foot"] %}
          <div class="summary-foot">{{ card["foot"] }}</div>
          {% endif %}
        </div>
        {% endfor %}
      </div>
      {% endif %}
      <div class="library-panels">
        {% for table in framework_section.display_tables %}
        <div class="result-card {{ table.layout_class }}">
          <div class="table-label">{{ table.label }}</div>
          <div class="table-wrap">{{ table.html|safe }}</div>
        </div>
        {% endfor %}
      </div>
    </section>

    <section class="section" id="supplement">
      <div class="section-head">
        <div>
          <div class="section-kicker">机制补充</div>
          <h2>把事件研究结果放回交易机制与执行场景。</h2>
          <div class="section-intro">这一部分用于补充事件时钟、机制链与冲击估算，帮助把统计结果转换成更完整的市场解释。</div>
        </div>
        <div class="section-side">{{ supplement_section.display_summary }}</div>
      </div>
      {% if supplement_section.summary_cards %}
      <div class="section-summary-grid">
        {% for card in supplement_section.summary_cards %}
        <div class="summary-card">
          <div class="summary-kicker">{{ card.kicker }}</div>
          <h3 class="summary-title">{{ card.title }}</h3>
          {% if card.meta %}
          <div class="summary-meta">{{ card.meta }}</div>
          {% endif %}
          <div class="summary-copy">{{ card["copy"] }}</div>
          {% if card["foot"] %}
          <div class="summary-foot">{{ card["foot"] }}</div>
          {% endif %}
        </div>
        {% endfor %}
      </div>
      {% endif %}
      <div class="library-panels">
        {% for table in supplement_section.display_tables %}
        <div class="result-card {{ table.layout_class }}">
          <div class="table-label">{{ table.label }}</div>
          <div class="table-wrap">{{ table.html|safe }}</div>
        </div>
        {% endfor %}
      </div>
    </section>
    {% endif %}

    <section class="section" id="limits">
      <div class="section-head">
        <div>
          <div class="section-kicker">研究边界</div>
          <h2>把结论放回样本期、识别范围与数据口径中理解。</h2>
          <div class="section-intro">这一部分不用于削弱结果，而是明确这套研究在样本、方法与数据来源上的适用范围，使结论表达更加稳健。</div>
        </div>
        <div class="section-side">{{ limits_section.summary }}</div>
      </div>
      {% if limits_section.summary_cards %}
      <div class="section-summary-grid">
        {% for card in limits_section.summary_cards %}
        <div class="summary-card">
          <div class="summary-kicker">{{ card.kicker }}</div>
          <h3 class="summary-title">{{ card.title }}</h3>
          {% if card.meta %}
          <div class="summary-meta">{{ card.meta }}</div>
          {% endif %}
          <div class="summary-copy">{{ card["copy"] }}</div>
          {% if card["foot"] %}
          <div class="summary-foot">{{ card["foot"] }}</div>
          {% endif %}
        </div>
        {% endfor %}
      </div>
      {% endif %}
      {% if mode != "brief" %}
      <div class="library-panels">
        {% for table in limits_section.tables %}
        <div class="result-card {{ table.layout_class }}">
          <div class="table-label">{{ table.label }}</div>
          <div class="table-wrap">{{ table.html|safe }}</div>
        </div>
        {% endfor %}
      </div>
      {% endif %}
    </section>

    <section class="cta-strip">
      <div>
        <h3>同一界面中的文献、样本与结果。</h3>
        <p>{{ cta_copy }}</p>
      </div>
      <form method="post" action="{{ url_for('refresh_dashboard', mode=mode) }}">
        <input type="hidden" name="anchor" value="overview" data-anchor-input>
        <button type="submit">刷新全部材料</button>
      </form>
    </section>
  </div>
  <script>
    (() => {
      const sectionLinks = Array.from(document.querySelectorAll("[data-section-link]"));
      const modeLinks = Array.from(document.querySelectorAll("[data-mode-link]"));
      const anchorInputs = Array.from(document.querySelectorAll("[data-anchor-input]"));
      const aliasMap = new Map([
        ["#price_pressure_track", "#tracks"],
        ["#demand_curve_track", "#tracks"],
        ["#identification_china_track", "#tracks"],
      ]);

      function currentHash() {
        return window.location.hash || "#overview";
      }

      function currentSectionHash() {
        const hash = currentHash();
        return aliasMap.get(hash) || hash;
      }

      function syncTopbarState() {
        const activeModeLink =
          modeLinks.find((link) => link.classList.contains("active")) ||
          modeLinks[0];
        const activeAllowedHashes = ((activeModeLink && activeModeLink.dataset.allowedHashes) || "")
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean);
        const activeDefaultHash = (activeModeLink && activeModeLink.dataset.defaultHash) || "#overview";
        const rawHash = currentHash();
        let hash = rawHash;
        if (activeAllowedHashes.length && !activeAllowedHashes.includes(hash)) {
          hash = ["#framework", "#supplement"].includes(hash) && activeAllowedHashes.includes("#tracks")
            ? "#tracks"
            : activeDefaultHash;
        }
        if (hash !== rawHash) {
          history.replaceState(null, "", `${window.location.pathname}${window.location.search}${hash}`);
          const target = document.querySelector(hash);
          if (target) {
            requestAnimationFrame(() => {
              target.scrollIntoView({ block: "start" });
            });
          }
        }

        sectionLinks.forEach((link) => {
          const linkHash = link.getAttribute("href");
          const normalizedSectionHash = aliasMap.get(hash) || hash;
          link.classList.toggle("active", linkHash === normalizedSectionHash);
        });

        modeLinks.forEach((link) => {
          const baseHref = link.dataset.baseHref || "/";
          const allowedHashes = (link.dataset.allowedHashes || "")
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean);
          const defaultHash = link.dataset.defaultHash || "#overview";
          let nextHash = hash;
          if (allowedHashes.length && !allowedHashes.includes(hash)) {
            nextHash = ["#framework", "#supplement"].includes(hash) && allowedHashes.includes("#tracks")
              ? "#tracks"
              : defaultHash;
          }
          link.setAttribute("href", `${baseHref}${nextHash}`);
        });

        anchorInputs.forEach((input) => {
          input.value = hash.replace(/^#/, "") || "overview";
        });
      }

      syncTopbarState();
      window.addEventListener("hashchange", syncTopbarState);
    })();
  </script>
</body>
</html>
"""


app = Flask(__name__)


@app.get("/favicon.ico")
def favicon():
    return ("", 204)


def _safe_relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def _render_table(frame, compact: bool = False) -> str:
    display = frame.copy()
    classes = ["dataframe"]
    if compact:
        classes.append("compact-table")
    if len(display) > 120:
        display = display.head(120)
    display = display.rename(columns={column: COLUMN_LABELS.get(column, column) for column in display.columns})
    for column in display.columns:
        if display[column].dtype == object:
            display[column] = display[column].replace(VALUE_LABELS)
    return display.to_html(
        index=False,
        classes=classes,
        border=0,
        justify="left",
        escape=False,
        float_format=lambda v: f"{v:0.4f}",
    )


def _translate_label(label: str) -> str:
    return TABLE_LABELS.get(label, label)


def _format_figure_caption(path: Path) -> str:
    stem = path.stem
    if stem == "average_abnormal_returns":
        return "平均异常收益路径图"
    if stem.endswith("_rdd_bins"):
        outcome = stem.removesuffix("_rdd_bins")
        outcome_label = COLUMN_LABELS.get(outcome, outcome)
        return f"{outcome_label} 分箱图"
    if stem.endswith("_rdd_main"):
        outcome = stem.removesuffix("_rdd_main")
        outcome_label = COLUMN_LABELS.get(outcome, outcome)
        return f"{outcome_label} 断点回归主图"
    if stem.endswith("_car_path"):
        parts = stem.split("_")
        if len(parts) >= 3:
            market = COLUMN_LABELS.get(parts[0].upper(), parts[0].upper())
            phase = COLUMN_LABELS.get(parts[1], parts[1])
            return f"{market}{phase}平均异常收益路径图"
    return stem.replace("_", " ")


def _build_figure_caption(path: Path, custom_caption: str | None = None, prefix: str | None = None) -> str:
    stem = path.stem
    if custom_caption:
        caption = custom_caption
    elif stem.endswith("_car_path"):
        parts = stem.split("_")
        market = COLUMN_LABELS.get(parts[0].upper(), parts[0].upper()) if len(parts) >= 1 else "样本"
        phase = COLUMN_LABELS.get(parts[1], parts[1]) if len(parts) >= 2 else "事件阶段"
        caption = f"图意：展示 {market}{phase} 的累计异常收益路径。阅读重点：比较事件日前后的斜率变化，以及 0 日之后价格是否继续累积或出现回吐。"
    elif stem.endswith("_rdd_bins"):
        outcome = stem.removesuffix("_rdd_bins")
        outcome_label = COLUMN_LABELS.get(outcome, outcome)
        caption = f"图意：展示 {outcome_label} 在断点两侧的分箱均值。阅读重点：观察 0 附近是否出现明显跳跃，以及左右两侧样本均值是否系统分离。"
    elif stem.endswith("_rdd_main"):
        outcome = stem.removesuffix("_rdd_main")
        outcome_label = COLUMN_LABELS.get(outcome, outcome)
        caption = f"中国样本 RDD 主图。图意：展示 {outcome_label} 的断点回归主图。阅读重点：同时观察断点两侧的分箱均值与拟合线，在 0 附近判断是否存在结构性跳跃。"
    elif stem == "sample_event_timeline":
        caption = "图意：展示真实纳入事件在时间轴上的分布。阅读重点：判断样本是否集中在少数批次，以及公告日和生效日是否在时间上形成清晰分层。"
    elif stem == "sample_car_heatmap":
        caption = "图意：把短窗口 CAR 在市场与事件阶段两个维度上压缩成一张总览图。阅读重点：优先比较美国公告日与中国生效日所在单元格的方向、幅度和显著性。"
    elif stem == "main_regression_coefficients":
        caption = "图意：展示主回归中纳入变量的估计系数与置信区间。阅读重点：比较不同市场、不同事件阶段下系数的方向与显著性，而不只看点估计大小。"
    elif stem == "mechanism_regression_coefficients":
        caption = "图意：展示机制回归中纳入变量对换手率、成交量和波动率的影响。阅读重点：比较中国 A 股与美国在公告日、生效日的机制方向是否一致。"
    elif stem == "match_diagnostics_overview":
        caption = "图意：同时展示匹配状态分布与匹配质量指标。阅读重点：先看匹配成功率，再看三对照构造和行业口径放宽占比，以判断对照组设计的稳定性。"
    else:
        caption = _format_figure_caption(path)
    if prefix:
        return f"{prefix}：{caption}"
    return caption


def _normalize_result(raw: dict[str, object]) -> dict[str, object]:
    summary_path = raw.get("summary_path")
    summary_text = raw.get("summary_text", "") if isinstance(raw.get("summary_text"), str) else ""
    if isinstance(summary_path, Path) and summary_path.exists():
        summary_text = summary_path.read_text(encoding="utf-8")
    tables = []
    for label, frame in raw.get("tables", {}).items():
        if frame is None:
            continue
        tables.append((_translate_label(label), _render_table(frame)))
    figure_paths = []
    for item in raw.get("figures", []):
        if isinstance(item, Path):
            figure_paths.append(
                {
                    "path": _safe_relative(item),
                    "caption": _build_figure_caption(item),
                }
            )
        elif isinstance(item, dict) and isinstance(item.get("path"), Path):
            figure_paths.append(
                {
                    "path": _safe_relative(item["path"]),
                    "caption": _build_figure_caption(item["path"], custom_caption=item.get("caption"), prefix=item.get("prefix")),
                }
            )
    output_dir = raw.get("output_dir")
    return {
        "id": raw.get("id"),
        "title": raw.get("title"),
        "description": raw.get("description", ""),
        "subtitle": raw.get("subtitle", ""),
        "summary_text": summary_text,
        "rendered_tables": tables,
        "figure_paths": figure_paths,
        "output_dir": _safe_relative(output_dir) if isinstance(output_dir, Path) else output_dir,
    }


def _attach_project_track_context(current: dict[str, object], config: dict[str, object]) -> dict[str, object]:
    project_module = config.get("project_module")
    if not project_module:
        return current
    track_summary = build_project_track_markdown(project_module).strip()
    if current.get("summary_text"):
        current["summary_text"] = f"{track_summary}\n\n---\n\n{current['summary_text']}"
    else:
        current["summary_text"] = track_summary
    support_table = ("支撑文献", _render_table(build_project_track_frame(project_module), compact=True))
    current["rendered_tables"] = [support_table, *current.get("rendered_tables", [])]
    current["support_papers"] = build_project_track_support_records(project_module)
    return current


def _load_saved_tables(output_dir: Path) -> list[tuple[str, str]]:
    csv_files = sorted(output_dir.rglob("*.csv"))
    tables: list[tuple[str, str]] = []
    seen: set[str] = set()
    preferred_order = [
        "event_study_summary.csv",
        "mechanism_summary.csv",
        "retention_summary.csv",
        "did_summary.csv",
        "regression_coefficients.csv",
        "regression_models.csv",
        "match_diagnostics.csv",
        "rdd_summary.csv",
        "event_level_with_running.csv",
    ]
    ordered_files: list[Path] = []
    for filename in preferred_order:
        ordered_files.extend(path for path in csv_files if path.name == filename)
    ordered_files.extend(path for path in csv_files if path not in ordered_files)
    for path in ordered_files:
        key = path.stem
        if key in seen:
            continue
        seen.add(key)
        try:
            import pandas as pd

            frame = pd.read_csv(path)
        except Exception:
            continue
        tables.append((_translate_label(key), _render_table(frame)))
        if len(tables) >= 6:
            break
    return tables


def _load_single_csv(output_dir: Path, filename: str):
    try:
        import pandas as pd
    except Exception:
        return None
    path = next(output_dir.rglob(filename), None)
    if path is None:
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _format_pct(value: float) -> str:
    return f"{value:.2%}"


def _format_p_value(value: float) -> str:
    return f"p={value:.3f}"


def _format_share(value: float) -> str:
    return f"{value:.1%}"


def _table_layout_for_label(label: str) -> str:
    wide_labels = {
        "长短窗口 CAR 对比",
        "文献演进总表",
        "事件时钟",
        "机制链",
        "A 股与美股并列总结",
        "样本窗口口径",
        "样本范围总表",
        "数据来源与口径",
        "识别范围说明",
    }
    return "wide" if label in wide_labels else ""


def _decorate_display_tables(tables: list[tuple[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "label": label,
            "html": html_table,
            "layout_class": _table_layout_for_label(label),
        }
        for label, html_table in tables
    ]


def _build_framework_summary_cards() -> list[dict[str, str]]:
    frame = build_camp_summary_frame()
    cards: list[dict[str, str]] = []
    for row in frame.to_dict("records"):
        cards.append(
            {
                "kicker": "文献阵营",
                "title": str(row["阵营"]),
                "meta": f'{row["副标题"]} · {int(row["文献数量"])} 篇',
                "copy": str(row["核心问题"]),
                "foot": "这一阵营中的文献围绕同一类问题展开，可作为对应研究争论的集中概括。",
            }
        )
    return cards


def _build_review_summary_cards() -> list[dict[str, str]]:
    return [
        {
            "kicker": "反方文献",
            "title": "先怀疑永久效应",
            "meta": "价格压力、动量错觉与效应弱化",
            "copy": "先看反方文献如何把早期上涨重新解释为短期流动性冲击、纳入前强势表现或现代市场中的提前定价。",
        },
        {
            "kicker": "中性文献",
            "title": "再看争论如何转向识别",
            "meta": "制度差异、套利约束与价格发现",
            "copy": "中性文献的重要性不在于表态，而在于说明结论会随着指数制度、市场摩擦和识别设计而改变。",
        },
        {
            "kicker": "正方文献",
            "title": "最后看哪些机制仍然成立",
            "meta": "需求曲线、长期保留与中国证据",
            "copy": "正方文献并不只是重复“会涨”，而是在更强设计下继续保留部分永久性、信息背书和中国市场不对称证据。",
        },
    ]


def _build_supplement_summary_cards() -> list[dict[str, str]]:
    event = build_event_clock_frame().iloc[1]
    mechanism = build_mechanism_chain_frame().iloc[0]
    impact = build_impact_formula_frame().iloc[2]
    playbook = build_case_playbook_frame().iloc[1]
    return [
        {
            "kicker": "事件时钟",
            "title": str(event["阶段"]),
            "meta": "先分清公告、生效与再平衡",
            "copy": str(event["对应观察指标"]),
            "foot": str(event["最容易犯的误判"]),
        },
        {
            "kicker": "机制链",
            "title": str(mechanism["机制环节"]),
            "meta": str(mechanism["学术对应"]),
            "copy": str(mechanism["交易台语言"]),
            "foot": f'对应变量：{mechanism["项目变量"]}',
        },
        {
            "kicker": "冲击估算",
            "title": str(impact["步骤"]),
            "meta": str(impact["公式/规则"]),
            "copy": str(impact["作用"]),
            "foot": "这一步用于把“指数纳入”转化为交易拥挤度与冲击强弱的估计。",
        },
        {
            "kicker": "表达场景",
            "title": str(playbook["场景"]),
            "meta": str(playbook["对应页面"]),
            "copy": str(playbook["核心表述"]),
            "foot": "这类表述可直接用于研究展示、论文讨论与投研交流。",
        },
    ]


def _build_price_pressure_cards() -> list[dict[str, str]]:
    output_dir = ROOT / "results" / "literature" / "harris_gurel"
    event = _load_single_csv(output_dir, "event_study_summary.csv")
    mechanism = _load_single_csv(output_dir, "mechanism_summary.csv")
    cards: list[dict[str, str]] = []
    if event is not None:
        us_announce = event.loc[(event["market"] == "US") & (event["event_phase"] == "announce") & (event["window_slug"] == "m1_p1")]
        cn_effective = event.loc[(event["market"] == "CN") & (event["event_phase"] == "effective") & (event["window_slug"] == "m1_p1")]
        if not us_announce.empty:
            row = us_announce.iloc[0]
            cards.append({"label": "美股公告日 CAR[-1,+1]", "value": _format_pct(float(row["mean_car"])), "copy": _format_p_value(float(row["p_value"]))})
        if not cn_effective.empty:
            row = cn_effective.iloc[0]
            cards.append({"label": "A 股生效日 CAR[-1,+1]", "value": _format_pct(float(row["mean_car"])), "copy": _format_p_value(float(row["p_value"]))})
    if mechanism is not None:
        us_announce_mech = mechanism.loc[(mechanism["market"] == "US") & (mechanism["event_phase"] == "announce")]
        cn_effective_mech = mechanism.loc[(mechanism["market"] == "CN") & (mechanism["event_phase"] == "effective")]
        if not us_announce_mech.empty:
            row = us_announce_mech.iloc[0]
            cards.append({"label": "美股公告日成交量变化", "value": _format_pct(float(row["mean_volume_change"])), "copy": "反映短期建仓冲击强度"})
        if not cn_effective_mech.empty:
            row = cn_effective_mech.iloc[0]
            cards.append({"label": "A 股生效日成交量变化", "value": _format_pct(float(row["mean_volume_change"])), "copy": "用于观察中国样本的调仓压力"})
    return cards[:4]


def _build_demand_curve_cards() -> list[dict[str, str]]:
    output_dir = ROOT / "results" / "literature" / "shleifer"
    event = _load_single_csv(output_dir, "event_study_summary.csv")
    retention = _load_single_csv(output_dir, "retention_summary.csv")
    cards: list[dict[str, str]] = []
    if retention is not None:
        us_announce = retention.loc[(retention["market"] == "US") & (retention["event_phase"] == "announce")]
        cn_effective = retention.loc[(retention["market"] == "CN") & (retention["event_phase"] == "effective")]
        if not us_announce.empty:
            row = us_announce.iloc[0]
            cards.append({"label": "美股公告日保留率", "value": f"{float(row['retention_ratio']):.2f}", "copy": "大于 1 表示长窗口效应仍在累积"})
        if not cn_effective.empty:
            row = cn_effective.iloc[0]
            cards.append({"label": "A 股生效日保留率", "value": f"{float(row['retention_ratio']):.2f}", "copy": "用于比较中国样本的长期保留程度"})
    if event is not None:
        us_long = event.loc[(event["market"] == "US") & (event["event_phase"] == "announce") & (event["window_slug"] == "p0_p120")]
        cn_long = event.loc[(event["market"] == "CN") & (event["event_phase"] == "announce") & (event["window_slug"] == "p0_p120")]
        if not us_long.empty:
            row = us_long.iloc[0]
            cards.append({"label": "美股公告日 CAR[0,+120]", "value": _format_pct(float(row["mean_car"])), "copy": _format_p_value(float(row["p_value"]))})
        if not cn_long.empty:
            row = cn_long.iloc[0]
            cards.append({"label": "A 股公告日 CAR[0,+120]", "value": _format_pct(float(row["mean_car"])), "copy": _format_p_value(float(row["p_value"]))})
    return cards[:4]


def _build_identification_cards() -> list[dict[str, str]]:
    style_dir = ROOT / "results" / "literature" / "hs300_style"
    rdd_dir = ROOT / "results" / "literature" / "hs300_rdd"
    event = _load_single_csv(style_dir, "event_study_summary.csv")
    did = _load_single_csv(style_dir, "did_summary.csv")
    regression = _load_single_csv(style_dir, "regression_coefficients.csv")
    rdd = _load_single_csv(rdd_dir, "rdd_summary.csv")
    cards: list[dict[str, str]] = []
    if event is not None:
        announce = event.loc[(event["event_phase"] == "announce") & (event["window_slug"] == "m1_p1")]
        if not announce.empty:
            row = announce.iloc[0]
            cards.append({"label": "中国样本公告日 CAR[-1,+1]", "value": _format_pct(float(row["mean_car"])), "copy": _format_p_value(float(row["p_value"]))})
    if did is not None:
        ar = did.loc[(did["event_phase"] == "announce") & (did["metric"] == "abnormal_return")]
        if not ar.empty:
            row = ar.iloc[0]
            cards.append({"label": "DID 异常收益估计", "value": _format_pct(float(row["did_estimate"])), "copy": f"处理组 {int(row['n_treated'])} / 对照组 {int(row['n_control'])}"})
    if regression is not None:
        inc = regression.loc[(regression["parameter"] == "inclusion") & (regression["specification"] == "main_car") & (regression["event_phase"] == "announce")]
        if not inc.empty:
            row = inc.iloc[0]
            cards.append({"label": "匹配回归纳入系数", "value": f"{float(row['coefficient']):.4f}", "copy": _format_p_value(float(row["p_value"]))})
    if rdd is not None:
        tau = rdd.loc[rdd["outcome"] == "car_m1_p1"]
        if not tau.empty:
            row = tau.iloc[0]
            cards.append({"label": "RDD 断点效应", "value": f"{float(row['tau']):.4f}", "copy": _format_p_value(float(row["p_value"]))})
    return cards[:4]


def _build_price_pressure_tables() -> list[tuple[str, str]]:
    output_dir = ROOT / "results" / "literature" / "harris_gurel"
    tables: list[tuple[str, str]] = []
    event = _load_single_csv(output_dir, "event_study_summary.csv")
    if event is not None:
        focus = event.loc[event["window_slug"].isin(["m1_p1", "m3_p3"]), ["market", "event_phase", "window", "mean_car", "p_value", "n_events"]]
        tables.append(("短窗口 CAR 摘要", _render_table(focus, compact=True)))
    mechanism = _load_single_csv(output_dir, "mechanism_summary.csv")
    if mechanism is not None:
        focus = mechanism.loc[:, ["market", "event_phase", "mean_turnover_change", "mean_volume_change", "mean_volatility_change", "n_events"]]
        tables.append(("机制变量变化", _render_table(focus, compact=True)))
    return tables


def _build_demand_curve_tables() -> list[tuple[str, str]]:
    output_dir = ROOT / "results" / "literature" / "shleifer"
    tables: list[tuple[str, str]] = []
    event = _load_single_csv(output_dir, "event_study_summary.csv")
    if event is not None:
        focus = event.loc[event["window_slug"].isin(["m1_p1", "p0_p20", "p0_p120"]), ["market", "event_phase", "window", "mean_car", "p_value", "n_events"]]
        tables.append(("长短窗口 CAR 对比", _render_table(focus, compact=True)))
    retention = _load_single_csv(output_dir, "retention_summary.csv")
    if retention is not None:
        focus = retention.loc[:, ["market", "event_phase", "short_mean_car", "long_mean_car", "car_reversal", "retention_ratio"]]
        tables.append(("保留率与回吐", _render_table(focus, compact=True)))
    return tables


def _build_identification_tables() -> list[tuple[str, str]]:
    style_dir = ROOT / "results" / "literature" / "hs300_style"
    rdd_dir = ROOT / "results" / "literature" / "hs300_rdd"
    tables: list[tuple[str, str]] = []
    event = _load_single_csv(style_dir, "event_study_summary.csv")
    if event is not None:
        focus = event.loc[event["window_slug"].isin(["m1_p1", "m3_p3"]), ["event_phase", "window", "mean_car", "p_value", "n_events"]]
        tables.append(("中国样本事件研究", _render_table(focus, compact=True)))
    did = _load_single_csv(style_dir, "did_summary.csv")
    if did is not None:
        focus = did.loc[:, ["event_phase", "metric", "did_estimate", "n_treated", "n_control"]]
        tables.append(("DID 摘要", _render_table(focus, compact=True)))
    regression = _load_single_csv(style_dir, "regression_coefficients.csv")
    if regression is not None:
        focus = regression.loc[(regression["parameter"] == "inclusion") & (regression["specification"] == "main_car"), ["event_phase", "dependent_variable", "coefficient", "p_value"]]
        tables.append(("匹配回归核心系数", _render_table(focus, compact=True)))
    rdd = _load_single_csv(rdd_dir, "rdd_summary.csv")
    if rdd is not None:
        focus = rdd.loc[:, ["outcome", "tau", "p_value", "n_obs", "bandwidth"]]
        tables.append(("RDD 摘要", _render_table(focus, compact=True)))
    return tables


def _create_identification_figures() -> list[dict[str, str]]:
    import pandas as pd
    import numpy as np
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    rdd_dir = ROOT / "results" / "literature" / "hs300_rdd" / "figures"
    rdd_dir.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(ROOT / "results" / "literature" / "hs300_rdd" / "event_level_with_running.csv")
    subset = panel.loc[panel["event_phase"] == "announce", ["distance_to_cutoff", "car_m1_p1"]].dropna().copy()
    if subset.empty:
        return []

    left = subset.loc[subset["distance_to_cutoff"] < 0].sort_values("distance_to_cutoff")
    right = subset.loc[subset["distance_to_cutoff"] >= 0].sort_values("distance_to_cutoff")

    def _binned(frame: pd.DataFrame, bins: int) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["center", "mean"])
        frame = frame.copy()
        frame["bin"] = pd.cut(frame["distance_to_cutoff"], bins=bins, duplicates="drop")
        grouped = (
            frame.groupby("bin", observed=True)
            .agg(center=("distance_to_cutoff", "mean"), mean=("car_m1_p1", "mean"))
            .reset_index(drop=True)
        )
        return grouped

    def _fit_line(frame: pd.DataFrame):
        if len(frame) < 2 or frame["distance_to_cutoff"].nunique() < 2:
            return None, None
        x = frame["distance_to_cutoff"].to_numpy(dtype=float)
        y = frame["car_m1_p1"].to_numpy(dtype=float)
        design = np.column_stack([np.ones_like(x), x])
        intercept, slope = np.linalg.lstsq(design, y, rcond=None)[0]
        x_values = np.linspace(x.min(), x.max(), 80)
        y_values = intercept + slope * x_values
        return x_values, y_values

    left_bins = _binned(left, bins=min(6, max(len(left) // 4, 3)))
    right_bins = _binned(right, bins=min(6, max(len(right) // 4, 3)))
    left_x, left_y = _fit_line(left)
    right_x, right_y = _fit_line(right)

    figure_path = rdd_dir / "car_m1_p1_rdd_main.png"
    fig, ax = plt.subplots(figsize=(10.8, 6.0))
    ax.axvline(0, color="#5c6b77", linestyle="--", linewidth=1.2)
    ax.scatter(left["distance_to_cutoff"], left["car_m1_p1"], color="#d7b49e", alpha=0.24, s=28)
    ax.scatter(right["distance_to_cutoff"], right["car_m1_p1"], color="#9cc7cf", alpha=0.24, s=28)
    if not left_bins.empty:
        ax.scatter(left_bins["center"], left_bins["mean"], color="#a63b28", s=72, label="断点左侧分箱均值", zorder=3)
    if not right_bins.empty:
        ax.scatter(right_bins["center"], right_bins["mean"], color="#0f5c6e", s=72, label="断点右侧分箱均值", zorder=3)
    if left_x is not None:
        ax.plot(left_x, left_y, color="#a63b28", linewidth=2.4)
    if right_x is not None:
        ax.plot(right_x, right_y, color="#0f5c6e", linewidth=2.4)
    ax.set_title("中国样本 RDD 主图：公告日 CAR[-1,+1] 断点回归", fontsize=16, pad=14)
    ax.set_xlabel("距断点距离")
    ax.set_ylabel("CAR[-1,+1]")
    ax.grid(alpha=0.22)
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(figure_path, dpi=220)
    plt.close(fig)
    return [
        {
            "path": _safe_relative(figure_path),
            "caption": "中国样本 RDD 主图。图意：以公告日 CAR[-1,+1] 为例展示断点两侧分箱均值与局部拟合线。阅读重点：聚焦 0 附近是否存在离散跳跃，而不是只看两侧散点的总体波动。",
        }
    ]


def _load_identification_china_saved_result() -> dict[str, object]:
    style_dir = ROOT / "results" / "literature" / "hs300_style"
    rdd_dir = ROOT / "results" / "literature" / "hs300_rdd"

    style_summary_path = style_dir / "summary.md"
    rdd_summary_path = rdd_dir / "summary.md"
    style_summary = style_summary_path.read_text(encoding="utf-8").strip() if style_summary_path.exists() else "暂无风格识别摘要。"
    rdd_summary = rdd_summary_path.read_text(encoding="utf-8").strip() if rdd_summary_path.exists() else "暂无断点回归摘要。"

    combined_tables: list[tuple[str, str]] = []
    for label, html_table in _load_saved_tables(style_dir):
        combined_tables.append((f"风格识别：{label}", html_table))
    for label, html_table in _load_saved_tables(rdd_dir):
        combined_tables.append((f"断点回归：{label}", html_table))

    figure_paths = []
    for path in sorted(style_dir.rglob("*.png")):
        figure_paths.append({"path": _safe_relative(path), "caption": _build_figure_caption(path, prefix="风格识别")})
    for path in sorted(rdd_dir.rglob("*.png")):
        figure_paths.append({"path": _safe_relative(path), "caption": _build_figure_caption(path, prefix="断点回归")})

    summary_text = "\n\n".join(
        [
            "# 制度识别与中国市场证据结果包",
            "",
            "这个页面把中国市场识别主线下的风格识别与断点回归结果合并到同一页中，便于直接比较两类识别策略。",
            "",
            "## 第一部分：风格识别",
            style_summary,
            "",
            "## 第二部分：断点回归",
            rdd_summary,
        ]
    ).strip()

    return {
        "id": "identification_china_track",
        "title": ANALYSES["identification_china_track"]["title"],
        "description": ANALYSES["identification_china_track"]["description_zh"],
        "subtitle": ANALYSES["identification_china_track"]["subtitle"],
        "summary_text": summary_text,
        "rendered_tables": combined_tables,
        "figure_paths": figure_paths,
        "output_dir": _safe_relative(ROOT / "results" / "literature"),
    }


def _load_literature_library_result() -> dict[str, object]:
    return {
        "id": "paper_library",
        "title": LIBRARY_CARD["title"],
        "description": LIBRARY_CARD["description_zh"],
        "subtitle": LIBRARY_CARD["subtitle"],
        "summary_text": build_literature_summary_markdown(),
        "summary_cards": [
            {
                "kicker": "文献目录",
                "title": "先按阵营读，再按年份读",
                "meta": "统一排序规则",
                "copy": "当前目录已统一按阵营排序，阵营内部按年份排序，适合从研究史角度把握指数效应争论的演进脉络。",
            },
            {
                "kicker": "深度信息",
                "title": "不只看立场，也看识别对象",
                "meta": "目录已加入识别对象与挑战的假设",
                "copy": "这张总表现在不仅告诉你文献属于哪一派，也告诉你每篇文献具体识别了什么，并反驳了哪条旧假设。",
            },
        ],
        "rendered_tables": [
            ("文献分组统计", _render_table(build_literature_summary_frame(), compact=True)),
            ("文献目录", _render_table(build_literature_dashboard_frame(), compact=True)),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def _load_literature_review_result() -> dict[str, object]:
    return {
        "id": "paper_review",
        "title": REVIEW_CARD["title"],
        "description": REVIEW_CARD["description_zh"],
        "subtitle": REVIEW_CARD["subtitle"],
        "summary_text": build_literature_review_markdown(),
        "summary_cards": _build_review_summary_cards(),
        "rendered_tables": [
            ("反方文献", _render_table(build_grouped_literature_frame("反方"), compact=True)),
            ("中性文献", _render_table(build_grouped_literature_frame("中性"), compact=True)),
            ("正方文献", _render_table(build_grouped_literature_frame("正方"), compact=True)),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def _load_literature_framework_result() -> dict[str, object]:
    return {
        "id": "paper_framework",
        "title": FRAMEWORK_CARD["title"],
        "description": FRAMEWORK_CARD["description_zh"],
        "subtitle": FRAMEWORK_CARD["subtitle"],
        "summary_text": build_literature_framework_markdown(),
        "summary_cards": _build_framework_summary_cards(),
        "rendered_tables": [
            ("五大阵营概览", _render_table(build_camp_summary_frame(), compact=True)),
            ("文献演进总表", _render_table(build_literature_evolution_frame(), compact=True)),
            ("研究表达框架", _render_table(build_literature_meeting_frame(), compact=True)),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


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


def _paper_brief_title(record: dict[str, object]) -> str:
    return f"{_compact_author_label(str(record.get('authors', '')))}（{record.get('year_label', '')}）"


def _project_module_display(project_module: str) -> str:
    return PROJECT_MODULE_DISPLAY.get(project_module, project_module)


def _group_evolution_cards(cards: list[dict[str, object]], key: str) -> list[dict[str, object]]:
    groups: dict[str, list[dict[str, object]]] = {}
    for card in cards:
        label = str(card.get(key, ""))
        groups.setdefault(label, []).append(card)
    ordered: list[dict[str, object]] = []
    for label, group_cards in groups.items():
        ordered.append(
            {
                "title": label,
                "meta": f"{len(group_cards)} 篇文献",
                "cards": group_cards,
            }
        )
    return ordered


def _load_paper_detail_result(paper_id: str) -> dict[str, object] | None:
    import pandas as pd

    paper = get_literature_paper(paper_id)
    if paper is None:
        return None

    catalog_full = build_literature_catalog_frame()
    current_rows = catalog_full.loc[catalog_full["paper_id"] == paper_id]
    if current_rows.empty:
        return None
    current_index = int(current_rows.index[0])
    current_catalog_record = current_rows.iloc[0].to_dict()

    catalog = build_literature_dashboard_frame()
    row = catalog.loc[catalog["PDF"].str.contains(f'/paper/{paper_id}"', regex=False)].head(1)
    if row.empty:
        return None
    record = row.iloc[0].to_dict()

    authors = [part.strip() for part in paper.authors.split(";") if part.strip()]
    short_authors = authors[0] if len(authors) == 1 else f"{authors[0]} 等"

    info_frame = pd.DataFrame(
        [
            {"项目": "作者", "内容": paper.authors},
            {"项目": "年份", "内容": paper.year_label},
            {"项目": "阵营", "内容": record.get("阵营", "")},
            {"项目": "立场", "内容": record.get("立场", "")},
            {"项目": "市场 / 指数", "内容": record.get("市场 / 指数", "")},
            {"项目": "方法 / 关键词", "内容": record.get("方法 / 关键词", "")},
            {"项目": "研究主线", "内容": paper.project_module},
            {
                "项目": "原文入口",
                "内容": f'<a href="/paper/{paper_id}/pdf" target="_blank">打开原文 PDF</a>' if paper.exists else "PDF 不存在",
            },
        ]
    )
    deep_frame = pd.DataFrame(
        [
            {"分析维度": "识别对象", "内容": record.get("识别对象", "")},
            {"分析维度": "挑战的假设", "内容": record.get("挑战的假设", "")},
            {"分析维度": "一句话定位", "内容": record.get("一句话定位", "")},
            {"分析维度": "争论推进", "内容": record.get("争论推进", "")},
            {"分析维度": "研究中的作用", "内容": record.get("研究中的作用", "")},
        ]
    )
    summary_cards = [
        {
            "kicker": "识别对象",
            "title": str(record.get("识别对象", "")),
            "meta": f'{record.get("阵营", "")} · {record.get("立场", "")}',
            "copy": "这篇论文真正试图识别的核心问题，可以帮助区分它是在讨论短期冲击、长期保留、信息效应，还是识别设计本身。",
        },
        {
            "kicker": "挑战的假设",
            "title": str(record.get("挑战的假设", "")),
            "meta": "这篇论文在反驳什么",
            "copy": "读这篇文献时，关键不只是记住它支持哪一派，更要看它究竟在拆掉哪条旧前提。",
        },
        {
            "kicker": "争论推进",
            "title": str(record.get("一句话定位", "")),
            "meta": "它把文献往前推了哪一步",
            "copy": str(record.get("争论推进", "")),
        },
        {
            "kicker": "本文用途",
            "title": paper.project_module,
            "meta": "在当前项目中的位置",
            "copy": str(record.get("研究中的作用", "")),
        },
    ]
    summary_paragraphs = [
        f"{short_authors}（{paper.year_label}）对应的原始论文题目为《{paper.title}》。",
        f"这篇论文位于“{record.get('阵营', '')}”阵营，在当前项目中主要服务于“{paper.project_module}”这条主线。",
        "阅读这页时，可先看识别对象与挑战的假设，再看它如何推动指数效应争论向前发展，最后再决定是否进入原文 PDF。",
    ]
    summary_text = " ".join(summary_paragraphs)

    sequence_cards: list[dict[str, object]] = []
    prev_row = catalog_full.iloc[current_index - 1].to_dict() if current_index > 0 else None
    next_row = catalog_full.iloc[current_index + 1].to_dict() if current_index < len(catalog_full) - 1 else None

    if prev_row is not None:
        sequence_cards.append(
            {
                "kicker": "前一篇",
                "title": _paper_brief_title(prev_row),
                "year_label": str(prev_row.get("year_label", "")),
                "camp": str(prev_row.get("camp", "")),
                "track_label": _project_module_display(str(prev_row.get("project_module", ""))),
                "meta": f"{prev_row.get('camp', '')} · {prev_row.get('method_focus', '')}",
                "copy": str(prev_row.get("deep_contribution", "")),
                "href": f"/paper/{prev_row.get('paper_id')}",
                "is_current": False,
            }
        )
    sequence_cards.append(
        {
            "kicker": "当前位置",
            "title": f"{short_authors}（{paper.year_label}）",
            "year_label": str(current_catalog_record.get("year_label", "")),
            "camp": str(current_catalog_record.get("camp", "")),
            "track_label": _project_module_display(str(current_catalog_record.get("project_module", ""))),
            "meta": f"{current_catalog_record.get('camp', '')} · {current_catalog_record.get('method_focus', '')}",
            "copy": str(current_catalog_record.get("deep_contribution", "")),
            "href": "",
            "is_current": True,
        }
    )
    if next_row is not None:
        sequence_cards.append(
            {
                "kicker": "后一篇",
                "title": _paper_brief_title(next_row),
                "year_label": str(next_row.get("year_label", "")),
                "camp": str(next_row.get("camp", "")),
                "track_label": _project_module_display(str(next_row.get("project_module", ""))),
                "meta": f"{next_row.get('camp', '')} · {next_row.get('method_focus', '')}",
                "copy": str(next_row.get("deep_contribution", "")),
                "href": f"/paper/{next_row.get('paper_id')}",
                "is_current": False,
            }
        )

    candidates = catalog_full.loc[catalog_full["paper_id"] != paper_id].copy()
    same_module = candidates.loc[candidates["project_module"] == paper.project_module]
    same_camp = same_module.loc[same_module["camp"] == paper.camp]
    recommended = pd.concat(
        [
            same_camp.head(2),
            same_module.loc[~same_module["paper_id"].isin(same_camp["paper_id"])].head(2),
            candidates.loc[~candidates["paper_id"].isin(same_module["paper_id"])].head(2),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["paper_id"]).head(2)
    recommended_cards = [
        {
            "kicker": "推荐下一篇",
            "title": _paper_brief_title(rec.to_dict()),
            "year_label": str(rec["year_label"]),
            "camp": str(rec["camp"]),
            "meta": f"{rec['camp']} · {rec['project_module']} · {rec['method_focus']}",
            "copy": str(rec["practical_use"]),
            "href": f"/paper/{rec['paper_id']}",
        }
        for _, rec in recommended.iterrows()
    ]
    evolution_nav_cards = [
        {
            "kicker": f"{idx + 1:02d} · {row['stance']}",
            "title": _paper_brief_title(row.to_dict()),
            "year_label": str(row["year_label"]),
            "camp": str(row["camp"]),
            "stance": str(row["stance"]),
            "project_module": str(row["project_module"]),
            "track_label": _project_module_display(str(row["project_module"])),
            "copy": str(row["one_line_role"]),
            "href": f"/paper/{row['paper_id']}",
            "is_current": str(row["paper_id"]) == paper_id,
        }
        for idx, (_, row) in enumerate(catalog_full.iterrows())
    ]
    evolution_nav_views = [
        {"id": "camp", "groups": _group_evolution_cards(evolution_nav_cards, "camp")},
        {"id": "track", "groups": _group_evolution_cards(evolution_nav_cards, "track_label")},
        {"id": "stance", "groups": _group_evolution_cards(evolution_nav_cards, "stance")},
    ]

    return {
        "id": f"paper_{paper_id}",
        "title": f"{short_authors}（{paper.year_label}）",
        "description": paper.title,
        "subtitle": f"{record.get('阵营', '')} · {record.get('方法 / 关键词', '')}",
        "summary_text": summary_text,
        "summary_paragraphs": summary_paragraphs,
        "summary_cards": summary_cards,
        "rendered_tables": [
            ("论文信息", _render_table(info_frame, compact=True)),
            ("深度解读", _render_table(deep_frame, compact=True)),
        ],
        "sequence_cards": sequence_cards,
        "recommended_cards": recommended_cards,
        "evolution_nav_cards": evolution_nav_cards,
        "evolution_nav_views": evolution_nav_views,
        "figure_paths": [],
        "primary_actions": ([{"label": "打开原文 PDF", "href": f"/paper/{paper_id}/pdf", "target": "_blank"}] if paper.exists else []),
        "output_dir": "",
    }


def _load_supplement_result() -> dict[str, object]:
    return {
        "id": "project_supplement",
        "title": SUPPLEMENT_CARD["title"],
        "description": SUPPLEMENT_CARD["description_zh"],
        "subtitle": SUPPLEMENT_CARD["subtitle"],
        "summary_text": build_supplementary_summary_markdown(),
        "rendered_tables": [
            ("事件时钟", _render_table(build_event_clock_frame())),
            ("机制链", _render_table(build_mechanism_chain_frame())),
            ("冲击估算步骤", _render_table(build_impact_formula_frame())),
            ("冲击估算示例", _render_table(estimate_impact_scenarios())),
            ("表达框架", _render_table(build_case_playbook_frame())),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


def _strip_markdown_title(text: str) -> str:
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[0].startswith("#"):
        lines = lines[1:]
    return "\n".join(lines).strip()


def _clean_display_text(text: str) -> str:
    cleaned = _strip_markdown_title(text)
    lines = []
    for raw_line in cleaned.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("关键输出文件"):
            continue
        if "/Users/" in line:
            continue
        line = line.replace("`", "").lstrip("- ").strip()
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def _dashboard_mode() -> str:
    mode = request.args.get("mode", "demo")
    return mode if mode in {"brief", "demo", "full"} else "demo"


def _normalize_anchor_for_mode(mode: str, anchor: str | None) -> str:
    raw = (anchor or "").strip().lstrip("#")
    allowed = {item["anchor"] for item in _nav_sections_for_mode(mode)} | {
        "price_pressure_track",
        "demand_curve_track",
        "identification_china_track",
    }
    if not raw:
        return "overview"
    if raw in allowed:
        return raw
    if raw in {"framework", "supplement"} and "tracks" in allowed:
        return "tracks"
    return "overview"


def _nav_sections_for_mode(mode: str) -> list[dict[str, str]]:
    items = [
        {"anchor": "overview", "label": "总览"},
        {"anchor": "design", "label": "样本与设计"},
        {"anchor": "tracks", "label": "主线结果"},
    ]
    if mode != "brief":
        items.extend(
            [
                {"anchor": "framework", "label": "文献框架"},
                {"anchor": "supplement", "label": "机制补充"},
            ]
        )
    items.append({"anchor": "limits", "label": "研究边界"})
    return items


def _available_hashes_for_mode(mode: str) -> list[str]:
    hashes = ["#overview", "#design", "#tracks", "#limits"]
    if mode != "brief":
        hashes.extend(["#framework", "#supplement"])
    hashes.extend(
        [
            "#price_pressure_track",
            "#demand_curve_track",
            "#identification_china_track",
        ]
    )
    return hashes


def _mode_tabs_for_mode(mode: str) -> list[dict[str, object]]:
    labels = {
        "brief": "3 分钟汇报",
        "demo": "展示版",
        "full": "完整材料",
    }
    default_hash = {
        "brief": "#overview",
        "demo": "#overview",
        "full": "#overview",
    }
    tabs: list[dict[str, object]] = []
    for tab_mode in ("brief", "demo", "full"):
        tabs.append(
            {
                "mode": tab_mode,
                "label": labels[tab_mode],
                "base_href": url_for("home", mode=tab_mode),
                "href": url_for("home", mode=tab_mode, _anchor=default_hash[tab_mode].lstrip("#")),
                "default_hash": default_hash[tab_mode],
                "allowed_hashes": _available_hashes_for_mode(tab_mode),
                "active": tab_mode == mode,
            }
        )
    return tabs


def _saved_output_dir_for_analysis(analysis_id: str) -> Path | None:
    mapping = {
        "price_pressure_track": ROOT / "results" / "literature" / "harris_gurel",
        "demand_curve_track": ROOT / "results" / "literature" / "shleifer",
    }
    return mapping.get(analysis_id)


def _load_saved_track_result(analysis_id: str, config: dict[str, object]) -> dict[str, object] | None:
    if analysis_id == "identification_china_track":
        current = _load_identification_china_saved_result()
        return _attach_project_track_context(current, config)
    output_dir = _saved_output_dir_for_analysis(analysis_id)
    if output_dir is None:
        return None
    summary_path = output_dir / "summary.md"
    if not summary_path.exists():
        return None
    current = {
        "id": config.get("project_module", analysis_id),
        "title": config["title"],
        "description": config["description_zh"],
        "subtitle": config["subtitle"],
        "summary_text": summary_path.read_text(encoding="utf-8"),
        "rendered_tables": _load_saved_tables(output_dir),
        "figure_paths": [
            {
                "path": _safe_relative(path),
                "caption": _build_figure_caption(path),
            }
            for path in sorted(output_dir.rglob("*.png"))
        ],
        "output_dir": _safe_relative(output_dir),
    }
    return _attach_project_track_context(current, config)


def _run_and_cache_all() -> None:
    for analysis_id, config in ANALYSES.items():
        raw = config["runner"](verbose=False)
        current = _normalize_result(raw)
        current["id"] = config.get("project_module", analysis_id)
        current["title"] = config["title"]
        current["description"] = raw.get("description", config["description_zh"])
        current["subtitle"] = config["subtitle"]
        current = _attach_project_track_context(current, config)
        RUN_CACHE[analysis_id] = current
    RUN_CACHE["paper_library"] = _load_literature_library_result()
    RUN_CACHE["paper_review"] = _load_literature_review_result()
    RUN_CACHE["paper_framework"] = _load_literature_framework_result()
    RUN_CACHE["project_supplement"] = _load_supplement_result()


def _load_or_build_track_section(analysis_id: str) -> dict[str, object]:
    current = RUN_CACHE.get(analysis_id)
    config = ANALYSES[analysis_id]
    if current is None:
        current = _load_saved_track_result(analysis_id, config)
        if current is not None:
            RUN_CACHE[analysis_id] = current
    if current is None:
        raw = config["runner"](verbose=False)
        current = _normalize_result(raw)
        current["id"] = config.get("project_module", analysis_id)
        current["title"] = config["title"]
        current["description"] = raw.get("description", config["description_zh"])
        current["subtitle"] = config["subtitle"]
        current = _attach_project_track_context(current, config)
        RUN_CACHE[analysis_id] = current
    return current


def _prepare_track_display(section: dict[str, object], analysis_id: str, demo_mode: bool) -> dict[str, object]:
    display = dict(section)
    curated_summary = {
        "price_pressure_track": "这条主线集中展示短窗口 CAR、公告日与生效日差异，以及交易活跃度变化。当前样本表明，美国市场的公告日效应更强，而中国 A 股在生效日呈现更明显的不对称性。",
        "demand_curve_track": "这条主线关注价格冲击是否只在短期出现，还是会在更长窗口中保留。阅读时应重点比较保留率、长窗口 CAR，以及短长窗口之间的差异。",
        "identification_china_track": "这条主线把中国样本的匹配对照组结果与 RDD 扩展并排展示，用于区分“现象是否存在”与“识别是否足够严格”这两个层面。",
    }
    display["display_summary"] = curated_summary.get(analysis_id, _clean_display_text(str(display.get("summary_text", ""))))
    display["display_support_papers"] = display.get("support_papers", [])
    cards = {
        "price_pressure_track": _build_price_pressure_cards(),
        "demand_curve_track": _build_demand_curve_cards(),
        "identification_china_track": _build_identification_cards(),
    }.get(analysis_id, [])
    display["result_cards"] = cards
    extra_figures = []
    if analysis_id == "identification_china_track":
        extra_figures = _create_identification_figures()
    all_figures = [*extra_figures, *display.get("figure_paths", [])]
    display["display_figures"] = all_figures[: (4 if demo_mode else 6)]
    curated_tables = {
        "price_pressure_track": _build_price_pressure_tables(),
        "demand_curve_track": _build_demand_curve_tables(),
        "identification_china_track": _build_identification_tables(),
    }.get(analysis_id, [])
    display["display_tables"] = _decorate_display_tables(curated_tables)
    display["badge"] = "核心结果" if demo_mode else "完整结果"
    takeaway = {
        "price_pressure_track": "当前样本更支持“短期冲击具有明显市场差异”这一判断，而不是简单地认为所有市场都会在纳入后同步上涨。",
        "demand_curve_track": "价格冲击并未在所有窗口中完全回吐，这意味着需求曲线效应仍有解释力，但其保留程度具有明显的阶段差异。",
        "identification_china_track": "中国市场证据不仅取决于现象本身，还取决于识别设计；匹配回归与 RDD 的并置展示正好体现了这一点。",
    }
    display["takeaway"] = takeaway.get(analysis_id, "")
    return display


def _prepare_framework_display(section: dict[str, object], demo_mode: bool) -> dict[str, object]:
    display = dict(section)
    display["display_summary"] = "这里把 16 篇文献组织成一条可以直接讲述的研究史：从 1986 年的经典对决，到现代市场里指数效应的弱化，再到 RDD 方法与中国市场证据的扩展。"
    raw_tables = {label: html for label, html in display.get("rendered_tables", [])}
    ordered_tables = [
        ("文献演进总表", raw_tables["文献演进总表"]),
        ("五大阵营概览", raw_tables["五大阵营概览"]),
        ("研究表达框架", raw_tables["研究表达框架"]),
    ]
    display["summary_cards"] = _build_framework_summary_cards()
    tables = ordered_tables
    display["display_tables"] = _decorate_display_tables(tables)
    return display


def _prepare_supplement_display(section: dict[str, object], demo_mode: bool) -> dict[str, object]:
    display = dict(section)
    display["display_summary"] = "这部分把事件研究背后的交易逻辑整理成更便于讨论的解释框架，重点在于说明资金何时进场、冲击为何形成，以及价格与流动性如何在不同阶段调整。"
    raw_tables = {label: html for label, html in display.get("rendered_tables", [])}
    ordered_tables = [
        ("事件时钟", raw_tables["事件时钟"]),
        ("机制链", raw_tables["机制链"]),
        ("冲击估算步骤", raw_tables["冲击估算步骤"]),
        ("冲击估算示例", raw_tables["冲击估算示例"]),
        ("表达框架", raw_tables["表达框架"]),
    ]
    display["summary_cards"] = _build_supplement_summary_cards()
    tables = ordered_tables
    display["display_tables"] = _decorate_display_tables(tables)
    return display


def _build_track_notes(analysis_id: str) -> list[dict[str, str]]:
    if analysis_id == "price_pressure_track":
        return [
            {"name": "主问题", "copy": "这条主线专门回答指数纳入后的上涨是不是主要来自短期交易冲击，而不是长期重估。"},
            {"name": "阅读顺序", "copy": "先比较短窗口 CAR，再观察公告日与生效日的相对强弱，最后结合成交量、换手率与波动率变化。"},
            {"name": "样本特征", "copy": "真实样本中，美股公告日效应更强；A 股生效日短窗口出现显著负向表现，说明中国样本具有更明显的异质性。"},
        ]
    if analysis_id == "demand_curve_track":
        return [
            {"name": "主问题", "copy": "这条主线专门判断上涨是不是只短暂发生，还是会保留到更长窗口，从而支持需求曲线向下倾斜。"},
            {"name": "阅读顺序", "copy": "优先观察 retention ratio、长窗口 CAR 与短长窗口对比，而不是只停留在公告当天的涨跌。"},
            {"name": "样本特征", "copy": "当前真实样本显示，中美市场都存在一定程度的长期保留，但公告阶段与生效阶段的保留形态并不一致。"},
        ]
    return [
        {"name": "主问题", "copy": "这条主线专门处理识别问题，回答不同制度背景和识别方法是否会改变对指数效应的判断。"},
        {"name": "阅读顺序", "copy": "先观察中国样本的匹配对照组结果，再查看 DID 风格摘要，最后结合 RDD 结果比较识别强度。"},
        {"name": "样本特征", "copy": "目前风格识别部分已基于真实样本运行；RDD 部分主要展示识别方法与边界样本思路，正式研究时可替换为真实候选排名文件。"},
    ]


def _build_overview_metrics() -> list[dict[str, str]]:
    import pandas as pd

    event_counts = pd.read_csv(ROOT / "results" / "real_tables" / "event_counts.csv")
    total_events = int(event_counts["n_events"].sum())
    return [
        {"value": "16", "label": "篇核心文献，构成理论基础"},
        {"value": "3", "label": "条研究主线，对应主要实证模块"},
        {"value": "5", "label": "个研究阵营，构成文献演进框架"},
        {"value": str(total_events), "label": "个真实纳入事件，纳入当前样本"},
    ]


def _build_overview_notes() -> list[dict[str, str]]:
    return [
        {"title": "文献层", "copy": "16 篇文献既可按反方、中性、正方阅读，也可按五大阵营理解研究演进。"},
        {"title": "实证层", "copy": "三条研究主线均已接入真实样本、核心表格与可视化结果。"},
        {"title": "方法层", "copy": "页面将事件研究、匹配回归与 RDD 并置展示，便于比较识别强度。"},
        {"title": "机制层", "copy": "补充部分展示事件时钟、机制链与冲击估算，用于解释统计结果背后的交易逻辑。"},
    ]


def _build_overview_notes_for_mode(mode: str) -> list[dict[str, str]]:
    if mode == "brief":
        return [
            {"title": "样本层", "copy": "首页先交代真实样本覆盖、事件窗口口径与跨市场比较的基本范围。"},
            {"title": "结果层", "copy": "3 分钟汇报模式保留三条研究主线的核心结果，便于快速建立主要结论。"},
            {"title": "识别层", "copy": "事件研究、匹配回归与 RDD 的识别含义仍保留在主线解释中，只是不再展开完整附加材料。"},
            {"title": "边界层", "copy": "页面最后仍保留研究边界，用于交代样本期、识别范围与数据口径。"},
        ]
    return _build_overview_notes()


def _build_overview_summary() -> str:
    return (
        "页面将文献框架、真实数据结果、机制解释与识别设计放在同一叙述结构中，"
        "从而形成一条完整且可连续展开的研究链条。"
    )


def _build_overview_summary_for_mode(mode: str) -> str:
    if mode == "brief":
        return (
            "页面将真实样本、三条研究主线与研究边界压缩为一套适合快速汇报的展示材料，"
            "用于在较短时间内说明现象、机制与识别三个层面。"
        )
    return _build_overview_summary()


def _build_cta_copy_for_mode(mode: str) -> str:
    if mode == "brief":
        return "页面以压缩方式呈现样本、主线与研究边界，适合在较短时间内完成问题提出、证据展示与边界交代。"
    return "页面同步呈现主线结果、文献框架与机制补充，便于在同一叙述中完成现象、机制与识别三个层面的展示。"


def _build_abstract_lead() -> str:
    return (
        "当前样本表明，指数纳入效应并非在所有市场与阶段中都以同一方向出现。"
        "更合理的解释框架是将短期冲击、长期保留与识别设计同时纳入分析。"
    )


def _build_abstract_points() -> list[dict[str, str]]:
    return [
        {
            "title": "现象层",
            "copy": "美国市场的公告日效应最稳定，中国 A 股则在生效阶段呈现更明显的不对称性，说明跨市场比较必须区分事件时点。",
        },
        {
            "title": "机制层",
            "copy": "短期价格压力与需求曲线效应并非互相排斥，更可能是在不同窗口中共同发挥作用，只是权重和持续性不同。",
        },
        {
            "title": "识别层",
            "copy": "事件研究能够说明现象，匹配回归帮助控制样本差异，而 RDD 则进一步提升识别强度，三者应被视为互补而非替代。",
        },
    ]


def _build_highlights() -> list[dict[str, str]]:
    import pandas as pd

    summary = pd.read_csv(ROOT / "results" / "real_tables" / "event_study_summary.csv")
    us_announce = summary.loc[
        (summary["market"] == "US") & (summary["event_phase"] == "announce") & (summary["window_slug"] == "m1_p1")
    ].iloc[0]
    cn_effective = summary.loc[
        (summary["market"] == "CN") & (summary["event_phase"] == "effective") & (summary["window_slug"] == "m1_p1")
    ].iloc[0]
    return [
        {
            "label": "最强结论",
            "headline": "美股公告日仍然呈现最稳定的短期正向效应。",
            "copy": f"当前真实样本里，美国市场公告日 CAR[-1,+1] 平均值为 {us_announce['mean_car']:.2%}，p 值为 {us_announce['p_value']:.4f}，是整套结果里最稳的短期正向证据。",
        },
        {
            "label": "最值得讨论",
            "headline": "A 股生效日并不简单重复美股叙事。",
            "copy": f"中国 A 股在生效日 CAR[-1,+1] 平均值为 {cn_effective['mean_car']:.2%}，且统计显著。这说明 A 股市场不能机械套用美股的经典指数纳入叙事。",
        },
        {
            "label": "方法含义",
            "headline": "研究价值不仅在于涨跌，更在于识别。",
            "copy": "事件研究说明现象，匹配回归帮助控制样本差异，RDD 提供更严格的识别框架。将三者并置展示，有助于更清楚地讨论结论的可信度。",
        },
    ]


def _dashboard_figure_dir() -> Path:
    target = ROOT / "results" / "real_figures"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _significance_stars(p_value: float) -> str:
    if p_value < 0.01:
        return "***"
    if p_value < 0.05:
        return "**"
    if p_value < 0.10:
        return "*"
    return ""


def _create_sample_design_figures() -> list[dict[str, str]]:
    import pandas as pd
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

    plt.rcParams["font.sans-serif"] = ["Songti SC", "STHeiti", "Arial Unicode MS", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    target_dir = _dashboard_figure_dir()
    event = pd.read_csv(ROOT / "results" / "real_tables" / "event_study_summary.csv")
    regression = pd.read_csv(ROOT / "results" / "real_regressions" / "regression_coefficients.csv")
    diagnostics = pd.read_csv(ROOT / "results" / "real_regressions" / "match_diagnostics.csv")
    real_events = pd.read_csv(ROOT / "data" / "raw" / "real_events.csv")

    market_labels = {"CN": "中国 A 股", "US": "美国"}
    phase_labels = {"announce": "公告日", "effective": "生效日"}
    spec_labels = {
        "turnover_mechanism": "换手率变化",
        "volume_mechanism": "成交量变化",
        "volatility_mechanism": "波动率变化",
    }
    market_colors = {"CN": "#a63b28", "US": "#0f5c6e"}

    timeline_path = target_dir / "sample_event_timeline.png"
    long_events = real_events.loc[:, ["market", "ticker", "announce_date", "effective_date"]].copy()
    announce = long_events.rename(columns={"announce_date": "event_date"}).assign(event_phase="announce")
    effective = long_events.rename(columns={"effective_date": "event_date"}).assign(event_phase="effective")
    timeline = pd.concat([announce, effective], ignore_index=True)
    timeline["event_date"] = pd.to_datetime(timeline["event_date"])
    timeline["row_label"] = timeline["market"].map(market_labels) + " · " + timeline["event_phase"].map(phase_labels)
    row_order = ["中国 A 股 · 公告日", "中国 A 股 · 生效日", "美国 · 公告日", "美国 · 生效日"]
    row_positions = {label: idx for idx, label in enumerate(row_order)}
    fig, ax = plt.subplots(figsize=(12.2, 4.8))
    for market in ["CN", "US"]:
        for phase, marker in [("announce", "o"), ("effective", "s")]:
            subset = timeline.loc[(timeline["market"] == market) & (timeline["event_phase"] == phase)].copy()
            if subset.empty:
                continue
            y_values = [row_positions[label] for label in subset["row_label"]]
            ax.scatter(
                subset["event_date"],
                y_values,
                s=72,
                marker=marker,
                color=market_colors[market],
                alpha=0.88,
                edgecolor="white",
                linewidth=0.7,
                label=f"{market_labels[market]}{phase_labels[phase]}",
            )
    ax.set_yticks(range(len(row_order)))
    ax.set_yticklabels(row_order, fontsize=11)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.tick_params(axis="x", labelrotation=0)
    ax.set_title("真实纳入事件时间线", fontsize=16, pad=14)
    ax.set_xlabel("事件日期")
    ax.set_ylabel("样本分层")
    ax.grid(axis="x", alpha=0.25)
    handles, labels = ax.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    ax.legend(unique.values(), unique.keys(), ncol=2, loc="upper left", frameon=False, fontsize=10)
    fig.tight_layout()
    fig.savefig(timeline_path, dpi=220)
    plt.close(fig)

    heatmap_path = target_dir / "sample_car_heatmap.png"
    window_order = ["[-1,+1]", "[-3,+3]", "[-5,+5]"]
    heat = event.copy()
    heat["row_label"] = heat["market"].map(market_labels) + " · " + heat["event_phase"].map(phase_labels)
    heat_matrix = (
        heat.pivot_table(index="row_label", columns="window", values="mean_car", aggfunc="first")
        .reindex(index=row_order, columns=window_order)
    )
    p_matrix = (
        heat.pivot_table(index="row_label", columns="window", values="p_value", aggfunc="first")
        .reindex(index=row_order, columns=window_order)
    )
    fig, ax = plt.subplots(figsize=(10.2, 5.8))
    cmap = LinearSegmentedColormap.from_list("car_heat", ["#9c2f55", "#f7f2ea", "#0f5c6e"])
    vmax = max(abs(float(heat_matrix.min().min())), abs(float(heat_matrix.max().max())), 0.01)
    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    image = ax.imshow(heat_matrix.values, cmap=cmap, norm=norm, aspect="auto")
    ax.set_xticks(range(len(window_order)))
    ax.set_xticklabels(window_order, fontsize=11)
    ax.set_yticks(range(len(row_order)))
    ax.set_yticklabels(row_order, fontsize=11)
    ax.set_title("真实样本短窗口 CAR 热力图", fontsize=16, pad=14)
    ax.set_xlabel("事件窗口")
    ax.set_ylabel("市场与事件阶段")
    for i, row_label in enumerate(row_order):
        for j, window in enumerate(window_order):
            car = float(heat_matrix.loc[row_label, window])
            p_value = float(p_matrix.loc[row_label, window])
            color = "white" if abs(car) > vmax * 0.45 else "#18212b"
            ax.text(
                j,
                i,
                f"{car:.2%}\n{_significance_stars(p_value)}",
                ha="center",
                va="center",
                fontsize=11,
                color=color,
                fontweight="bold",
            )
    color_bar = fig.colorbar(image, ax=ax, shrink=0.92)
    color_bar.ax.set_ylabel("平均 CAR", rotation=90, labelpad=12)
    fig.tight_layout()
    fig.savefig(heatmap_path, dpi=220)
    plt.close(fig)

    main_path = target_dir / "main_regression_coefficients.png"
    main = regression.loc[
        (regression["parameter"] == "inclusion") & (regression["specification"] == "main_car")
    ].copy()
    main["label"] = main["market"].map(market_labels) + " · " + main["event_phase"].map(phase_labels)
    main["ci"] = 1.96 * main["std_error"]
    main_order = ["中国 A 股 · 公告日", "中国 A 股 · 生效日", "美国 · 公告日", "美国 · 生效日"]
    main = main.set_index("label").reindex(main_order).reset_index()
    fig, ax = plt.subplots(figsize=(10.2, 5.6))
    y_positions = list(range(len(main)))
    colors = ["#a63b28" if "中国" in label else "#0f5c6e" for label in main["label"]]
    ax.axvline(0, color="#8894a0", linewidth=1.2, linestyle="--")
    for idx, row in main.iterrows():
        ax.errorbar(
            row["coefficient"],
            idx,
            xerr=row["ci"],
            fmt="o",
            color=colors[idx],
            ecolor=colors[idx],
            elinewidth=2,
            capsize=4,
            markersize=8,
        )
        offset = 0.002 if row["coefficient"] >= 0 else -0.002
        ax.text(
            row["coefficient"] + offset,
            idx + 0.18,
            _format_p_value(float(row["p_value"])),
            color=colors[idx],
            fontsize=10,
            ha="left" if row["coefficient"] >= 0 else "right",
        )
    ax.set_yticks(y_positions)
    ax.set_yticklabels(main["label"], fontsize=11)
    ax.set_xlabel("纳入变量估计系数")
    ax.set_title("主回归纳入系数与 95% 置信区间", fontsize=16, pad=14)
    ax.grid(axis="x", alpha=0.25)
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(main_path, dpi=220)
    plt.close(fig)

    mechanism_path = target_dir / "mechanism_regression_coefficients.png"
    mechanism = regression.loc[
        (regression["parameter"] == "inclusion") & (regression["specification"] != "main_car")
    ].copy()
    mechanism["metric_label"] = mechanism["specification"].map(spec_labels)
    mechanism["phase_label"] = mechanism["event_phase"].map(phase_labels)
    mechanism["ci"] = 1.96 * mechanism["std_error"]
    fig, axes = plt.subplots(1, 2, figsize=(12.8, 6.0))
    for ax, market in zip(axes, ["CN", "US"]):
        subset = mechanism.loc[mechanism["market"] == market].copy()
        subset["label"] = subset["phase_label"] + " · " + subset["metric_label"]
        order = [
            "公告日 · 换手率变化",
            "公告日 · 成交量变化",
            "公告日 · 波动率变化",
            "生效日 · 换手率变化",
            "生效日 · 成交量变化",
            "生效日 · 波动率变化",
        ]
        subset = subset.set_index("label").reindex(order).dropna(subset=["coefficient"]).reset_index()
        color = "#a63b28" if market == "CN" else "#0f5c6e"
        ax.axvline(0, color="#8894a0", linewidth=1.2, linestyle="--")
        for idx, row in subset.iterrows():
            ax.errorbar(
                row["coefficient"],
                idx,
                xerr=row["ci"],
                fmt="o",
                color=color,
                ecolor=color,
                elinewidth=1.8,
                capsize=4,
                markersize=7,
            )
            ax.text(
                row["coefficient"] + (0.01 if row["coefficient"] >= 0 else -0.01),
                idx + 0.16,
                _significance_stars(float(row["p_value"])),
                color=color,
                fontsize=11,
                ha="left" if row["coefficient"] >= 0 else "right",
            )
        ax.set_yticks(range(len(subset)))
        ax.set_yticklabels(subset["label"], fontsize=10)
        ax.set_title(market_labels[market], fontsize=14)
        ax.grid(axis="x", alpha=0.25)
        ax.invert_yaxis()
    fig.supxlabel("纳入变量估计系数", fontsize=12)
    fig.suptitle("机制回归纳入系数与 95% 置信区间", fontsize=16, y=0.98)
    fig.tight_layout()
    fig.savefig(mechanism_path, dpi=220)
    plt.close(fig)

    match_path = target_dir / "match_diagnostics_overview.png"
    matched_total = len(diagnostics)
    status_counts = diagnostics["status"].value_counts().sort_values(ascending=False)
    metrics = {
        "匹配成功率": (diagnostics["status"] == "matched").mean(),
        "完整三对照比例": (diagnostics["selected_controls"] == 3).mean(),
        "行业口径放宽占比": diagnostics["sector_relaxed"].fillna(False).astype(bool).mean(),
    }
    fig, axes = plt.subplots(1, 2, figsize=(11.8, 4.8))
    ax = axes[0]
    colors = ["#0f5c6e" if status == "matched" else "#c36a2d" for status in status_counts.index]
    ax.bar(status_counts.index, status_counts.values, color=colors, width=0.6)
    for idx, value in enumerate(status_counts.values):
        ax.text(idx, value + 0.6, f"{int(value)}", ha="center", va="bottom", fontsize=11)
    ax.set_title("匹配状态分布", fontsize=14)
    ax.set_ylabel("事件数")
    ax.grid(axis="y", alpha=0.22)
    ax.set_axisbelow(True)
    ax.text(
        0.02,
        0.94,
        f"总事件数：{matched_total}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="#445463",
    )

    ax = axes[1]
    metric_labels = list(metrics.keys())
    metric_values = list(metrics.values())
    y_positions = list(range(len(metric_labels)))
    ax.barh(y_positions, metric_values, color=["#0f5c6e", "#1f7a8c", "#a63b28"], height=0.52)
    for idx, value in enumerate(metric_values):
        ax.text(value + 0.015, idx, _format_share(value), va="center", fontsize=11, color="#223546")
    ax.set_yticks(y_positions)
    ax.set_yticklabels(metric_labels, fontsize=11)
    ax.set_xlim(0, 1.05)
    ax.set_title("匹配质量概览", fontsize=14)
    ax.set_xlabel("比例")
    ax.grid(axis="x", alpha=0.22)
    ax.set_axisbelow(True)
    fig.suptitle("匹配诊断图", fontsize=16, y=0.99)
    fig.tight_layout()
    fig.savefig(match_path, dpi=220)
    plt.close(fig)

    return [
        {
            "label": "真实纳入事件时间线",
            "caption": "图意：按市场与事件阶段展开所有真实纳入事件。阅读重点：观察样本是否集中于少数批次，以及公告日与生效日是否在时间轴上形成清晰层次。",
            "path": _safe_relative(timeline_path),
            "layout_class": "wide",
        },
        {
            "label": "真实样本短窗口 CAR 热力图",
            "caption": "图意：把三组短窗口 CAR 压缩到同一张热力图中。阅读重点：优先比较美国公告日和中国生效日单元格的方向、幅度与显著性差异。",
            "path": _safe_relative(heatmap_path),
            "layout_class": "wide",
        },
        {
            "label": "主回归纳入系数图",
            "caption": "图意：展示主回归中纳入变量系数与 95% 置信区间。阅读重点：比较不同市场、不同事件阶段的方向是否一致，以及置信区间是否跨越 0。",
            "path": _safe_relative(main_path),
            "layout_class": "",
        },
        {
            "label": "机制回归系数图",
            "caption": "图意：把换手率、成交量与波动率三类机制回归放在同一张图中。阅读重点：观察中国 A 股与美国在公告日和生效日的机制方向是否一致。",
            "path": _safe_relative(mechanism_path),
            "layout_class": "",
        },
        {
            "label": "匹配诊断图",
            "caption": "图意：同时展示匹配状态分布与匹配质量指标。阅读重点：先看匹配成功率，再看三对照构造与行业口径放宽占比，从而判断对照组设计是否稳定。",
            "path": _safe_relative(match_path),
            "layout_class": "wide",
        },
    ]


def _build_sample_design_cards() -> list[dict[str, str]]:
    import pandas as pd

    sample_scope = pd.read_csv(ROOT / "results" / "real_tables" / "sample_scope.csv")
    data_sources = pd.read_csv(ROOT / "results" / "real_tables" / "data_sources.csv")
    diagnostics = pd.read_csv(ROOT / "results" / "real_regressions" / "match_diagnostics.csv")

    event_row = sample_scope.loc[sample_scope["样本层"] == "事件样本"].iloc[0]
    short_row = sample_scope.loc[sample_scope["样本层"] == "事件研究面板"].iloc[0]
    matched_row = sample_scope.loc[sample_scope["样本层"] == "匹配回归面板"].iloc[0]
    source_row = data_sources.loc[data_sources["数据集"] == "事件样本"].iloc[0]
    total_events = int(event_row["事件数"])
    avg_obs = int(round(float(short_row["观测值"]) / float(short_row["事件相位窗口数"])))
    matched_rate = (diagnostics["status"] == "matched").mean()
    return [
        {
            "kicker": "真实样本",
            "title": f"{total_events} 个纳入事件",
            "meta": f'{source_row["市场范围"]} · {source_row["起始日期"]} 至 {source_row["结束日期"]}',
            "copy": "当前样本以正式事件样本表为基础，统一覆盖真实纳入事件、事件相位窗口与跨市场比较所需的核心样本层。",
            "foot": "样本层的重点不是单纯扩大数量，而是在同一口径下同时覆盖不同市场、不同事件阶段与可比事件窗口。",
        },
        {
            "kicker": "事件口径",
            "title": "公告日与生效日双时点",
            "meta": f"平均事件窗口观测数约 {avg_obs:,}",
            "copy": f'短窗口事件研究面板共 {int(short_row["观测值"]):,} 条观测值，长窗口保留分析另行扩展到 {int(sample_scope.loc[sample_scope["样本层"] == "长窗口保留分析", "观测值"].iloc[0]):,} 条观测值。',
            "foot": "这一步有助于把“预期形成”和“被动调仓执行”分开，并避免用短面板误读长窗口结论。",
        },
        {
            "kicker": "识别设计",
            "title": "事件研究、匹配回归与 RDD",
            "meta": f"匹配成功率 {_format_share(matched_rate)}",
            "copy": f'匹配回归面板目前包含 {int(matched_row["观测值"]):,} 条观测值，用于把事件研究结果与控制变量、对照组设计结合起来理解。',
            "foot": "这组设计并非相互替代，而是对应不同研究问题：先确认现象，再讨论机制，最后提升识别可信度。",
        },
    ]


def _build_sample_design_tables() -> list[dict[str, str]]:
    import pandas as pd

    event_counts = pd.read_csv(ROOT / "results" / "real_tables" / "event_counts.csv").copy()
    sample_scope = pd.read_csv(ROOT / "results" / "real_tables" / "sample_scope.csv").copy()
    data_sources = pd.read_csv(ROOT / "results" / "real_tables" / "data_sources.csv").copy()
    if "文件" in data_sources.columns:
        data_sources = data_sources.drop(columns=["文件"])
    event_summary = pd.read_csv(ROOT / "results" / "real_tables" / "event_study_summary.csv")
    regression = pd.read_csv(ROOT / "results" / "real_regressions" / "regression_coefficients.csv")

    event_counts["sample_share"] = event_counts["n_events"] / event_counts["n_events"].sum()
    comparison_rows = []
    for market, market_label in [("CN", "中国 A 股"), ("US", "美国")]:
        market_events = int(event_counts.loc[event_counts["market"] == market, "n_events"].sum())
        short_window = event_summary.loc[
            (event_summary["market"] == market) & (event_summary["window_slug"] == "m1_p1")
        ].copy()
        strongest = short_window.loc[short_window["mean_car"].abs().idxmax()]
        main_reg = regression.loc[
            (regression["market"] == market)
            & (regression["parameter"] == "inclusion")
            & (regression["specification"] == "main_car")
        ].copy()
        reg_focus = main_reg.loc[main_reg["p_value"].idxmin()]
        stage_text = f'{VALUE_LABELS.get(str(strongest["event_phase"]), strongest["event_phase"])} {strongest["window"]}'
        car_text = f'{float(strongest["mean_car"]):.2%}（{_format_p_value(float(strongest["p_value"]))}）'
        reg_text = f'{float(reg_focus["coefficient"]):.4f}（{_format_p_value(float(reg_focus["p_value"]))}）'
        if market == "CN":
            discussion = "生效日短窗口显著为负，说明中国样本更适合围绕执行阶段、制度差异与不对称性展开解释。"
            implication = "仅用美股经典叙事解释 A 股并不充分，匹配回归与中国制度背景讨论更重要。"
        else:
            discussion = "公告日短窗口正向效应最稳定，说明美股市场里预期确认与提前布局仍是最值得优先讨论的环节。"
            implication = "美股部分更适合用短期价格压力、抢跑交易与效应减弱来组织结果解释。"
        comparison_rows.append(
            {
                "市场": market_label,
                "纳入事件数": f"{market_events:,}",
                "最强阶段": stage_text,
                "最强短窗口 CAR": car_text,
                "主回归纳入系数": reg_text,
                "最值得讨论": discussion,
                "识别含义": implication,
            }
        )

    comparison_table = pd.DataFrame(comparison_rows)
    return [
        {
            "label": "样本范围总表",
            "html": _render_table(sample_scope, compact=True),
            "layout_class": "wide",
        },
        {
            "label": "数据来源与口径",
            "html": _render_table(data_sources, compact=True),
            "layout_class": "wide",
        },
        {
            "label": "A 股与美股并列总结",
            "html": _render_table(comparison_table, compact=True),
            "layout_class": "wide",
        },
    ]


def _build_sample_design_section() -> dict[str, object]:
    return {
        "summary": "这一部分优先交代真实样本覆盖、短窗口口径与回归结果的总体轮廓，使后续主线解释建立在清晰的样本与识别设计之上。",
        "summary_cards": _build_sample_design_cards(),
        "figures": _create_sample_design_figures(),
        "tables": _build_sample_design_tables(),
    }


def _build_limits_section() -> dict[str, object]:
    import pandas as pd

    identification_scope = pd.read_csv(ROOT / "results" / "real_tables" / "identification_scope.csv")
    data_sources = pd.read_csv(ROOT / "results" / "real_tables" / "data_sources.csv")
    sample_scope = pd.read_csv(ROOT / "results" / "real_tables" / "sample_scope.csv")
    diagnostics = pd.read_csv(ROOT / "results" / "real_regressions" / "match_diagnostics.csv")

    event_row = data_sources.loc[data_sources["数据集"] == "事件样本"].iloc[0]
    price_row = data_sources.loc[data_sources["数据集"] == "日频价格"].iloc[0]
    benchmark_row = data_sources.loc[data_sources["数据集"] == "基准收益"].iloc[0]
    matched_row = sample_scope.loc[sample_scope["样本层"] == "匹配回归面板"].iloc[0]
    short_id_row = identification_scope.loc[identification_scope["分析层"] == "短窗口事件研究"].iloc[0]
    rdd_row = identification_scope.loc[identification_scope["分析层"] == "中国 RDD 扩展"].iloc[0]
    matched_rate = (diagnostics["status"] == "matched").mean()
    sector_relaxed_rate = diagnostics["sector_relaxed"].fillna(False).astype(bool).mean()

    summary_cards = [
        {
            "kicker": "样本期",
            "title": "结果主要反映 2024 至 2025 年的纳入批次",
            "meta": f'事件样本 {event_row["起始日期"]} 至 {event_row["结束日期"]}',
            "copy": "当前真实事件主要集中在较新的指数调整批次，结论更适合作为近期制度环境下的证据，而不应直接外推到更早时期的长期历史平均效应。",
            "foot": f'价格与基准收益分别覆盖到 {price_row["结束日期"]} 与 {benchmark_row["结束日期"]}，用于构造事件窗口与市场调整收益。',
        },
        {
            "kicker": "识别范围",
            "title": "事件研究、匹配回归与 RDD 分别回答不同问题",
            "meta": f"匹配成功率 {_format_share(matched_rate)}",
            "copy": str(short_id_row["当前口径"]),
            "foot": f'当前匹配回归面板共 {int(matched_row["观测值"]):,} 条观测值；中国 RDD 扩展目前的证据状态为“{rdd_row["证据状态"]}”。',
        },
        {
            "kicker": "数据口径",
            "title": "公开数据足以支撑课程论文，但并不等同官方成分股数据库",
            "meta": f"行业口径放宽占比 {_format_share(sector_relaxed_rate)}",
            "copy": "当前项目优先使用公开可得数据构造价格、成交量、换手率与市值口径，因此适合课程论文、研究展示与方法演示，但不应被表述为交易所官方历史精确口径。",
            "foot": "这一边界主要影响对机制强度和匹配精度的解释，不会改变“不同市场与事件阶段存在明显异质性”这一一级结论。",
        },
    ]

    scope_table = pd.DataFrame(
        [
            {"模块": "真实事件样本", "范围": f'{event_row["起始日期"]} 至 {event_row["结束日期"]}', "说明": "以真实纳入事件为基础，覆盖中国 A 股与美国两个市场。"},
            {"模块": "价格数据", "范围": f'{price_row["起始日期"]} 至 {price_row["结束日期"]}', "说明": "用于构造事件窗口、异常收益与机制变量。"},
            {"模块": "基准指数数据", "范围": f'{benchmark_row["起始日期"]} 至 {benchmark_row["结束日期"]}', "说明": "用于市场调整收益与异常收益计算。"},
            {"模块": "匹配回归", "范围": f"匹配成功率 {_format_share(matched_rate)}", "说明": "对照组构造总体稳定，但仍存在少量无法匹配的事件。"},
            {"模块": "RDD 扩展", "范围": str(rdd_row["证据状态"]), "说明": str(rdd_row["当前口径"])},
        ]
    )

    return {
        "summary": "明确研究边界的目的，不是削弱结果，而是让结论与样本期、识别设计、数据来源保持一致，从而提升整套展示的可信度。",
        "summary_cards": summary_cards,
        "tables": [
            {"label": "样本与数据范围", "html": _render_table(scope_table, compact=True), "layout_class": "wide"},
            {"label": "识别范围说明", "html": _render_table(identification_scope, compact=True), "layout_class": "wide"},
        ],
    }


@app.route("/")
def home():
    display_mode = _dashboard_mode()
    demo_mode = display_mode != "full"
    track_sections = []
    for analysis_id in ANALYSES:
        section = _load_or_build_track_section(analysis_id)
        section["anchor"] = analysis_id
        section["notes"] = _build_track_notes(analysis_id)
        section = _prepare_track_display(section, analysis_id, demo_mode)
        track_sections.append(section)
    if display_mode != "brief":
        framework_section = RUN_CACHE.get("paper_framework") or _load_literature_framework_result()
        framework_section = _prepare_framework_display(framework_section, demo_mode)
        RUN_CACHE["paper_framework"] = framework_section
        supplement_section = RUN_CACHE.get("project_supplement") or _load_supplement_result()
        supplement_section = _prepare_supplement_display(supplement_section, demo_mode)
        RUN_CACHE["project_supplement"] = supplement_section
    else:
        framework_section = {"display_summary": "", "display_tables": [], "summary_cards": []}
        supplement_section = {"display_summary": "", "display_tables": [], "summary_cards": []}
    design_section = _build_sample_design_section()
    return render_template_string(
        HOME_TEMPLATE,
        mode=display_mode,
        nav_sections=_nav_sections_for_mode(display_mode),
        mode_tabs=_mode_tabs_for_mode(display_mode),
        overview_metrics=_build_overview_metrics(),
        overview_notes=_build_overview_notes_for_mode(display_mode),
        overview_summary=_build_overview_summary_for_mode(display_mode),
        cta_copy=_build_cta_copy_for_mode(display_mode),
        abstract_lead=_build_abstract_lead(),
        abstract_points=_build_abstract_points(),
        highlights=_build_highlights(),
        design_section=design_section,
        track_sections=track_sections,
        framework_section=framework_section,
        supplement_section=supplement_section,
        limits_section=_build_limits_section(),
    )


@app.post("/refresh")
def refresh_dashboard():
    _run_and_cache_all()
    mode = _dashboard_mode()
    redirect_kwargs: dict[str, str] = {"mode": mode}
    anchor = _normalize_anchor_for_mode(mode, request.form.get("anchor", ""))
    if anchor:
        redirect_kwargs["_anchor"] = anchor
    return redirect(url_for("home", **redirect_kwargs))


@app.post("/run/<analysis_id>")
def run_analysis(analysis_id: str):
    config = ANALYSES.get(analysis_id)
    if not config:
        abort(404)
    raw = config["runner"](verbose=False)
    current = _normalize_result(raw)
    current["id"] = config.get("project_module", analysis_id)
    current["title"] = config["title"]
    current["description"] = raw.get("description", config["description_zh"])
    current["subtitle"] = config["subtitle"]
    current = _attach_project_track_context(current, config)
    RUN_CACHE[analysis_id] = current
    return redirect(url_for("home", _anchor=analysis_id))


@app.get("/library")
def show_library():
    return redirect(url_for("home", _anchor="framework"))


@app.get("/review")
def show_review():
    return redirect(url_for("home", _anchor="framework"))


@app.get("/framework")
def show_framework():
    return redirect(url_for("home", _anchor="framework"))


@app.get("/supplement")
def show_supplement():
    return redirect(url_for("home", _anchor="supplement"))


@app.get("/analysis/<analysis_id>")
def show_analysis(analysis_id: str):
    if analysis_id not in ANALYSES:
        abort(404)
    return redirect(url_for("home", _anchor=analysis_id))


@app.get("/files/<path:subpath>")
def serve_result_file(subpath: str):
    full_path = (ROOT / subpath).resolve()
    root = ROOT.resolve()
    if root not in full_path.parents and full_path != root:
        abort(404)
    if not full_path.exists() or not full_path.is_file():
        abort(404)
    return send_file(full_path)


@app.get("/paper/<paper_id>")
def show_paper_brief(paper_id: str):
    current = _load_paper_detail_result(paper_id)
    if current is None:
        abort(404)
    return render_template_string(PAPER_TEMPLATE, current=current)


@app.get("/paper/<paper_id>/pdf")
def serve_library_pdf(paper_id: str):
    paper = get_literature_paper(paper_id)
    if paper is None or not paper.exists:
        abort(404)
    return send_file(paper.pdf_path)


def main() -> None:
    print("正在启动文献分析界面：http://127.0.0.1:5001")
    app.run(host="127.0.0.1", port=5001, debug=False)


if __name__ == "__main__":
    main()
