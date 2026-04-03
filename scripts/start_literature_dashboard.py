from __future__ import annotations

import os
import sys
from pathlib import Path

from flask import Flask, abort, redirect, render_template_string, send_file, url_for

os.environ.setdefault("MPLBACKEND", "Agg")

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
SRC = ROOT / "src"
for path in [SCRIPTS, SRC]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from start_harris_gurel import run_analysis as run_harris_gurel
from start_hs300_rdd import run_analysis as run_hs300_rdd
from start_hs300_style import run_analysis as run_hs300_style
from start_shleifer import run_analysis as run_shleifer
from index_inclusion_research.literature_catalog import (
    build_grouped_literature_frame,
    build_literature_dashboard_frame,
    build_literature_review_markdown,
    build_literature_summary_frame,
    build_literature_summary_markdown,
    build_project_track_frame,
    build_project_track_markdown,
    get_literature_paper,
)


def run_hs300_paper(verbose: bool = False) -> dict[str, object]:
    style_result = run_hs300_style(verbose=verbose)
    rdd_result = run_hs300_rdd(verbose=verbose)
    style_summary = ""
    rdd_summary = ""
    if isinstance(style_result.get("summary_path"), Path) and style_result["summary_path"].exists():
        style_summary = style_result["summary_path"].read_text(encoding="utf-8").strip()
    if isinstance(rdd_result.get("summary_path"), Path) and rdd_result["summary_path"].exists():
        rdd_summary = rdd_result["summary_path"].read_text(encoding="utf-8").strip()

    combined_tables: dict[str, object] = {}
    for label, frame in style_result.get("tables", {}).items():
        combined_tables[f"风格识别：{label}"] = frame
    for label, frame in rdd_result.get("tables", {}).items():
        combined_tables[f"断点回归：{label}"] = frame

    combined_figures: list[dict[str, object]] = []
    for path in style_result.get("figures", []):
        combined_figures.append({"path": path, "prefix": "风格识别"})
    for path in rdd_result.get("figures", []):
        combined_figures.append({"path": path, "prefix": "断点回归"})

    summary_text = "\n\n".join(
        [
            "# 制度识别与中国市场证据结果包",
            "",
            "这条研究主线用中性文献与中国市场文献组织识别策略，在同一页里同时展示匹配对照组结果与断点回归扩展结果。",
            "",
            "## 第一部分：风格识别",
            style_summary or "暂无风格识别摘要。",
            "",
            "## 第二部分：断点回归",
            rdd_summary or "暂无断点回归摘要。",
        ]
    ).strip()

    return {
        "id": "hs300_paper",
        "title": "沪深300 论文复现",
        "description": "第三篇中文论文的风格识别结果与断点回归结果",
        "subtitle": "HS300 Paper",
        "summary_text": summary_text,
        "tables": combined_tables,
        "figures": combined_figures,
        "output_dir": ROOT / "results" / "literature",
    }


ANALYSES = {
    "harris_gurel": {
        "title": "短期价格压力与效应减弱",
        "subtitle": "Price Pressure & Disappearing Effect",
        "description_zh": "以反方文献和早期事件研究证据为底，检验短窗口 CAR、成交量冲击和效应减弱问题",
        "project_module": "短期价格压力",
        "runner": run_harris_gurel,
    },
    "shleifer": {
        "title": "需求曲线与长期保留",
        "subtitle": "Demand Curves & Long-run Retention",
        "description_zh": "以正方机制文献为底，检验价格是否只部分回吐以及需求曲线是否向下倾斜",
        "project_module": "需求曲线效应",
        "runner": run_shleifer,
    },
    "hs300_paper": {
        "title": "制度识别与中国市场证据",
        "subtitle": "Identification & China Evidence",
        "description_zh": "以中性文献和中国市场证据为底，整合匹配对照组、DID 风格分析和 RDD 扩展",
        "project_module": "沪深300论文复现",
        "runner": run_hs300_paper,
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

RUN_CACHE: dict[str, dict[str, object]] = {}

TABLE_LABELS = {
    "event_study_summary": "事件研究汇总表",
    "mechanism_summary": "机制变量汇总表",
    "retention_summary": "保留率汇总表",
    "did_summary": "DID 汇总表",
    "regression_coefficients": "回归系数表",
    "regression_models": "模型统计表",
    "regression_dataset": "回归样本表",
    "match_diagnostics": "匹配诊断表",
    "rdd_summary": "RDD 汇总表",
    "event_level_with_running": "事件层运行变量样本表",
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

APP_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>指数纳入文献分析面板</title>
  <style>
    :root {
      --bg: #f7f1e8;
      --card: #fffaf2;
      --ink: #1f2a37;
      --muted: #52606d;
      --line: #d9c9b1;
      --accent: #005f73;
      --accent-2: #ae2012;
      --shadow: 0 18px 40px rgba(42, 36, 26, 0.08);
    }
    * { box-sizing: border-box; }
    html, body {
      max-width: 100%;
      overflow-x: hidden;
    }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(0,95,115,0.08), transparent 26%),
        radial-gradient(circle at top right, rgba(174,32,18,0.08), transparent 24%),
        linear-gradient(180deg, #f9f4ed 0%, var(--bg) 100%);
    }
    a { color: var(--accent); text-decoration: none; }
    .page {
      width: min(1280px, 100%);
      margin: 24px auto 56px;
      padding: 0 16px;
    }
    .hero, .panel, .analysis-card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
    }
    .hero {
      padding: 28px 30px;
      margin-bottom: 18px;
    }
    .hero h1 {
      margin: 0 0 10px;
      font-size: clamp(30px, 4vw, 48px);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }
    .hero p {
      margin: 0;
      max-width: 880px;
      color: var(--muted);
      font-size: 17px;
      line-height: 1.6;
    }
    .sidebar-title {
      margin: 2px 0 0;
      color: #7a6d5a;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-weight: 700;
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(260px, 320px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
      min-width: 0;
    }
    .sidebar {
      display: grid;
      gap: 16px;
      position: sticky;
      top: 18px;
      min-width: 0;
    }
    .analysis-card {
      padding: 18px;
    }
    .analysis-card h3 {
      margin: 0 0 6px;
      font-size: 22px;
    }
    .analysis-card .subtle {
      margin: 0 0 8px;
      font-size: 13px;
      color: #7a6d5a;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .analysis-card p {
      margin: 0 0 14px;
      color: var(--muted);
      line-height: 1.5;
    }
    .btn-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .btn {
      appearance: none;
      border: 1px solid var(--accent);
      background: var(--accent);
      color: white;
      padding: 10px 14px;
      border-radius: 999px;
      font-size: 14px;
      cursor: pointer;
    }
    .btn.secondary {
      background: transparent;
      color: var(--accent);
    }
    .panel {
      padding: 24px;
      min-height: 70vh;
      min-width: 0;
      overflow: hidden;
    }
    .panel h2 {
      margin: 0 0 8px;
      font-size: 28px;
      letter-spacing: -0.02em;
    }
    .kicker {
      color: var(--accent-2);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      margin-bottom: 8px;
      font-weight: 700;
    }
    .meta {
      color: var(--muted);
      margin-bottom: 18px;
      line-height: 1.6;
    }
    .section {
      margin-top: 24px;
    }
    .section h3 {
      margin: 0 0 12px;
      font-size: 20px;
    }
    .notice {
      background: rgba(0,95,115,0.08);
      border: 1px solid rgba(0,95,115,0.16);
      padding: 14px 16px;
      border-radius: 16px;
      color: var(--ink);
      line-height: 1.55;
    }
    .md {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px 18px;
      line-height: 1.65;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-size: 15px;
    }
    .figure-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      min-width: 0;
    }
    .figure-card {
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 18px;
      padding: 16px;
    }
    .figure-card img {
      width: 100%;
      height: auto;
      display: block;
      border-radius: 12px;
      border: 1px solid #eadfce;
      background: #fff;
    }
    .figure-card .caption {
      margin-top: 10px;
      font-size: 14px;
      color: var(--muted);
      word-break: break-all;
    }
    .table-wrap {
      width: 100%;
      max-width: 100%;
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
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
      position: sticky;
      top: 0;
      background: #fbf7f1;
      z-index: 1;
    }
    .empty {
      color: var(--muted);
      font-style: italic;
    }
    .foot {
      margin-top: 18px;
      font-size: 13px;
      color: var(--muted);
      overflow-wrap: anywhere;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .sidebar { position: static; }
      .page {
        margin: 16px auto 36px;
        padding: 0 12px;
      }
      .hero, .panel, .analysis-card {
        border-radius: 18px;
      }
      .hero {
        padding: 22px 20px;
      }
      .panel {
        padding: 18px;
      }
      .analysis-card {
        padding: 16px;
      }
      .figure-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>指数纳入文献分析面板</h1>
      <p>这个界面以 16 篇指数效应文献为底，抽出三条研究主线。你可以直接运行分析，并在同一页里同时看到支撑文献、结果表和图表解释。</p>
    </section>
    <div class="grid">
      <aside class="sidebar">
        <div class="sidebar-title">分析入口</div>
        {% for key, item in analyses.items() %}
        <div class="analysis-card">
          <h3>{{ item.title }}</h3>
          <div class="subtle">{{ item.subtitle }}</div>
          <p>{{ item.description_zh }}</p>
          <div class="btn-row">
            <form method="post" action="{{ url_for('run_analysis', analysis_id=key) }}">
              <button class="btn" type="submit">运行并刷新</button>
            </form>
            <a class="btn secondary" href="{{ url_for('show_analysis', analysis_id=key) }}">查看结果</a>
          </div>
        </div>
        {% endfor %}
        <div class="sidebar-title">参考文献</div>
        <div class="analysis-card">
          <h3>{{ library_card.title }}</h3>
          <div class="subtle">{{ library_card.subtitle }}</div>
          <p>{{ library_card.description_zh }}</p>
          <div class="btn-row">
            <a class="btn secondary" href="{{ url_for('show_library') }}">查看文献</a>
          </div>
        </div>
        <div class="analysis-card">
          <h3>{{ review_card.title }}</h3>
          <div class="subtle">{{ review_card.subtitle }}</div>
          <p>{{ review_card.description_zh }}</p>
          <div class="btn-row">
            <a class="btn secondary" href="{{ url_for('show_review') }}">查看综述</a>
          </div>
        </div>
      </aside>
      <main class="panel">
        {% if current %}
          <div class="kicker">{{ current.id }}</div>
          <h2>{{ current.title }}</h2>
          <div class="meta">
            <div>{{ current.description }}</div>
            {% if current.subtitle %}
            <div style="font-size: 14px; color: #7a6d5a; text-transform: uppercase; letter-spacing: 0.05em;">{{ current.subtitle }}</div>
            {% endif %}
          </div>
          {% if current.summary_text %}
          <div class="section">
            <h3>摘要说明</h3>
            <div class="md">{{ current.summary_text }}</div>
          </div>
          {% endif %}
          <div class="section">
            <h3>结果表</h3>
            {% for label, html_table in current.rendered_tables %}
              <h4>{{ label }}</h4>
              <div class="table-wrap">{{ html_table|safe }}</div>
            {% else %}
              <div class="empty">暂无表格结果。</div>
            {% endfor %}
          </div>
          <div class="section">
            <h3>图表</h3>
            {% if current.figure_paths %}
              <div class="figure-grid">
                {% for figure in current.figure_paths %}
                <div class="figure-card">
                  <img src="{{ url_for('serve_result_file', subpath=figure.path) }}" alt="{{ figure.caption }}">
                  <div class="caption">{{ figure.caption }}</div>
                </div>
                {% endfor %}
              </div>
            {% else %}
              <div class="empty">暂无图表。</div>
            {% endif %}
          </div>
          <div class="foot">输出目录：{{ current.output_dir }}</div>
        {% else %}
          <div class="notice">
            先从左侧选择一条研究主线。点击“运行并刷新”后，界面会自动展示这条主线对应的支撑文献、图表和数据表。
          </div>
        {% endif %}
      </main>
    </div>
  </div>
</body>
</html>
"""


app = Flask(__name__)


def _safe_relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def _render_table(frame) -> str:
    display = frame.copy()
    if len(display) > 120:
        display = display.head(120)
    display = display.rename(columns={column: COLUMN_LABELS.get(column, column) for column in display.columns})
    for column in display.columns:
        if display[column].dtype == object:
            display[column] = display[column].replace(VALUE_LABELS)
    return display.to_html(
        index=False,
        classes=["dataframe"],
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
    if stem.endswith("_car_path"):
        parts = stem.split("_")
        if len(parts) >= 3:
            market = COLUMN_LABELS.get(parts[0].upper(), parts[0].upper())
            phase = COLUMN_LABELS.get(parts[1], parts[1])
            return f"{market}{phase}平均异常收益路径图"
    return stem.replace("_", " ")


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
                    "caption": _format_figure_caption(item),
                }
            )
        elif isinstance(item, dict) and isinstance(item.get("path"), Path):
            prefix = f"{item['prefix']}：" if item.get("prefix") else ""
            figure_paths.append(
                {
                    "path": _safe_relative(item["path"]),
                    "caption": f"{prefix}{_format_figure_caption(item['path'])}",
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
    support_table = ("支撑文献", _render_table(build_project_track_frame(project_module)))
    current["rendered_tables"] = [support_table, *current.get("rendered_tables", [])]
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


def _load_hs300_paper_saved_result() -> dict[str, object]:
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
        figure_paths.append({"path": _safe_relative(path), "caption": f"风格识别：{_format_figure_caption(path)}"})
    for path in sorted(rdd_dir.rglob("*.png")):
        figure_paths.append({"path": _safe_relative(path), "caption": f"断点回归：{_format_figure_caption(path)}"})

    summary_text = "\n\n".join(
        [
            "# 第三篇论文结果包",
            "",
            "这个页面把第三篇中文论文相关结果合并到同一页中，便于直接比较两类识别策略。",
            "",
            "## 第一部分：风格识别",
            style_summary,
            "",
            "## 第二部分：断点回归",
            rdd_summary,
        ]
    ).strip()

    return {
        "id": "hs300_paper",
        "title": ANALYSES["hs300_paper"]["title"],
        "description": ANALYSES["hs300_paper"]["description_zh"],
        "subtitle": ANALYSES["hs300_paper"]["subtitle"],
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
        "rendered_tables": [
            ("文献分组统计", _render_table(build_literature_summary_frame())),
            ("文献目录", _render_table(build_literature_dashboard_frame())),
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
        "rendered_tables": [
            ("反方文献", _render_table(build_grouped_literature_frame("反方"))),
            ("中性文献", _render_table(build_grouped_literature_frame("中性"))),
            ("正方文献", _render_table(build_grouped_literature_frame("正方"))),
        ],
        "figure_paths": [],
        "output_dir": "docs",
    }


@app.route("/")
def home():
    current = None
    if RUN_CACHE:
        last_key = next(reversed(RUN_CACHE))
        current = RUN_CACHE[last_key]
    return render_template_string(
        APP_TEMPLATE,
        analyses=ANALYSES,
        library_card=LIBRARY_CARD,
        review_card=REVIEW_CARD,
        current=current,
    )


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
    return redirect(url_for("show_analysis", analysis_id=analysis_id))


@app.get("/library")
def show_library():
    current = RUN_CACHE.get("paper_library")
    if current is None:
        current = _load_literature_library_result()
        RUN_CACHE["paper_library"] = current
    return render_template_string(
        APP_TEMPLATE,
        analyses=ANALYSES,
        library_card=LIBRARY_CARD,
        review_card=REVIEW_CARD,
        current=current,
    )


@app.get("/review")
def show_review():
    current = RUN_CACHE.get("paper_review")
    if current is None:
        current = _load_literature_review_result()
        RUN_CACHE["paper_review"] = current
    return render_template_string(
        APP_TEMPLATE,
        analyses=ANALYSES,
        library_card=LIBRARY_CARD,
        review_card=REVIEW_CARD,
        current=current,
    )


@app.get("/analysis/<analysis_id>")
def show_analysis(analysis_id: str):
    config = ANALYSES.get(analysis_id)
    if not config:
        abort(404)
    current = RUN_CACHE.get(analysis_id)
    if current is None:
        if analysis_id == "hs300_paper":
            current = _load_hs300_paper_saved_result()
            current = _attach_project_track_context(current, config)
            return render_template_string(
                APP_TEMPLATE,
                analyses=ANALYSES,
                library_card=LIBRARY_CARD,
                review_card=REVIEW_CARD,
                current=current,
            )
        output_dir = ROOT / "results" / "literature" / analysis_id
        summary_path = output_dir / "summary.md"
        if summary_path.exists():
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
                        "caption": _format_figure_caption(path),
                    }
                    for path in sorted(output_dir.rglob("*.png"))
                ],
                "output_dir": _safe_relative(output_dir),
            }
            current = _attach_project_track_context(current, config)
        else:
            current = None
    return render_template_string(
        APP_TEMPLATE,
        analyses=ANALYSES,
        library_card=LIBRARY_CARD,
        review_card=REVIEW_CARD,
        current=current,
    )


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
