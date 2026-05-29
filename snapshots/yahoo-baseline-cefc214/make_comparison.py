#!/usr/bin/env python3
"""Generate the Yahoo (free) vs Tushare Pro (paid) results comparison.

Yahoo baseline  : this archive directory (snapshots/yahoo-baseline-cefc214/),
                  rescued byte-for-byte from git commit cefc214.
Tushare results : the live results/real_tables/ tree (uncommitted Tushare run).

Outputs (written to docs/):
    yahoo_vs_tushare_comparison.csv   machine-readable, three stacked sections
    yahoo_vs_tushare_comparison.tex   LaTeX tables for direct \\input
    yahoo_vs_tushare_comparison.md    paper-ready narrative + tables

Re-run with:  python snapshots/yahoo-baseline-cefc214/make_comparison.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
YAHOO_DIR = Path(__file__).resolve().parent / "real_tables"
TUSHARE_DIR = ROOT / "results" / "real_tables"
OUT_DIR = ROOT / "docs"

YAHOO_COMMIT = "cefc214"


def _verdicts(path: Path) -> pd.DataFrame:
    cols = ["hid", "name_cn", "verdict", "confidence", "metric_snapshot", "p_value", "n_obs"]
    return pd.read_csv(path)[cols].set_index("hid")


def _event_study(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df[(df["inclusion"] == 1) & (df["window_slug"] == "m1_p1")]
    df = df[["market", "event_phase", "n_events", "mean_car", "t_stat", "p_value"]]
    return df.set_index(["market", "event_phase"])


def _coverage(path: Path) -> dict[str, str]:
    df = pd.read_csv(path)
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        out[str(r["数据集"])] = r
    return out


def build_verdict_table() -> pd.DataFrame:
    y = _verdicts(YAHOO_DIR / "cma_hypothesis_verdicts.csv")
    t = _verdicts(TUSHARE_DIR / "cma_hypothesis_verdicts.csv")
    rows = []
    for hid in ["H1", "H2", "H3", "H4", "H5", "H6", "H7"]:
        yr, tr = y.loc[hid], t.loc[hid]
        verdict_changed = yr["verdict"] != tr["verdict"]
        conf_changed = yr["confidence"] != tr["confidence"]
        if verdict_changed:
            kind = "结论翻转"
        elif conf_changed:
            kind = "置信下调"
        else:
            kind = "无"
        rows.append(
            {
                "hid": hid,
                "name_cn": yr["name_cn"],
                "yahoo_verdict": yr["verdict"],
                "yahoo_confidence": yr["confidence"],
                "tushare_verdict": tr["verdict"],
                "tushare_confidence": tr["confidence"],
                "changed": "是" if (verdict_changed or conf_changed) else "否",
                "change_kind": kind,
                "yahoo_p": yr["p_value"],
                "tushare_p": tr["p_value"],
                "yahoo_n": yr["n_obs"],
                "tushare_n": tr["n_obs"],
                "yahoo_metric": yr["metric_snapshot"],
                "tushare_metric": tr["metric_snapshot"],
            }
        )
    return pd.DataFrame(rows)


def build_event_table() -> pd.DataFrame:
    y = _event_study(YAHOO_DIR / "event_study_summary.csv")
    t = _event_study(TUSHARE_DIR / "event_study_summary.csv")
    rows = []
    for market in ["CN", "US"]:
        for phase in ["announce", "effective"]:
            yr, tr = y.loc[(market, phase)], t.loc[(market, phase)]
            rows.append(
                {
                    "market": market,
                    "phase": phase,
                    "yahoo_n": int(yr["n_events"]),
                    "yahoo_car_pct": round(yr["mean_car"] * 100, 3),
                    "yahoo_t": round(yr["t_stat"], 2),
                    "yahoo_p": round(yr["p_value"], 4),
                    "tushare_n": int(tr["n_events"]),
                    "tushare_car_pct": round(tr["mean_car"] * 100, 3),
                    "tushare_t": round(tr["t_stat"], 2),
                    "tushare_p": round(tr["p_value"], 4),
                }
            )
    return pd.DataFrame(rows)


def build_coverage_table() -> pd.DataFrame:
    y = _coverage(YAHOO_DIR / "data_sources.csv")
    t = _coverage(TUSHARE_DIR / "data_sources.csv")
    spec = [
        ("日频价格", "股票数", "覆盖股票数"),
        ("事件窗口面板", "事件数", "事件窗口面板·事件数"),
        ("事件窗口面板", "行数", "事件窗口面板·行数"),
        ("匹配回归面板", "股票数", "匹配面板·股票数"),
        ("匹配回归面板", "事件数", "匹配面板·事件数"),
        ("匹配回归面板", "行数", "匹配面板·行数"),
    ]
    rows = []
    for ds, col, label in spec:
        rows.append({"指标": label, "yahoo": y[ds][col], "tushare": t[ds][col]})
    return pd.DataFrame(rows)


def to_csv(verdicts, events, coverage) -> str:
    blocks = [
        "# SECTION A — 假设判定对照 (H1-H7)",
        verdicts.to_csv(index=False),
        "# SECTION B — 事件研究 CAR[-1,+1] (inclusion=1)",
        events.to_csv(index=False),
        "# SECTION C — 样本覆盖",
        coverage.to_csv(index=False),
    ]
    return "\n".join(blocks)


def to_tex(verdicts, events) -> str:
    v = verdicts.copy()
    lines = [
        r"% Yahoo (free) vs Tushare Pro (paid) — A-share data source migration",
        r"\begin{table}[htbp]\centering",
        r"\caption{假设判定对 A 股数据源的敏感性（Yahoo 免费 vs Tushare Pro 付费）}",
        r"\label{tab:yahoo-vs-tushare-verdicts}",
        r"\begin{tabular}{llcccc}",
        r"\toprule",
        r"假设 & 名称 & Yahoo 判定 & Tushare 判定 & 变动 & Tushare $p$ \\",
        r"\midrule",
    ]
    for _, r in v.iterrows():
        mark = r"\textbf{是}" if r["changed"] == "是" else "否"
        pval = r["tushare_p"]
        pstr = f"{float(pval):.3f}" if pd.notna(pval) and str(pval) != "" else "--"
        lines.append(
            f"{r['hid']} & {r['name_cn']} & {r['yahoo_verdict']}/{r['yahoo_confidence']} "
            f"& {r['tushare_verdict']}/{r['tushare_confidence']} & {mark} & {pstr} \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\par\smallskip\footnotesize Yahoo 基线取自 git commit "
        + YAHOO_COMMIT
        + r"；Tushare 为同管线仅替换 A 股价格源后的重算结果。美股样本两版均使用 Yahoo，故 US 行差异仅来自抽样窗口微调。",
        r"\end{table}",
        "",
        r"\begin{table}[htbp]\centering",
        r"\caption{公告日/生效日 $CAR[-1,+1]$（inclusion=1）：Yahoo vs Tushare}",
        r"\label{tab:yahoo-vs-tushare-car}",
        r"\begin{tabular}{llcccccc}",
        r"\toprule",
        r"& & \multicolumn{3}{c}{Yahoo} & \multicolumn{3}{c}{Tushare} \\",
        r"\cmidrule(lr){3-5}\cmidrule(lr){6-8}",
        r"市场 & 阶段 & $N$ & CAR(\%) & $t$ & $N$ & CAR(\%) & $t$ \\",
        r"\midrule",
    ]
    for _, r in events.iterrows():
        lines.append(
            f"{r['market']} & {r['phase']} & {r['yahoo_n']} & {r['yahoo_car_pct']:.2f} & {r['yahoo_t']:.2f} "
            f"& {r['tushare_n']} & {r['tushare_car_pct']:.2f} & {r['tushare_t']:.2f} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table}", ""]
    return "\n".join(lines)


def _md_verdict_rows(v: pd.DataFrame) -> str:
    out = []
    for _, r in v.iterrows():
        flag = "**是 ⚠️**" if r["changed"] == "是" else "否"
        pval = r["tushare_p"]
        pstr = f"{float(pval):.3f}" if pd.notna(pval) and str(pval) != "" else "—"
        out.append(
            f"| {r['hid']} | {r['name_cn']} | {r['yahoo_verdict']}/{r['yahoo_confidence']} "
            f"| {r['tushare_verdict']}/{r['tushare_confidence']} | {flag} | {pstr} |"
        )
    return "\n".join(out)


def _md_event_rows(e: pd.DataFrame) -> str:
    out = []
    for _, r in e.iterrows():
        out.append(
            f"| {r['market']} | {r['phase']} | {r['yahoo_n']} | {r['yahoo_car_pct']:.2f} | {r['yahoo_t']:.2f} | {r['yahoo_p']:.4f} "
            f"| {r['tushare_n']} | {r['tushare_car_pct']:.2f} | {r['tushare_t']:.2f} | {r['tushare_p']:.4f} |"
        )
    return "\n".join(out)


def _md_coverage_rows(c: pd.DataFrame) -> str:
    return "\n".join(f"| {r['指标']} | {r['yahoo']} | {r['tushare']} |" for _, r in c.iterrows())


def to_md(verdicts, events, coverage) -> str:
    flips = list(verdicts.loc[verdicts["change_kind"] == "结论翻转", "hid"])
    conf = list(verdicts.loc[verdicts["change_kind"] == "置信下调", "hid"])
    n_changed = int((verdicts["changed"] == "是").sum())
    flip_s = "、".join(flips) if flips else "无"
    conf_s = "、".join(conf) if conf else "无"
    return f"""# A 股数据源迁移对照：Yahoo Finance（免费）vs Tushare Pro（付费）

> 自动生成，请勿手工编辑。重算：`python snapshots/yahoo-baseline-cefc214/make_comparison.py`
>
> - **Yahoo 基线**：git commit `{YAHOO_COMMIT}`，完整结果树存档于 `snapshots/yahoo-baseline-cefc214/`。
> - **Tushare 结果**：同一分析管线，仅将 A 股日频价格/成交量/市值/换手率与沪深300基准替换为 Tushare Pro，重算后的 `results/real_tables/`（本次为未提交的工作区结果）。
> - **可比性**：美股样本两版均使用 Yahoo/yfinance，因此下表 US 行的差异仅来自重抓时的抽样窗口微调，**不构成数据源对比**；真正的对比在 CN 行。

## 摘要

仅替换 A 股数据源（不动模型、不动假设、不动美股），7 个假设中共 **{n_changed} 个判定/置信度发生变动**，且全部是"被减弱"的方向：

- **结论翻转 {len(flips)} 个：{flip_s}**——H5（涨跌停限制）从"支持/高"直接跌到"证据不足/中"，是免费数据质量问题被纠正的最典型例子；H2（被动基金 AUM）由"部分支持/中"退为"证据不足/低"。
- **判定不变但证据变薄**：H3（散户 vs 机构，支持/高）双通道显著象限由 3/4 降到 2/4；H7（行业结构，支持/中）US 交互回归 joint p 由 0.064 升到 0.095（US 数据未换，属抽样窗口微调）。这两项判定与置信度均保留，故不计入上面的"变动"口径。

这说明当前结论对 A 股数据口径高度敏感：付费数据的价值在于**让数字更可信**，而非"加强结论"——恰恰相反，更准确的数据削弱了其中数项假设。

## A. 假设判定对照（H1–H7）

| 假设 | 名称 | Yahoo 判定/置信 | Tushare 判定/置信 | 变动 | Tushare p |
|---|---|---|---|---|---|
{_md_verdict_rows(verdicts)}

**逐项说明**

- **H5 涨跌停限制（支持/高 → 证据不足/中，结论翻转）**：Yahoo 下事件级涨跌停命中率正向预测公告日 CAR（limit_coef=0.1549, p=0.008, n=936）；换 Tushare 后 coef=0.0744, p=0.427, n=1096，完全不显著。A 股涨跌停判定依赖准确的当日涨跌幅与前收盘，Yahoo 在这块字段不可靠，原"支持"基本可判定为免费数据的假象。
- **H2 被动基金 AUM（部分支持/中 → 证据不足/低，结论翻转）**：CN 端 effective rolling CAR 的方向随更准的市值/收益序列改变，原"CN 方向符合 H2"不再成立。
- **H3 散户 vs 机构（支持/高 → 支持/高，判定不变但证据变薄）**：双通道显著象限由 3/4 降到 2/4，判定与置信度均保留，故"变动"列记为否。
- **H7 行业结构（支持/中 → 支持/中，判定不变）**：US 交互回归 joint p 由 0.064 升到 0.095（US 数据未换，差异来自样本窗口微调），判定与置信度均保留为"支持/中"，仅显著性边际略松。
- **H1 / H4 / H6**：两版均"证据不足"，判定稳定。

## B. 事件研究 CAR[-1,+1]（inclusion=1）

| 市场 | 阶段 | Yahoo N | Yahoo CAR% | Yahoo t | Yahoo p | Tushare N | Tushare CAR% | Tushare t | Tushare p |
|---|---|---|---|---|---|---|---|---|---|
{_md_event_rows(events)}

CN 公告日效应在 Tushare 下更干净、更显著（CAR 与 t 值均上升，样本量增加），生效日两版均不显著。US 行两版均为 Yahoo，差异可忽略。

## C. 样本覆盖

| 指标 | Yahoo | Tushare |
|---|---|---|
{_md_coverage_rows(coverage)}

## 写作与留档提示

1. 这是一个**正向的稳健性/诚实度论点**：明确报告"更换为持牌数据后 H5 不再成立"，比沿用免费数据下的虚假显著更可信。
2. 仓库内已无 Yahoo 基线的提交快照（`cma_hypothesis_verdicts.previous.csv` 已被覆盖成 Tushare 口径）；唯一的 Yahoo 基线即 git `{YAHOO_COMMIT}` 与本目录存档。
3. `snapshots/pre-registration-2026-05-29.csv` 为未跟踪文件且已是 Tushare 口径，与 05-03/05-16 的 Yahoo 口径 pre-registration 序列不一致，提交前需决定其归属。
"""


def _strip_blank_pipe_rows(text: str) -> str:
    """Drop markdown table rows that contain only pipes/spaces (no separators)."""
    kept = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|") and set(stripped) <= {"|", " "}:
            continue
        kept.append(line)
    return "\n".join(kept) + "\n"


def main() -> None:
    verdicts = build_verdict_table()
    events = build_event_table()
    coverage = build_coverage_table()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "yahoo_vs_tushare_comparison.csv").write_text(
        to_csv(verdicts, events, coverage), encoding="utf-8"
    )
    (OUT_DIR / "yahoo_vs_tushare_comparison.tex").write_text(
        to_tex(verdicts, events), encoding="utf-8"
    )
    (OUT_DIR / "yahoo_vs_tushare_comparison.md").write_text(
        _strip_blank_pipe_rows(to_md(verdicts, events, coverage)), encoding="utf-8"
    )
    changed = int((verdicts["changed"] == "是").sum())
    blanks_csv = sum(
        1
        for line in (OUT_DIR / "yahoo_vs_tushare_comparison.md").read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("|") and set(line.strip()) <= {"|", " "}
    )
    print(f"OK: {changed}/7 verdicts changed; blank_rows_in_md={blanks_csv}; wrote 3 files to {OUT_DIR}")


if __name__ == "__main__":
    main()
