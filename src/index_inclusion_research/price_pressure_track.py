from __future__ import annotations

from index_inclusion_research.harris_gurel import (
    run_analysis as run_harris_gurel_analysis,
)


def run_analysis(verbose: bool = True) -> dict[str, object]:
    result = run_harris_gurel_analysis(verbose=verbose)
    result["id"] = "price_pressure_track"
    result["title"] = "短期价格压力与效应减弱"
    result["description"] = "以反方文献和早期事件研究证据为底，检验短窗口 CAR、交易冲击与效应减弱。"
    result["subtitle"] = "Price Pressure & Disappearing Effect"
    return result


def main(argv: list[str] | None = None) -> None:
    del argv
    run_analysis(verbose=True)


if __name__ == "__main__":
    main()
