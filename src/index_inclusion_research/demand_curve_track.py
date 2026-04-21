from __future__ import annotations

from index_inclusion_research.shleifer import run_analysis as run_shleifer_analysis


def run_analysis(verbose: bool = True) -> dict[str, object]:
    result = run_shleifer_analysis(verbose=verbose)
    result["id"] = "demand_curve_track"
    result["title"] = "需求曲线与长期保留"
    result["description"] = "以正方机制文献为底，检验长窗口 CAR、保留率与需求曲线向下倾斜。"
    result["subtitle"] = "Demand Curves & Long-run Retention"
    return result


def main(argv: list[str] | None = None) -> None:
    del argv
    run_analysis(verbose=True)


if __name__ == "__main__":
    main()
