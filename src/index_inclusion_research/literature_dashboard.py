from __future__ import annotations

from index_inclusion_research import dashboard_cli
from index_inclusion_research.dashboard_app import app


parse_dashboard_args = dashboard_cli.parse_dashboard_args


def main(argv: list[str] | None = None) -> None:
    dashboard_cli.run_dashboard_app(app, argv)


if __name__ == "__main__":
    main()
