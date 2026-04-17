from __future__ import annotations

import argparse


def parse_dashboard_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch the literature dashboard.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind the Flask app to.")
    parser.add_argument("--port", type=int, default=5001, help="Port to bind the Flask app to.")
    return parser.parse_args(argv)


def run_dashboard_app(app, argv: list[str] | None = None) -> None:
    args = parse_dashboard_args(argv)
    display_host = "localhost" if args.host == "127.0.0.1" else args.host
    print(f"正在启动文献分析界面：http://{display_host}:{args.port}")
    if args.host == "127.0.0.1":
        print(f"建议优先使用 http://localhost:{args.port} 打开界面；Firefox 对 localhost 的兼容性更稳定。")
    app.run(host=args.host, port=args.port, debug=False)
