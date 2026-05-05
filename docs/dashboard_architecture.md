# Literature Dashboard Architecture

这份说明对应当前的 literature dashboard 主干实现，目标是帮助后续维护者快速理解：

- 应用是如何启动的
- 请求是如何从 Flask 路由流到页面上下文的
- `runtime / services / route bindings` 现在各自负责什么
- 改动某一类需求时，优先从哪一层下手

## 启动链路

推荐入口现在是：

```bash
index-inclusion-dashboard
```

启动路径如下：

1. [src/index_inclusion_research/cli.py](../src/index_inclusion_research/cli.py)
   `index-inclusion-dashboard` 会先走统一 CLI wrapper，再委托给包内 dashboard 启动模块。
2. [src/index_inclusion_research/literature_dashboard.py](../src/index_inclusion_research/literature_dashboard.py)
   负责解析 dashboard CLI 参数，并把启动职责交给已组装好的 Flask `app`。
3. [src/index_inclusion_research/dashboard_app.py](../src/index_inclusion_research/dashboard_app.py)
   负责 bootstrap 路径、构建全局 `dashboard_application`，并导出当前仍然有用的薄别名：
   `app`、`runtime`、`services`、`route_views`、`ANALYSES` 等。
4. [src/index_inclusion_research/dashboard_factory.py](../src/index_inclusion_research/dashboard_factory.py)
   负责定义 `DashboardShell` / `DashboardApplication`，并把 `shell`、`services`、`route_views` 和 Flask `app` 组装起来。

如果只是想回答"为什么打开这个页面会走到这里"，先从
`literature_dashboard.py -> dashboard_app.py -> dashboard_factory.py -> dashboard_route_bindings.py`
看起最快。

## 分层概览

当前主干大致分成七层：

1. `dashboard_bootstrap.py`
   负责计算 repo root / `src` / `web/templates` / `web/static`，并把 `src` 放入 `sys.path`。
2. `dashboard_app.py`
   进程内单例入口。只做 bootstrap、组装和显式导出。
3. `dashboard_factory.py`
   负责创建 `DashboardShell`：
   `analyses`、`runtime`、`refresh_coordinator`、`app` 都在这里生成。
   `DashboardApplication` 也已经并回这里，不再单独保留 `dashboard_application.py`。
4. `dashboard_services.py`
   request-aware 服务层。把 refresh 状态、mode 解析、URL 构造、runtime 调用收成显式方法。
5. `dashboard_runtime.py`
   dashboard 领域 façade。对外暴露明确的 `load_* / run_* / build_*` 表面。
6. `dashboard_route_bindings.py` + `dashboard_routes.py`
   路由装配和请求适配层。前者负责 wiring，后者负责 request parsing 和 view/handler。
7. `dashboard_page_runtime.py` + `dashboard_home.py`
   页面内容装配层。前者管理 outline / sections runtime，后者负责首页 context 的最终拼装。

可以把它粗略理解成：

`bootstrap -> app entrypoint -> factory/services/runtime -> route factory -> route handlers -> page builders -> templates`

## Runtime 结构

[src/index_inclusion_research/dashboard_runtime.py](../src/index_inclusion_research/dashboard_runtime.py)
现在是一个显式 façade，不再依赖 `__getattr__` 去把内部组件偷偷透出。

它内部组合了两部分：

- `track`: [dashboard_track_runtime.py](../src/index_inclusion_research/dashboard_track_runtime.py)
- `page`: [dashboard_page_runtime.py](../src/index_inclusion_research/dashboard_page_runtime.py)

### Track Runtime

这一层现在已经收口到
[dashboard_track_runtime.py](../src/index_inclusion_research/dashboard_track_runtime.py)，
不再保留 `dashboard_track_support_runtime.py` /
`dashboard_track_content_runtime.py` /
`dashboard_track_display_runtime.py` 这三个中间文件。

当前做法是：`DashboardTrackRuntime` 保持对外 façade 不变，但显式调用 helper 模块：

- `dashboard_formatting`
  负责 label、格式化、表格渲染、figure caption
- `dashboard_loaders`
  负责 saved result / CSV / RDD contract / manifest 读取
- `dashboard_presenters`
  负责 table tier、展示层切分与 display 装饰
- `dashboard_figures`
  负责页面附属 figure 生成
- `dashboard_refresh`
  负责 snapshot source 与 refresh meta
- `dashboard_content` / `dashboard_tracks`
  负责文献页、framework、supplement 和主线结果内容

所以现在更准确的理解是：
`DashboardTrackRuntime` 是“集中协调器”，而不是“再向下拆三层 runtime 文件”的薄代理。

### Page Runtime

`page` 侧仍然拆成两层：

- [dashboard_page_outline_runtime.py](../src/index_inclusion_research/dashboard_page_outline_runtime.py)
  放导航、mode tabs、overview 文案、abstract、highlights。
- [dashboard_page_sections_runtime.py](../src/index_inclusion_research/dashboard_page_sections_runtime.py)
  放首页 section 级内容：
  `design / robustness / limits / cross_market_asymmetry / home_context`。

`DashboardPageRuntime` 现在只是把 `outline` 和 `sections` 组合起来的薄 façade。

## 首页上下文装配

首页最大的内容拼装点现在在：

- [dashboard_home.py](../src/index_inclusion_research/dashboard_home.py)

其中：

- `build_overview_metrics()` 负责顶层指标
- `build_highlights()` 负责首页重点结论
- `DashboardHomeContextBuilder` 负责整个首页 context 的装配

`build_home_context(...)` 还保留着，但已经只是 `DashboardHomeContextBuilder(...).build(...)` 的薄包装。

如果你要改：

- 首页 section 的组合关系：优先看 `DashboardHomeContextBuilder`
- overview/highlight 文案逻辑：优先看 `dashboard_home.py`
- section 具体内容来源：优先看 `dashboard_page_sections_runtime.py`
- 如果是新增首页独立板块（例如 CMA section）：优先看
  `dashboard_page_sections_runtime.py` + `dashboard_home.py`

## 路由层

路由层现在拆成两步：

1. [dashboard_route_bindings.py](../src/index_inclusion_research/dashboard_route_bindings.py)
   负责显式 wiring。
   这里现在是 `DashboardRouteDependencies` + `DashboardRouteFactory`，不再依赖字符串 `getattr`。
2. [dashboard_routes.py](../src/index_inclusion_research/dashboard_routes.py)
   负责请求适配和 handler。
   这里现在有：
   `DashboardRequestAdapter`、
   `DashboardRefreshHandler`、
   `DashboardRefreshStatusHandler`、
   `DashboardRunAnalysisHandler`、
   `DashboardPaperRequestAdapter`。

可以把它理解成：

- `route_bindings` 解决“把哪些依赖接给哪个 view”
- `routes` 解决“某个 view 收到请求后怎么解析和返回响应”

另外，当前 `/library`、`/review`、`/framework`、`/supplement`、
`/analysis/<analysis_id>` 这些旧二级页入口已经主要作为锚点跳转保留；
真正的主入口仍然是首页 `/`。

## Refresh 状态

refresh 仍然是本地研究面板导向的实现：

- [dashboard_refresh.py](../src/index_inclusion_research/dashboard_refresh.py)
- [dashboard_refresh_coordinator.py](../src/index_inclusion_research/dashboard_refresh_coordinator.py)
- [dashboard_services.py](../src/index_inclusion_research/dashboard_services.py)

当前特点：

- 状态仍在进程内
- coordinator 负责 refresh state + lock + payload 计算
- services 负责把 request/time/runtime 组合进 refresh 逻辑

这对本地单进程使用是合适的；如果以后要走多用户或多 worker，需要把这一层升级成持久化任务状态。

## 配置与类型

- [dashboard_config.py](../src/index_inclusion_research/dashboard_config.py)
  负责 `analyses`、card、details panel keys 等静态配置。
- [dashboard_types.py](../src/index_inclusion_research/dashboard_types.py)
  负责 `AnalysesConfig`、`AnalysisRunner`、request/url builder protocols、route registration map 等共享类型。

如果你要继续增强类型边界，先从 `dashboard_types.py` 往里推最稳。

## 测试护栏

这条线现在最关键的回归护栏是：

- `python3 -m pytest -q tests/test_dashboard_frontend_js.py`
- `make typecheck`
- `make doctor-strict`
- `pytest -q`
- `RUN_BROWSER_SMOKE=1 pytest -q tests/test_dashboard_browser_smoke.py`

其中比较值钱的测试文件包括：

- [tests/test_dashboard_structure.py](../tests/test_dashboard_structure.py)
- [tests/test_dashboard_bootstrap_cli.py](../tests/test_dashboard_bootstrap_cli.py)
- [tests/test_dashboard_factory.py](../tests/test_dashboard_factory.py)
- [tests/test_dashboard_route_bindings.py](../tests/test_dashboard_route_bindings.py)
- [tests/test_dashboard_routes.py](../tests/test_dashboard_routes.py)
- [tests/test_dashboard_runtime.py](../tests/test_dashboard_runtime.py)
- [tests/test_dashboard_browser_smoke.py](../tests/test_dashboard_browser_smoke.py)

## 后续改动建议

如果接下来继续演进，优先顺序建议是：

1. 先收提交边界，不要把 dashboard refactor、RDD 输入契约、README 大改混成一个 commit。
2. 再继续收紧类型，把 `dict[str, object]` / `Callable[..., object]` 往更具体的 context/result 类型推进。
3. 如果要产品化，再考虑 refresh 状态持久化、任务日志、auth 和部署边界。
4. 如果继续扩首页 section，保持 “mode gating + browser smoke 覆盖” 一起落地，避免首页再次变成只靠手点验证的脆弱面。

如果只是改视觉或页面内容，通常不需要动 `factory/services` 这一层。优先在
`dashboard_home.py`、`dashboard_page_sections_runtime.py`、`dashboard_sections.py`、
`dashboard_presenters.py` 和静态资源里完成更安全。
