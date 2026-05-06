# mypy 严格化路线图

## 当前状态（2026-05-06，第二轮）

`make typecheck` 通过，`ignore_errors=true` 模块清单已**全部清空**。所有
108 个源文件参与类型检查。

工程债的承载方式：

- `pyproject.toml` 锁 `pandas-stubs<2.3`（避免 stub 严苛化反复炸窗口）。
- 行级 `# type: ignore[<code>]`：当前 127 处，集中在 9 个 pandas-heavy
  文件（最多的是 `sample_data` 16 处、`chart_data` 12 处、
  `pipeline.matching` 10 处）。绝大多数是 stub 死结
  （`pd.isna(object)`、`int(<pandas value>)`、Series indexing 上的
  `Hashable` 推导等），不是逻辑 bug。
- `warn_unused_ignores=true` 持续守护：当 stub 改善或源码重写让某个
  ignore 不再必要，mypy 会主动报错推动清理。

## 解锁历史

| 日期 | 操作 | 结果 |
|---|---|---|
| 2026-05-06 #1 | 钉 `pandas-stubs<2.3` + 6 模块 `ignore_errors=true` + 66 行 ignore | 238 → 0 errors，CI 绿 |
| 2026-05-06 #2 | 解锁全部 6 个 baseline 模块（`dashboard_figures` / `chart_data` / `sample_data` / `verdicts/_paper` / `pipeline/matching` / `research_report`），又新增 ~50 行 ignore | baseline 列表清空，全部 108 文件参与 typecheck |

## 接下来怎么继续清

剩下的 127 处 `# type: ignore` 不是工程债的全部 — 它们是**当前 stub
能力的边界**。继续推进有两条路：

1. **等 stub 改进 / 升级 pandas-stubs**。当 `<2.3` 上限可以解开时，
   按 `docs/mypy_rollout.md` "pandas-stubs 升级处理" 流程跑：先在本地
   解锁版本看新增错误数，必要时降级回 `<2.4` 等更窄区间。`warn_unused_ignores`
   会自动报 stub 改进后变多余的 ignore，那时直接删。

2. **替换为更类型安全的源码**。常见可改写法：
   - `pd.isna(x)` 当 `x: object` → 改成 `x is None or (isinstance(x, float) and math.isnan(x))`
     或在调用前先 `x = cast(float | None, x)`
   - `int(<pandas value>)` → 用 `pd.to_numeric(..., errors="coerce")` 显式过一遍，
     然后 `.astype(int)`，整列同时处理
   - `dict.get(<pandas key>)` → 在调用前 `key = str(row["..."])`
   - `for idx, row in df.iterrows()` 当 `idx` 当数字用 → 换 `enumerate(df.iterrows())`，
     `dashboard_figures.py` 已经按这个套路改了

不要用 TypedDict 包 pandas 行 — 对纯函数体没有收益，徒增维护成本。

## pandas-stubs 升级时的处理

当准备把 `pandas-stubs<2.3` 解开时:

1. 先在本地装 `pandas-stubs>=2.3` 跑 `mypy`，看新增错误清单。
2. 若新增 < 20 处，直接修；若 > 20 处，把上限改为 `<2.4` 之类的渐进升级。
3. 同步更新 `.pre-commit-config.yaml` 里 mypy hook 的 `additional_dependencies`。

## 不在路线图里的事

- **`web/` 模块整体被 mypy `exclude`**。Flask 模板 + 大量 `Any`，严格化收益低。除非把模板换成 type-safe 渲染，否则保留 exclude。
- **`literature_dashboard.py`** 同上 — coverage 也排除了。
