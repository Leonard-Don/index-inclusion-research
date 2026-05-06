# mypy 严格化路线图

## 当前状态（2026-05-06）

`make typecheck` 通过,**但严格度比看起来低**:

1. `pyproject.toml` 里 `pandas-stubs` 上限锁在 `<2.3`。pandas-stubs 2.3 / 3.0 升级会引入 ~30+ 个新 stub 错误,目前不接收。
2. 6 个 pandas/numpy 重度文件用 `[tool.mypy.overrides] ignore_errors = true` 整文件忽略。
3. 26 个文件加了行级 `# type: ignore[arg-type|call-overload|...]`,大部分是 stub 死结(`pd.isna(object)`、`int(<pandas value>)`、`Series(dtype="string")` 等),少数是真有歧义的源码。

也就是说: typecheck 不会因 stub 噪声 fail,但**新增的逻辑错误**(`attr-defined`、`return-value`、`name-defined`)依然会被发现。

## 6 个 baseline 文件

`pyproject.toml` 的 `ignore_errors=true` 列表(132 个被屏蔽错误):

| 模块 | baseline 时错误数 |
|---|---|
| `research_report` | 34 |
| `pipeline.matching` | 30 |
| `analysis.cross_market_asymmetry.verdicts._paper` | 24 |
| `sample_data` | 19 |
| `chart_data` | 15 |
| `dashboard_figures` | 10 |

## 解锁单个文件的流程

1. 在 `pyproject.toml` 的 `[[tool.mypy.overrides]]` `module = [...]` 里**删掉**该模块。
2. `make typecheck` 看新增错误。
3. 优先用 `cast(...)` 或修源码(改类型签名、显式类型),**避免新增 `# type: ignore`**。
4. 实在死结再加 `# type: ignore[<具体 code>]`。
5. 跑 `make ci` 端到端验证。

## 解锁顺序建议

按错误密度从低到高,先攻易拿下的:

1. **`dashboard_figures`(10)** — 主要是 Series indexing,容易补类型。
2. **`chart_data`(15)** — 同上。
3. **`sample_data`(19)** — 数据生成,类型路径相对线性。
4. **`pipeline.matching`(30)** — 重要,但需要熟悉匹配逻辑。
5. **`verdicts._paper`(24)** — 论文产出层,改动风险低。
6. **`research_report`(34)** — 留到最后,产出层,牵扯多。

## 行级 `# type: ignore` 的清理

```bash
grep -rn "# type: ignore" src/index_inclusion_research/ | wc -l
```

每解锁一个真 bug,删掉对应行的 `# type: ignore`。pre-commit 配置中 `warn_unused_ignores = true` 会在多余的 ignore 上报错,自然推动清理。

## pandas-stubs 升级时的处理

当准备把 `pandas-stubs<2.3` 解开时:

1. 先在本地装 `pandas-stubs>=2.3` 跑 `mypy`,看新增错误清单。
2. 若新增 < 20 处,直接修;若 > 20 处,把上限改为 `<2.4` 之类的渐进升级。
3. 同步更新 `.pre-commit-config.yaml` 里 mypy hook 的 `additional_dependencies`。

## 不在路线图里的事

- **`web/` 模块整体被 mypy `exclude`**。Flask 模板 + 大量 `Any`,严格化收益低。除非把模板换成 type-safe 渲染,否则保留 exclude。
- **`literature_dashboard.py`** 同上 — coverage 也排除了。
