# 在 taskmq 仓库里改代码

> 入口见 `SKILL.md`。这里只讲**这个仓库**的开发规矩（用 taskmq 写业务代码请看 `usage.md`）。

## 仓库长什么样

```text
taskmq/            包本体（protocol / transport{memory,sqlite,redis,postgres,amqp} / worker / workflow / schedule / plugins / testing）
  _compat.py       3.9 ↔ 3.10 差异的垫片
tests/             pytest 全量（306 个）+ 一致性套件
docs/              文档站源（guide/ 使用文档 + design/ 设计文档）
skills/taskmq/     给 AI agent 用的 skill（本文件所在）
examples/          可跑示例（含插件示例 examples/plugin_rocketmq）
.github/workflows/ ci.yml / release.yml / pypi.yml / docs.yml
Makefile           开发入口（uv 优先，没 uv 回落到 .venv / $PYTHON）
```

## 环境

```bash
uv sync            # 推荐：uv 管解释器 + .venv + uv.lock
make test          # = uv run pytest
make check         # = ruff + mypy + pyright + pytest（提交前必跑）
make test-py39     # 在最低支持版本 3.9 上再跑一遍（独立环境 .venv39，不动 .venv）
make coverage      # 覆盖率
make stress        # 只跑规模/边界/吞吐用例（tests/test_limits_stress.py）
make bench         # 压测吞吐/延迟（ARGS 透传：make bench ARGS='-n 20000 -c 16'）
```

没有 uv 也能开发：`make venv`（venv + `pip install -e ".[dev]"`），之后 `make test` 会自动用 `.venv`。
需要真服务时：`make pg-up` / `make mq-up` / `make redis-cluster-up`（配 `test-postgres` / `test-amqp` / `test-redis-cluster`）。

## 3.9 ↔ 3.10 的分工（重要）

- `.python-version` 固定 **3.10**：日常开发与**类型检查的语言级别**都按 3.10（mypy 2.x 也已不支持 3.9 目标）；
- **运行时下限是 3.9**，由 CI 在真 3.9 上跑全量测试保证（本地用 `make test-py39`）；
- 想在代码里用 3.10 专属能力，必须让 3.9 走回退，两种写法：

```python
# ① 标准库 API 差异 → 版本分支
if sys.version_info >= (3, 10):
    from typing import Concatenate, ParamSpec
else:                                   # pragma: no cover - 3.9
    from typing_extensions import Concatenate, ParamSpec

# ② 语法/行为差异 → 走 _compat 提供的垫片
from ._compat import _SLOTS
@dataclasses.dataclass(frozen=True, **_SLOTS)     # 3.10+ 传 slots=True；3.9 传空（语义不变）
class Delivery: ...
```

`typing_extensions` 是 **`python_version < "3.10"` 的条件依赖**（已写在 pyproject），3.10+ 不装。
「类型检查过了」不等于「3.9 能跑」——`Task[..., str]` 这种写法 mypy 不报，3.9 运行时才 `TypeError`；所以**改完必须跑 `make test-py39`**。

## 代码风格约定

- 数据类一律 `@dataclasses.dataclass(frozen=True, **_SLOTS)`（不可变 + slots）；异常明确抛 `ConfigError` / `TransportError` 等既有的；
- 注释与 docstring 写**为什么**（取舍、坑），不写「这行在干什么」；
- 新增后端能力要**如实声明** `supports_leases / supports_workers / supports_job_listing` 与 `limitations`，
  并在自家 CI 跑 `taskmq.testing.transport_conformance`（16 个场景）；
- 公共 API 只从 `taskmq/__init__.py` 暴露（`__all__`）；插件只能用 `taskmq.transport` / `taskmq.protocol` / `taskmq.plugins` 的公开名字；
- 提交信息用中文，主题一行 + `-` 要点（见 git log）。

## 测试与门禁

```bash
make check                                  # ruff + mypy + pyright + 全量 pytest
uv run pytest tests/test_redis_cluster.py -q  # 单文件
```

- 类型检查跑**两个引擎**（mypy = CI 口径，pyright = 编辑器口径，pyright 需要 node 在 PATH）；
- Redis 相关测试在没服务时自动 skip；`TASKMQ_TEST_REDIS_URL` / `TASKMQ_TEST_REDIS_CLUSTER_URL` /
  `TASKMQ_TEST_POSTGRES_URL` / `TASKMQ_TEST_AMQP_BROKER` 可覆盖地址；
- 加新 transport / 改语义，必须同时更新一致性套件能覆盖到的行为，以及 `docs/design/*.md`。

## 文档

- 站点是 MkDocs Material（`mkdocs.yml` + `docs/**`），push 到 `main` 自动构建 + 部署 GitHub Pages；
- 本地预览：`uv run --with "mkdocs-material>=9,<10" mkdocs serve`；CI 用 `mkdocs build --strict`（坏链接会让流水线红）；
- 中文标题需要 unicode slugify（`mkdocs.yml` 里已配），否则锚点是空的。

## 发版（全自动链路）

```bash
# 1) 只改 pyproject.toml 的 version（可带 .devN，发布时会被去掉），合进 main
# 2) 之后全自动：
#    ci（3.9/3.10 全量 + pip 矩阵）→ release.yml 打 tag + 建 GitHub Release
#    → 显式 dispatch pypi.yml → uv publish（Trusted Publishing / OIDC，无 token）
```

- tag = `pyproject.toml` 的 version 去掉 `.devN`；**同名 tag 已存在就跳过**（幂等）；
- PyPI 上的**分发名是 `taskmq-py`**（`taskmq` 与已有的 `task-mq` 相似度过不了），import 名与命令仍是 `taskmq`；
- 不要手工 `gh release create` 绕过链路：`pypi.yml` 需要被显式 dispatch（GITHUB_TOKEN 创建的事件不再触发其它 workflow），
  但 `pypi.yml` 有「版本已存在则跳过」的幂等保护，重跑无害；
- 发布后检查：`gh run list`、`gh release view vX.Y.Z`、`https://pypi.org/pypi/taskmq-py/json`。

## 收尾清单

- [ ] `make check` 绿（ruff + mypy + pyright + pytest）
- [ ] 动过 3.9 可能受影响的地方 → `make test-py39` 绿
- [ ] 动了文档/README → 站点能 `mkdocs build --strict` 过
- [ ] 推之前 `git status` 干净、没夹带临时文件
- [ ] 推完看 `gh run list` 确认 ci/docs/release 都绿（release 在 tag 已存在时应为 skipped/success）
