# taskmq 开发入口（统一走 uv）。
# 说明：托管 Python 的目录是 uv 的全局设置，只能用环境变量指定，
# 因此这里统一导出 UV_PYTHON_INSTALL_DIR，保证 3.10 解释器随工程走。
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.uv-python

PY ?= 3.10

.PHONY: sync test test-py310 lint fmt fmt-check typecheck typecheck-mypy typecheck-pyright check clean

sync:            ## 安装依赖（含 dev group）
	uv sync

test:            ## 跑测试（当前 .venv 的解释器）
	uv run pytest

test-py310:      ## 在最低支持版本 3.10 上跑测试
	uv run --python $(PY) pytest

lint:
	uv run ruff check taskmq tests

fmt:
	uv run ruff format taskmq tests
	uv run ruff check --fix taskmq tests

typecheck: typecheck-mypy typecheck-pyright  ## 两个类型检查器都跑

typecheck-mypy:
	uv run mypy

typecheck-pyright:
	uv run pyright

check: lint typecheck test  ## 本地 CI 等价检查

clean:
	rm -rf .venv .uv-cache .uv-python .pytest_cache .ruff_cache .mypy_cache
