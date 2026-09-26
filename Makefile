# taskmq 开发入口。
#
# 开发用 **uv**（推荐）：它同时管解释器、虚拟环境、锁文件
#   make sync        # uv sync（按 uv.lock 装依赖）
#   make test        # uv run pytest
#   make test-py39   # uv run --python 3.9 pytest（最低支持版本，独立环境 .venv39，不动 .venv）
#
# 没装 uv 也能用（使用方/CI 的 pip 路径）：自动回落到 .venv 或 ${PYTHON}
#   make venv        # python -m venv + pip install -e ".[dev]"
#   make test PY=/path/to/python3.9

PYTHON ?= python3
VENV_PY := $(wildcard .venv/bin/python)
PY ?= $(if $(VENV_PY),$(VENV_PY),$(PYTHON))
UV ?= $(shell command -v uv 2>/dev/null)

# 托管 Python 的目录是 uv 的全局设置，只能用环境变量指定：导出让 3.9/3.10 解释器都随工程走
# （HOME 不可写的受限沙箱里也能直接用；普通开发机不受影响）
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.uv-python

ifeq ($(UV),)
  PYTEST  := $(PY) -m pytest
  RUFF    := $(PY) -m ruff
  MYPY    := $(PY) -m mypy
  PYRIGHT := $(PY) -m pyright
else
  PYTEST  := $(UV) run pytest
  RUFF    := $(UV) run ruff
  MYPY    := $(UV) run mypy
  PYRIGHT := $(UV) run pyright
endif

.PHONY: venv sync test test-py39 lint fmt typecheck typecheck-mypy typecheck-pyright coverage \
	pg-up pg-down test-postgres mq-up mq-down test-amqp redis-cluster-up redis-cluster-down \
	test-redis-cluster check clean

venv:            ## 不用 uv 的开发环境：venv + pip install -e ".[dev]"
	$(PYTHON) -m venv .venv
	.venv/bin/python -m pip install -U pip
	.venv/bin/python -m pip install -e ".[dev]"

ifeq ($(UV),)
sync:
	@echo "没找到 uv；用 make venv（pip 路径）或先装 uv：https://docs.astral.sh/uv/"
	@exit 1
test-py39:
	@echo "切解释器要 uv；没有 uv 就用：make test PY=/path/to/python3.9"
	@exit 1
else
sync:            ## 用 uv 安装依赖（含 dev group）
	$(UV) sync

test-py39:       ## 在最低支持版本 3.9 上跑测试（uv 管解释器；独立环境 .venv39）
	UV_PROJECT_ENVIRONMENT=.venv39 $(UV) run --python 3.9 pytest
endif

test:            ## 跑测试
	$(PYTEST)

lint:
	$(RUFF) check taskmq tests

fmt:
	$(RUFF) format taskmq tests
	$(RUFF) check --fix taskmq tests

typecheck: typecheck-mypy typecheck-pyright  ## 两个类型检查器都跑（都按最低版本 3.9 检查）

typecheck-mypy:
	$(MYPY)

typecheck-pyright:
	$(PYRIGHT)

coverage:
	$(PYTEST) -q --cov=taskmq --cov-report=term-missing

# PostgreSQL transport 的测试环境：一个可随时删掉的专用容器（55432 端口，避开本机 5432）
PG_URL ?= postgresql://taskmq:taskmq@127.0.0.1:55432/taskmq

pg-up:           ## 起专用 PostgreSQL 测试容器（--restart 保证 VM 重启后也拉得回来）
	docker run -d --name taskmq-postgres-test --restart unless-stopped \
		-e POSTGRES_USER=taskmq -e POSTGRES_PASSWORD=taskmq -e POSTGRES_DB=taskmq \
		-p 127.0.0.1:55432:5432 postgres:16-alpine

pg-down:         ## 删掉测试容器
	docker rm -f taskmq-postgres-test

test-postgres:   ## 只跑 Postgres transport 测试（默认连上面的容器）
	TASKMQ_TEST_POSTGRES_URL=$(PG_URL) $(PYTEST) tests/test_postgres_transport.py

# AMQP transport 同理：专用 RabbitMQ 容器（55672 AMQP / 15673 管理台）
AMQP_BROKER ?= amqp://taskmq:taskmq@127.0.0.1:55672/%2F

mq-up:           ## 起专用 RabbitMQ 测试容器
	docker run -d --name taskmq-rabbitmq-test --restart unless-stopped \
		-e RABBITMQ_DEFAULT_USER=taskmq -e RABBITMQ_DEFAULT_PASS=taskmq \
		-p 127.0.0.1:55672:5672 -p 127.0.0.1:15673:15672 rabbitmq:3.12-management-alpine

mq-down:         ## 删掉测试容器
	docker rm -f taskmq-rabbitmq-test

test-amqp:       ## 只跑 AMQP transport 测试（默认连上面的容器）
	TASKMQ_TEST_AMQP_BROKER='$(AMQP_BROKER)' $(PYTEST) tests/test_amqp_transport.py

# Redis Cluster 测试环境：3 主（7380-7382）。`--cluster-announce-ip 127.0.0.1` 让 MOVED
# 返回宿主机可达的地址；容器内三个节点共用 127.0.0.1 的不同端口，gossip 也走得通。
REDIS_CLUSTER_URL ?= redis://127.0.0.1:7380/0

redis-cluster-up:  ## 起 3 主 Redis Cluster 测试容器（7380-7382）
	docker run -d --name taskmq-redis-cluster --restart unless-stopped \
		-p 127.0.0.1:7380:7380 -p 127.0.0.1:7381:7381 -p 127.0.0.1:7382:7382 \
		redis:7.0.5 sh -c 'for p in 7380 7381 7382; do redis-server --port $$p \
			--cluster-enabled yes --cluster-config-file /data/nodes-$$p.conf \
			--cluster-node-timeout 5000 --cluster-announce-ip 127.0.0.1 \
			--appendonly no --daemonize yes --dir /data; done; sleep 1; \
			redis-cli --cluster create 127.0.0.1:7380 127.0.0.1:7381 127.0.0.1:7382 --cluster-yes; \
			tail -f /dev/null'

redis-cluster-down:  ## 删掉测试容器
	docker rm -f taskmq-redis-cluster

test-redis-cluster:  ## 只跑 Redis Cluster 测试（默认连上面的容器）
	TASKMQ_TEST_REDIS_CLUSTER_URL=$(REDIS_CLUSTER_URL) $(PYTEST) tests/test_redis_cluster.py

check: lint typecheck test  ## 本地 CI 等价检查

clean:
	rm -rf .venv .venv39 .uv-cache .uv-python .pytest_cache .ruff_cache .mypy_cache
