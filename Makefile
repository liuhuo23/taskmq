# taskmq 开发入口（统一走 uv）。
# 说明：托管 Python 的目录是 uv 的全局设置，只能用环境变量指定，
# 因此这里统一导出 UV_PYTHON_INSTALL_DIR，保证 3.10 解释器随工程走。
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.uv-python

PY ?= 3.10

.PHONY: sync test test-py310 lint fmt fmt-check typecheck typecheck-mypy typecheck-pyright coverage \
	pg-up pg-down test-postgres mq-up mq-down test-amqp redis-cluster-up redis-cluster-down \
	test-redis-cluster check clean

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

coverage:
	uv run pytest -q --cov=taskmq --cov-report=term-missing

# PostgreSQL transport 的测试环境：一个可随时删掉的专用容器（55432 端口，避开本机 5432）
PG_URL ?= postgresql://taskmq:taskmq@127.0.0.1:55432/taskmq

pg-up:           ## 起专用 PostgreSQL 测试容器（--restart 保证 VM 重启后也拉得回来）
	docker run -d --name taskmq-postgres-test --restart unless-stopped \
		-e POSTGRES_USER=taskmq -e POSTGRES_PASSWORD=taskmq -e POSTGRES_DB=taskmq \
		-p 127.0.0.1:55432:5432 postgres:16-alpine

pg-down:         ## 删掉测试容器
	docker rm -f taskmq-postgres-test

test-postgres:   ## 只跑 Postgres transport 测试（默认连上面的容器）
	TASKMQ_TEST_POSTGRES_URL=$(PG_URL) uv run pytest tests/test_postgres_transport.py

# AMQP transport 同理：专用 RabbitMQ 容器（55672 AMQP / 15673 管理台）
AMQP_BROKER ?= amqp://taskmq:taskmq@127.0.0.1:55672/%2F

mq-up:           ## 起专用 RabbitMQ 测试容器
	docker run -d --name taskmq-rabbitmq-test --restart unless-stopped \
		-e RABBITMQ_DEFAULT_USER=taskmq -e RABBITMQ_DEFAULT_PASS=taskmq \
		-p 127.0.0.1:55672:5672 -p 127.0.0.1:15673:15672 rabbitmq:3.12-management-alpine

mq-down:         ## 删掉测试容器
	docker rm -f taskmq-rabbitmq-test

test-amqp:       ## 只跑 AMQP transport 测试（默认连上面的容器）
	TASKMQ_TEST_AMQP_BROKER='$(AMQP_BROKER)' uv run pytest tests/test_amqp_transport.py

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
	TASKMQ_TEST_REDIS_CLUSTER_URL=$(REDIS_CLUSTER_URL) uv run pytest tests/test_redis_cluster.py

check: lint typecheck test  ## 本地 CI 等价检查

clean:
	rm -rf .venv .uv-cache .uv-python .pytest_cache .ruff_cache .mypy_cache
