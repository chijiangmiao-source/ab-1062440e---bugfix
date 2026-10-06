.PHONY: up verify test local

up:        ## 构建并启动记录页服务（前台）
	docker compose up --build app

verify:    ## 在编排环境内运行 verify 服务，以其退出码报告结果
	@docker compose up --build --exit-code-from verify --abort-on-container-exit; \
	code=$$?; docker compose down; exit $$code

test:      ## 本地运行单元测试
	python3 -m unittest discover -s tests -t . -v

local:     ## 本地启动服务（数据在 ./data）
	DB_PATH=./data/app.db python3 -m app.server
