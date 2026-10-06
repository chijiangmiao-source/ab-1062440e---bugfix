#!/usr/bin/env sh
# 在编排环境内运行 verify 服务，并以其退出码作为脚本退出码。
set -u
docker compose up --build --exit-code-from verify --abort-on-container-exit
code=$?
docker compose down
exit "$code"
