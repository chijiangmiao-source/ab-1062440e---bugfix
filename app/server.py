#!/usr/bin/env python3
"""离线记录页 HTTP 服务：REST API + 静态页面。

仅使用 Python 标准库，适配野外实验站离线环境。
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.db import ApiError, Store  # noqa: E402

STATIC_DIR = Path(__file__).resolve().parent / "static"
DB_PATH = os.environ.get("DB_PATH", "./data/app.db")
PORT = int(os.environ.get("PORT", "8000"))
PAGE_TTL = int(os.environ.get("PAGE_TTL_SECONDS", "45"))
ALLOW_SHUTDOWN = os.environ.get("ALLOW_ADMIN_SHUTDOWN") == "1"

CONTENT_TYPES = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
                 ".css": "text/css; charset=utf-8"}


class Handler(BaseHTTPRequestHandler):
    server_version = "EpochFieldStation/1.0"
    protocol_version = "HTTP/1.1"

    # ------------------------------------------------------------ 基础工具

    @property
    def store(self) -> Store:
        return self.server.store  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):
        print(f"[http] {self.address_string()} {fmt % args}", file=sys.stderr)

    def _send_json(self, status: int, obj: dict):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_static(self, path: Path):
        try:
            body = path.read_bytes()
        except OSError:
            raise ApiError(404, "not_found", "资源不存在")
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPES.get(path.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > 1_000_000:
            raise ApiError(413, "too_large", "请求体过大")
        try:
            data = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            raise ApiError(400, "bad_json", "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "bad_json", "请求体必须是 JSON 对象")
        return data

    # ------------------------------------------------------------ 路由

    ROUTES = [
        ("GET", re.compile(r"^/healthz$"), "h_health"),
        ("GET", re.compile(r"^/$"), "h_index"),
        ("GET", re.compile(r"^/(app\.js|style\.css)$"), "h_static"),
        ("GET", re.compile(r"^/api/workspaces$"), "h_ws_list"),
        ("POST", re.compile(r"^/api/workspaces$"), "h_ws_create"),
        ("GET", re.compile(r"^/api/workspaces/([\w-]+)/state$"), "h_ws_state"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/pages$"), "h_page_open"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/pages/([\w-]+)/heartbeat$"), "h_page_heartbeat"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/pages/([\w-]+)/close$"), "h_page_close"),
        ("DELETE", re.compile(r"^/api/workspaces/([\w-]+)/pages/([\w-]+)$"), "h_page_close"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/records$"), "h_record_add"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/migration/start$"), "h_mig_start"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/migration/copy$"), "h_mig_copy"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/migration/validate$"), "h_mig_validate"),
        ("POST", re.compile(r"^/api/workspaces/([\w-]+)/migration/publish$"), "h_mig_publish"),
        ("POST", re.compile(r"^/api/admin/shutdown$"), "h_admin_shutdown"),
    ]

    def _dispatch(self, method: str):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        for m, pattern, handler_name in self.ROUTES:
            if m != method:
                continue
            match = pattern.match(parsed.path)
            if match:
                getattr(self, handler_name)(*match.groups(), query=query)
                return
        raise ApiError(404, "not_found", "资源不存在")

    def _handle(self, method: str):
        try:
            self._dispatch(method)
        except ApiError as e:
            body = {"error": e.code, "message": e.message}
            body.update(e.extra)
            self._send_json(e.status, body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            try:
                self._send_json(500, {"error": "internal", "message": str(e)})
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_DELETE(self):
        self._handle("DELETE")

    # ------------------------------------------------------------ 处理器

    def h_health(self, query):
        self.store.conn.execute("SELECT 1")
        self._send_json(200, {"ok": True, "db": "up"})

    def h_index(self, query):
        self._send_static(STATIC_DIR / "index.html")

    def h_static(self, name, query):
        self._send_static(STATIC_DIR / name)

    def h_ws_list(self, query):
        self._send_json(200, {"workspaces": self.store.list_workspaces()})

    def h_ws_create(self, query):
        body = self._read_json()
        self._send_json(201, self.store.create_workspace(body.get("name", "")))

    def h_ws_state(self, ws_id, query):
        touch = (query.get("page_id") or [None])[0]
        self._send_json(200, self.store.get_state(ws_id, touch_page_id=touch))

    def h_page_open(self, ws_id, query):
        self._send_json(201, self.store.open_page(ws_id))

    def h_page_heartbeat(self, ws_id, page_id, query):
        self._send_json(200, self.store.heartbeat(ws_id, page_id))

    def h_page_close(self, ws_id, page_id, query):
        self._send_json(200, self.store.close_page(ws_id, page_id))

    def h_record_add(self, ws_id, query):
        body = self._read_json()
        self._send_json(201, self.store.add_record(
            ws_id, body.get("page_id", ""), body.get("content", ""),
            body.get("save_id", ""), body.get("origin_epoch_id", "")))

    def h_mig_start(self, ws_id, query):
        body = self._read_json()
        self._send_json(200, self.store.start_migration(
            ws_id, body.get("page_id", ""), body.get("target_version", "")))

    def h_mig_copy(self, ws_id, query):
        body = self._read_json()
        self._send_json(200, self.store.copy_batch(
            ws_id, body.get("page_id", ""), body.get("batch_size", 1)))

    def h_mig_validate(self, ws_id, query):
        body = self._read_json()
        self._send_json(200, self.store.validate_migration(ws_id, body.get("page_id", "")))

    def h_mig_publish(self, ws_id, query):
        body = self._read_json()
        self._send_json(200, self.store.publish_migration(ws_id, body.get("page_id", "")))

    def h_admin_shutdown(self, query):
        # 仅供编排环境验证“崩溃后重开”的恢复语义，需显式开启
        if not ALLOW_SHUTDOWN:
            raise ApiError(403, "disabled", "未启用管理关闭（ALLOW_ADMIN_SHUTDOWN!=1）")
        self._send_json(200, {"ok": True, "message": "进程即将退出，编排层将按重启策略拉起"})
        threading.Timer(0.3, lambda: os._exit(0)).start()


def main():
    store = Store(DB_PATH, page_ttl_seconds=PAGE_TTL)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    server.store = store  # type: ignore[attr-defined]
    print(f"[server] 监听 :{PORT}，数据库 {DB_PATH}，页面 TTL {PAGE_TTL}s", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
