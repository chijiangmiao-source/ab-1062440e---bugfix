#!/usr/bin/env python3
"""verify 服务：在编排环境内完成全部验收并自行退出，以退出码报告结果。

步骤穿插执行：构建检查 -> 代码测试 -> 健康/页面 HTTP 冒烟 -> API 冒烟 ->
场景一（两页面迁移 + 旧写拒绝 + 并发迁移不建第二候选）->
场景二（复制中断重开不展示部分数据 + 校验阶段续用同一候选）->
场景三（发布后进程重启，本地恢复的纪元/记录/失效状态一致）。

用法：
  python verify.py [--base-url http://app:8000] [--no-restart]
"""

from __future__ import annotations

import argparse
import compileall
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent

RESULTS: list[tuple[str, bool, str]] = []


class VerifyError(Exception):
    pass


def step(name):
    """注册并包装一个验收步骤。"""
    def deco(fn):
        def wrapped(ctx):
            print(f"\n[verify] === {name} ===", flush=True)
            try:
                fn(ctx)
            except Exception as e:  # noqa: BLE001
                RESULTS.append((name, False, str(e)))
                print(f"[verify] ✗ {name}: {e}", flush=True)
            else:
                RESULTS.append((name, True, ""))
                print(f"[verify] ✓ {name}", flush=True)
        return wrapped
    return deco


def check(cond, msg):
    if not cond:
        raise VerifyError(msg)


# ---------------------------------------------------------------- HTTP 工具

def req(ctx, method, path, body=None, expect=None):
    url = ctx["base"] + path
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            code, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, payload = e.code, e.read()
    obj = json.loads(payload) if payload else {}
    if expect is not None and code != expect:
        raise VerifyError(f"{method} {path} 期望 {expect} 实际 {code}: {obj}")
    return code, obj


def wait_health(ctx, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, obj = req(ctx, "GET", "/healthz")
            if code == 200 and obj.get("ok"):
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(1)
    raise VerifyError(f"等待健康检查超时（{timeout}s）")


# ---------------------------------------------------------------- 构建与测试

@step("构建检查：Python 编译、模块导入与静态资源完整性")
def build_check(ctx):
    ok = compileall.compile_dir(str(ROOT / "app"), quiet=1, force=True)
    check(ok, "app/ 编译失败")
    ok = compileall.compile_dir(str(ROOT / "tests"), quiet=1, force=True)
    check(ok, "tests/ 编译失败")
    ok = compileall.compile_file(str(ROOT / "verify.py"), quiet=1, force=True)
    check(ok, "verify.py 编译失败")
    for rel in ("app/static/index.html", "app/static/app.js", "app/static/style.css"):
        check((ROOT / rel).exists(), f"缺少 {rel}")
    html = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    check("/app.js" in html and "/style.css" in html, "index.html 未引用静态资源")
    sys.path.insert(0, str(ROOT))
    import app.db  # noqa: F401
    import app.server  # noqa: F401


@step("代码测试：纪元迁移单元测试")
def unit_tests(ctx):
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", "."],
        cwd=ROOT, capture_output=True, text=True, timeout=120)
    sys.stdout.write(proc.stdout[-2000:])
    sys.stdout.write(proc.stderr[-2000:])
    check(proc.returncode == 0, "单元测试失败")


# ---------------------------------------------------------------- 冒烟

@step("HTTP 冒烟：健康响应")
def smoke_health(ctx):
    wait_health(ctx)
    code, obj = req(ctx, "GET", "/healthz", expect=200)
    check(obj.get("ok") is True and obj.get("db") == "up", f"健康响应异常: {obj}")


@step("HTTP 冒烟：页面与静态资源")
def smoke_pages(ctx):
    # 页面是 HTML，直接原始读取而非 JSON 解析
    with urllib.request.urlopen(ctx["base"] + "/", timeout=10) as resp:
        text = resp.read().decode()
        check(resp.status == 200, "首页状态码非 200")
    check("离线记录页" in text, "首页缺少应用标识")
    for asset in ("/app.js", "/style.css"):
        with urllib.request.urlopen(ctx["base"] + asset, timeout=10) as resp:
            check(resp.status == 200, f"{asset} 状态码非 200")
            check(len(resp.read()) > 100, f"{asset} 内容异常")


@step("API 冒烟：建工作区、开页面、写记录、读状态")
def smoke_api(ctx):
    ws_name = f"verify-{uuid.uuid4().hex[:8]}"
    code, ws = req(ctx, "POST", "/api/workspaces", {"name": ws_name}, expect=201)
    ctx["ws"] = ws["id"]
    check(ws["current_epoch"]["number"] == 1, "初始纪元应为 #1")
    code, page = req(ctx, "POST", f"/api/workspaces/{ws['id']}/pages", expect=201)
    ctx["page_a"] = page["page_id"]
    for i in range(5):
        req(ctx, "POST", f"/api/workspaces/{ws['id']}/records",
            {"page_id": page["page_id"], "content": f"观测记录-{i + 1}"}, expect=201)
    code, s = req(ctx, "GET", f"/api/workspaces/{ws['id']}/state", expect=200)
    check(len(s["records"]) == 5, "记录数应为 5")
    check(s["current_epoch"]["record_count"] == 5, "纪元计数应为 5")


# ---------------------------------------------------------------- 场景一

@step("场景一：两页面迁移，旧页迟到保存在发布前后均被拒绝")
def scenario_stale_write_rejected(ctx):
    ws = ctx["ws"]
    page_b = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    ctx["page_b"] = page_b

    # 发起迁移；并发发起第二个迁移必须被拒绝且不产生第二候选
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
               {"page_id": ctx["page_a"], "target_version": "v2"}, expect=200)
    check(s["migration"]["phase"] == "copying", "迁移应进入复制阶段")
    code, err = req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
                    {"page_id": page_b, "target_version": "v2b"})
    check(code == 409 and err.get("error") == "migration_active",
          f"并发迁移应被拒绝: {code} {err}")
    check(s["migration"]["candidate_epoch"] is not None, "应存在唯一候选纪元")

    # 发布前：另一页的保存被拒绝
    code, err = req(ctx, "POST", f"/api/workspaces/{ws}/records",
                    {"page_id": page_b, "content": "迟到记录"})
    check(code == 409 and err.get("error") == "migration_in_progress",
          f"发布前旧写应被拒绝: {code} {err}")

    # 复制 -> 校验 -> 原子发布
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
                   {"page_id": ctx["page_a"], "batch_size": 2}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/validate",
        {"page_id": ctx["page_a"]}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/publish",
               {"page_id": ctx["page_a"]}, expect=200)
    check(s["migration"]["phase"] == "published", "迁移应已发布")
    check(s["current_epoch"]["number"] == 2, "当前纪元应为 #2")
    check(s["current_epoch"]["version"] == "v2", "当前版本应为 v2")
    check([r["content"] for r in s["records"]] == [f"观测记录-{i}" for i in range(1, 6)],
          "新纪元记录应完整")

    # 发布后：仍停留在旧页的迟到保存被拒绝并提示重新载入
    code, err = req(ctx, "POST", f"/api/workspaces/{ws}/records",
                    {"page_id": page_b, "content": "迟到记录"})
    check(code == 409 and "重新载入" in err.get("message", ""),
          f"发布后旧写应被拒绝并提示重新载入: {code} {err}")

    # 重开页面：只能读到新纪元完整记录，且可继续写入
    page_c = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    ctx["page_c"] = page_c
    req(ctx, "POST", f"/api/workspaces/{ws}/records",
        {"page_id": page_c, "content": "新纪元记录"}, expect=201)
    _, s = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(len(s["records"]) == 6, "新纪元应有 6 条记录")
    states = {p["id"]: p["state"] for p in s["pages"]}
    check(states[ctx["page_a"]] == "invalidated", "页面A应已失效")
    check(states[page_b] == "invalidated", "页面B应已失效")
    check(states[page_c] == "active", "页面C应为活动")


# ---------------------------------------------------------------- 场景二

@step("场景二：复制中断重开不展示部分数据；校验阶段续用同一候选")
def scenario_interruption_recovery(ctx):
    ws = ctx["ws"]
    page_c = ctx["page_c"]

    # —— 复制中断：复制一部分后关闭负责页面 ——
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
        {"page_id": page_c, "target_version": "v3"}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
               {"page_id": page_c, "batch_size": 1}, expect=200)
    check(s["migration"]["copied"] == 1 and s["migration"]["phase"] == "copying",
          "应处于部分复制状态")
    req(ctx, "POST", f"/api/workspaces/{ws}/pages/{page_c}/close", expect=200)

    # 重开页面触发恢复：候选被安全回收，读取仍是完整旧纪元
    page_d = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    _, s = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(s["migration"]["phase"] == "aborted",
          f"复制中断应被安全回收: {s['migration']['phase']}")
    check(s["migration"]["candidate_epoch"] is None, "候选应已回收")
    check(s["current_epoch"]["number"] == 2, "仍应读到旧纪元 #2")
    check(len(s["records"]) == 6, "记录应完整（无部分复制数据）")

    # —— 校验阶段中断：后来页面续用同一候选 ——
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
        {"page_id": page_d, "target_version": "v3"}, expect=200)
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
                   {"page_id": page_d, "batch_size": 3}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    cand = s["migration"]["candidate_epoch"]
    req(ctx, "POST", f"/api/workspaces/{ws}/pages/{page_d}/close", expect=200)

    page_e = req(ctx, "POST", f"/api/workspaces/{ws}/pages", expect=201)[1]["page_id"]
    ctx["page_e"] = page_e
    _, s = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(s["migration"]["phase"] == "validating", "校验阶段应保留待续")
    check(s["migration"]["candidate_epoch"] == cand, "应续用同一候选纪元")
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/validate",
        {"page_id": page_e}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/publish",
               {"page_id": page_e}, expect=200)
    check(s["current_epoch"]["number"] == 3, "当前纪元应为 #3")
    check(s["current_epoch"]["version"] == "v3", "当前版本应为 v3")
    check(len(s["records"]) == 6, "新纪元记录应完整")


# ---------------------------------------------------------------- 场景四

def _drive_migration(ctx, ws, page, version, batch=10):
    """发起并完成一次目标版本迁移，返回发布后的状态。"""
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/start",
               {"page_id": page, "target_version": version}, expect=200)
    check(s["migration"]["phase"] == "copying", "迁移应进入复制阶段")
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/copy",
                   {"page_id": page, "batch_size": batch}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    req(ctx, "POST", f"/api/workspaces/{ws}/migration/validate",
        {"page_id": page}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws}/migration/publish",
               {"page_id": page}, expect=200)
    check(s["migration"]["phase"] == "published", "迁移应已发布")
    return s


@step("场景四：保留保存恢复——已提交恰好一次 / 未提交旧纪元拒绝 / 标识冲突")
def scenario_pending_save_recovery(ctx):
    ws_name = f"verify-pending-{uuid.uuid4().hex[:8]}"
    ws = req(ctx, "POST", "/api/workspaces", {"name": ws_name}, expect=201)[1]
    ws_id = ws["id"]
    epoch1 = ws["current_epoch"]["id"]
    page_a = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]
    # 普通新记录（无 save_id）保持既有行为
    for i in range(2):
        req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
            {"page_id": page_a, "content": f"基线-{i + 1}"}, expect=201)
    # 另一页（离线页），持有旧纪元围栏
    page_b = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]

    # —— 情形一：已提交但未获回执（请求在迁移前已落库，客户端没收到结果）——
    save_committed = f"save-{uuid.uuid4().hex}"
    code, r = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                  {"page_id": page_b, "content": "已提交离线记录",
                   "save_id": save_committed, "origin_epoch_id": epoch1})
    check(code == 201 and r.get("replayed") is False, f"首次接受异常: {code} {r}")
    committed_seq = r["seq"]

    # 另一页完成目标版本迁移并发布
    s = _drive_migration(ctx, ws_id, page_a, "v2")
    check(s["current_epoch"]["number"] == 2, "应发布到纪元 #2")
    check(len(s["records"]) == 3, f"新纪元应恰好复制 3 条: {len(s['records'])}")

    # 旧页B 已被失效，但它重放自己那条【已接受】保存：仍稳定返回同一业务结果
    states = {p["id"]: p["state"] for p in s["pages"]}
    check(states[page_b] == "invalidated", "旧页B 应已失效")
    code, r2 = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                   {"page_id": page_b, "content": "已提交离线记录",
                    "save_id": save_committed, "origin_epoch_id": epoch1})
    check(code == 201 and r2.get("replayed") is True, f"已接受保存应稳定重放: {code} {r2}")
    check(r2["seq"] == committed_seq and r2["content"] == "已提交离线记录",
          "重放应返回同一业务结果（seq/内容）")

    # 重开页B（新页面，持有新纪元围栏），再次恢复：仍恰好一次，不新增记录
    page_c = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]
    code, r3 = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                   {"page_id": page_c, "content": "已提交离线记录",
                    "save_id": save_committed, "origin_epoch_id": epoch1})
    check(code == 201 and r3.get("replayed") is True, f"重开后重放异常: {code} {r3}")
    check(r3["id"] == r2["id"] and r3["seq"] == committed_seq, "重放结果必须稳定")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check(len(s["records"]) == 3, f"迁移后重放不得重复记录，仍应为 3 条: {len(s['records'])}")

    # —— 情形二：未提交即断线（请求从未到达服务端）——
    # 借重开的新页面（活动、持新纪元围栏）发送一个指向旧纪元、从未接受的保存
    save_lost = f"save-{uuid.uuid4().hex}"
    code, err = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                    {"page_id": page_c, "content": "断线未达记录",
                     "save_id": save_lost, "origin_epoch_id": epoch1})
    check(code == 409 and err.get("error") == "stale_epoch",
          f"未提交的旧纪元保存必须被拒绝: {code} {err}")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check(len(s["records"]) == 3, "旧纪元拒绝不得产生新纪元记录")
    check(not any(r["content"] == "断线未达记录" for r in s["records"]),
          "断线未达记录不得借新页面落库")

    # 已失效的旧页B 发送另一条【全新】保存：仍按页面围栏拒绝（两页面旧写拒绝）
    save_new = f"save-{uuid.uuid4().hex}"
    code, err = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                    {"page_id": page_b, "content": "旧页另一条新保存",
                     "save_id": save_new, "origin_epoch_id": epoch1})
    check(code == 409 and err.get("error") == "page_not_active",
          f"失效旧页的新保存应被拒绝: {code} {err}")

    # —— 情形三：相同稳定保存标识但内容不同 = 明确冲突 ——
    epoch2 = s["current_epoch"]["id"]
    save_conf = f"save-{uuid.uuid4().hex}"
    req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
        {"page_id": page_c, "content": "冲突原稿",
         "save_id": save_conf, "origin_epoch_id": epoch2}, expect=201)
    _drive_migration(ctx, ws_id, page_c, "v3")
    page_e = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]
    before_conf = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)[1]
    code, err = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                    {"page_id": page_e, "content": "被篡改内容",
                     "save_id": save_conf, "origin_epoch_id": epoch2})
    check(code == 409 and err.get("error") == "save_id_conflict",
          f"同标识不同内容必须明确冲突: {code} {err}")
    _, after_conf = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check(len(after_conf["records"]) == len(before_conf["records"]),
          "冲突不得新增记录")
    check(not any(r["content"] == "被篡改内容" for r in after_conf["records"]),
          "冲突内容不得落库")
    # 相同内容重放仍返回同一结果
    code, rc = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                   {"page_id": page_e, "content": "冲突原稿",
                    "save_id": save_conf, "origin_epoch_id": epoch2})
    check(code == 201 and rc.get("replayed") is True and rc["content"] == "冲突原稿",
          f"同内容应能稳定重放: {code} {rc}")
    ctx["pending_ws"] = ws_id
    ctx["pending_page"] = page_e
    ctx["pending_save"] = save_committed  # 供重启场景复核


# ---------------------------------------------------------------- 场景三

@step("场景三：发布后进程重启，本地恢复的纪元/记录/失效状态一致")
def scenario_restart_consistency(ctx):
    if ctx["no_restart"]:
        print("[verify] 跳过（--no-restart）")
        return
    ws = ctx["ws"]
    _, before = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)

    code, obj = req(ctx, "POST", "/api/admin/shutdown")
    if code == 403:
        print("[verify] 服务端未启用管理关闭，跳过重启场景")
        return
    check(code == 200, f"关闭请求失败: {code} {obj}")
    time.sleep(1)
    wait_health(ctx, timeout=90)  # 编排层按重启策略拉起

    _, after = req(ctx, "GET", f"/api/workspaces/{ws}/state", expect=200)
    check(after["current_epoch"] == before["current_epoch"],
          f"纪元不一致: {after['current_epoch']} != {before['current_epoch']}")
    check([r["content"] for r in after["records"]] ==
          [r["content"] for r in before["records"]], "记录不一致")
    check(after["migration"]["phase"] == "published", "迁移阶段应为已发布")
    inv_before = {p["id"] for p in before["pages"] if p["state"] == "invalidated"}
    inv_after = {p["id"] for p in after["pages"] if p["state"] == "invalidated"}
    check(inv_before and inv_before <= inv_after, "失效页面状态不一致")

    # 重启后，保留保存的稳定标识仍然有效：同标识重放依旧恰好一次、不重复记录。
    pending_ws = ctx.get("pending_ws")
    pending_save = ctx.get("pending_save")
    if pending_ws and pending_save:
        p0 = req(ctx, "POST", f"/api/workspaces/{pending_ws}/pages", expect=201)[1]
        before_records = req(ctx, "GET", f"/api/workspaces/{pending_ws}/state",
                             expect=200)[1]["records"]
        # origin_epoch_id 用最初的旧纪元也无妨：已接受标识优先重放
        code, rr = req(ctx, "POST", f"/api/workspaces/{pending_ws}/records",
                       {"page_id": p0["page_id"], "content": "已提交离线记录",
                        "save_id": pending_save, "origin_epoch_id": ""})
        check(code == 201 and rr.get("replayed") is True,
              f"重启后已接受保存应仍稳定重放: {code} {rr}")
        after_records = req(ctx, "GET", f"/api/workspaces/{pending_ws}/state",
                            expect=200)[1]["records"]
        check(len(after_records) == len(before_records),
              "重启后重放不得重复记录")
        check([r["content"] for r in after_records] ==
              [r["content"] for r in before_records], "重启后记录内容不一致")


# ---------------------------------------------------------------- 主流程

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument("--no-restart", action="store_true",
                        help="跳过进程重启场景（本地无编排重启策略时使用）")
    args = parser.parse_args()
    ctx = {"base": args.base_url.rstrip("/"), "no_restart": args.no_restart}

    print(f"[verify] 目标 {ctx['base']}", flush=True)
    steps = [build_check, unit_tests, smoke_health, smoke_pages, smoke_api,
             scenario_stale_write_rejected, scenario_interruption_recovery,
             scenario_pending_save_recovery, scenario_restart_consistency]
    for s in steps:
        s(ctx)

    print("\n[verify] ================= 结果汇总 =================", flush=True)
    failed = 0
    for name, ok, err in RESULTS:
        print(f"[verify] {'PASS' if ok else 'FAIL'}  {name}" +
              (f"  —— {err}" if err else ""), flush=True)
        failed += 0 if ok else 1
    print(f"[verify] {len(RESULTS) - failed}/{len(RESULTS)} 步通过", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
