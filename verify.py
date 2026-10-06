#!/usr/bin/env python3
"""verify 服务：在编排环境内完成全部验收并自行退出，以退出码报告结果。

步骤穿插执行：构建检查 -> 代码测试 -> 健康/页面 HTTP 冒烟 -> API 冒烟 ->
场景一（两页面迁移 + 旧写拒绝 + 并发迁移不建第二候选）->
场景二（复制中断重开不展示部分数据 + 校验阶段续用同一候选）->
场景三（发布后进程重启，本地恢复的纪元/记录/失效状态一致）->
场景四（保留保存恢复：已提交未回执恰好一次 / 未提交旧纪元明确拒绝 /
同标识内容冲突 / 两页面旧写拒绝 / 复制中断后完整旧纪元账本不丢）。

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

@step("场景四：保留保存恢复——已提交未回执恰好一次 / 未提交旧纪元拒绝 / 标识冲突")
def scenario_pending_save_recovery(ctx):
    """独立工作区内复现「两页面同开 + 短暂断线保留保存 + 另一页完成迁移」。"""
    ws_name = f"verify-pending-{uuid.uuid4().hex[:8]}"
    ws = req(ctx, "POST", "/api/workspaces", {"name": ws_name}, expect=201)[1]
    ws_id = ws["id"]
    old_epoch = ws["current_epoch"]["id"]

    # 两个页面同时打开同一工作区
    page_a = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]
    page_b = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]

    req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
        {"page_id": page_a, "content": "既有记录-1"}, expect=201)

    # 情形一：请求在迁移前已落库，但客户端未收到结果（save_id 由客户端稳定携带）
    save_committed = f"save-{uuid.uuid4().hex}"
    code, first = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                      {"page_id": page_a, "content": "已提交未回执",
                       "save_id": save_committed, "origin_epoch_id": old_epoch})
    check(code == 201 and first.get("replayed") is False,
          f"首次接受应为 201/replayed=false: {code} {first}")
    # 情形二：请求从未到达服务端即断线，只存在于旧页面的本地保留队列
    save_unsent = f"save-{uuid.uuid4().hex}"

    # 另一页完成目标版本迁移并发布
    req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/start",
        {"page_id": page_b, "target_version": "v2"}, expect=200)
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/copy",
                   {"page_id": page_b, "batch_size": 50}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/validate",
        {"page_id": page_b}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/publish",
               {"page_id": page_b}, expect=200)
    epoch2 = s["current_epoch"]["id"]
    check(s["current_epoch"]["number"] == 2, "应发布到纪元 #2")

    # 两页面旧写拒绝：两个旧页面的（无标识）迟到写入都被拒绝
    for pid in (page_a, page_b):
        code, err = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                        {"page_id": pid, "content": "两页旧写"})
        check(code == 409 and err.get("error") == "page_not_active",
              f"旧页面 {pid} 的写入应被拒绝: {code} {err}")

    # 迁移复制会为新纪元的记录重新分配 id（旧纪元记录保留不删），因此跨纪元稳定的
    # 业务结果是 seq + content；记录 id 在同一纪元内保持稳定。
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    committed_e2 = [r for r in s["records"] if r["content"] == "已提交未回执"][0]

    # —— 情形一恢复：旧页面（已失效）重放已接受的保存 —— 稳定返回原业务结果，200/replayed
    code, replay1 = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                        {"page_id": page_a, "content": "已提交未回执",
                         "save_id": save_committed, "origin_epoch_id": old_epoch})
    check(code == 200 and replay1.get("replayed") is True,
          f"已接受保存的重放应稳定幂等成功: {code} {replay1}")
    check(replay1["seq"] == first["seq"] and replay1["id"] == committed_e2["id"],
          "重放必须返回新纪元中对应的同一条业务结果（seq 稳定、id 为新纪元记录）")

    # 重开前一页（新页面，围栏新纪元）再恢复一次：仍命中同一保存，不重复记录
    page_c = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]
    code, replay2 = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                        {"page_id": page_c, "content": "已提交未回执",
                         "save_id": save_committed, "origin_epoch_id": old_epoch})
    check(code == 200 and replay2["replayed"] is True
          and replay2["id"] == replay1["id"], f"同纪元再次重放应返回同一条: {code} {replay2}")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    contents = [r["content"] for r in s["records"]]
    check(len(s["records"]) == 2, f"迁移后记录应恰好 2 条（无重复写入）: {contents}")
    check(contents.count("已提交未回执") == 1, f"该保存只能落库一次: {contents}")

    # —— 情形二恢复：从未被接受的保存，分别由旧页面与新页面重放，都按旧纪元拒绝 ——
    for pid in (page_a, page_c):
        code, err = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                        {"page_id": pid, "content": "未提交即断线",
                         "save_id": save_unsent, "origin_epoch_id": old_epoch})
        check(code == 409 and err.get("error") == "stale_epoch_save",
              f"未送达的旧纪元保存必须明确拒绝（页面 {pid}）: {code} {err}")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check(len(s["records"]) == 2, "被拒绝的保存不得出现在新纪元")
    check("未提交即断线" not in [r["content"] for r in s["records"]],
          "未送达保存不得借新页面变成新纪元写入")

    # —— 相同稳定保存标识但内容不同：明确冲突 ——
    code, err = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                    {"page_id": page_c, "content": "被篡改的内容",
                     "save_id": save_committed, "origin_epoch_id": old_epoch})
    check(code == 409 and err.get("error") == "save_content_conflict",
          f"同标识不同内容必须报冲突: {code} {err}")
    check(err.get("accepted_seq") == first["seq"], "冲突响应应指明已接受的业务结果")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check("被篡改的内容" not in [r["content"] for r in s["records"]], "冲突内容不得写入")

    # —— 复制中断后的完整旧纪元：候选回收不得丢失/错误归属保存账本 ——
    code, s3 = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                   {"page_id": page_c, "content": "新纪元保留记录",
                    "save_id": f"save-{uuid.uuid4().hex}", "origin_epoch_id": epoch2})
    check(code == 201, f"新纪元新保存应被接受: {code} {s3}")
    save_new = s3["save_id"]
    req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/start",
        {"page_id": page_c, "target_version": "v3"}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/copy",
               {"page_id": page_c, "batch_size": 1}, expect=200)
    check(s["migration"]["phase"] == "copying", "应停留在部分复制状态")
    req(ctx, "POST", f"/api/workspaces/{ws_id}/pages/{page_c}/close", expect=200)
    page_d = req(ctx, "POST", f"/api/workspaces/{ws_id}/pages", expect=201)[1]["page_id"]
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check(s["migration"]["phase"] == "aborted" and s["current_epoch"]["id"] == epoch2,
          "复制中断后候选应回收且仍读完整旧纪元")
    # 旧纪元账本完好：两个已接受保存都可稳定重放，记录不增
    for save_id, content, expect_id in (
            (save_committed, "已提交未回执", committed_e2["id"]),
            (save_new, "新纪元保留记录", s3["id"])):
        code, r = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                      {"page_id": page_d, "content": content,
                       "save_id": save_id, "origin_epoch_id": epoch2})
        check(code == 200 and r.get("replayed") is True and r["id"] == expect_id,
              f"候选回收后 {save_id} 应仍可幂等重放: {code} {r}")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check(len(s["records"]) == 3, "回收候选前后记录数必须稳定为 3")

    # 完整走完第二次迁移：账本随复制进入新纪元（#3），重放仍恰好一次
    req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/start",
        {"page_id": page_d, "target_version": "v3"}, expect=200)
    while True:
        _, s = req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/copy",
                   {"page_id": page_d, "batch_size": 50}, expect=200)
        if s["migration"]["phase"] == "validating":
            break
    req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/validate",
        {"page_id": page_d}, expect=200)
    _, s = req(ctx, "POST", f"/api/workspaces/{ws_id}/migration/publish",
               {"page_id": page_d}, expect=200)
    check(s["current_epoch"]["number"] == 3 and s["current_epoch"]["version"] == "v3",
          "应发布到纪元 #3 / v3")
    epoch3 = s["current_epoch"]["id"]
    code, r = req(ctx, "POST", f"/api/workspaces/{ws_id}/records",
                  {"page_id": page_d, "content": "已提交未回执",
                   "save_id": save_committed, "origin_epoch_id": old_epoch})
    check(code == 200 and r.get("replayed") is True
          and r["epoch_id"] == epoch3 and r["seq"] == first["seq"]
          and r["content"] == "已提交未回执",
          f"跨两个纪元重放应命中复制归属后的账本且业务结果（seq/内容）不变: {code} {r}")
    _, s = req(ctx, "GET", f"/api/workspaces/{ws_id}/state", expect=200)
    check([r["content"] for r in s["records"]] ==
          ["既有记录-1", "已提交未回执", "新纪元保留记录"],
          f"新纪元记录应恰好一次且完整: {[r['content'] for r in s['records']]}")


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
