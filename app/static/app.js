/* 野外实验站离线记录页前端：每个标签页是一个持有纪元围栏的“页面”。 */
"use strict";

const $ = (id) => document.getElementById(id);

const PHASE_LABELS = {
  idle: "空闲", copying: "复制中", validating: "校验中",
  publishing: "待发布", published: "已发布", failed: "失败",
  aborted: "已中止（候选已回收）",
};
const PAGE_STATE_LABELS = { active: "活动", invalidated: "已失效", closed: "已关闭" };

let wsId = localStorage.getItem("fs_ws_id") || null;
let pageId = null;
let lastState = null;
const PENDING_SAVES_KEY = "fs_pending_saves";

function readPendingSaves() {
  try {
    const saved = JSON.parse(localStorage.getItem(PENDING_SAVES_KEY) || "[]");
    return Array.isArray(saved) ? saved.filter((item) =>
      item && typeof item.id === "string" && typeof item.content === "string") : [];
  } catch (_) {
    return [];
  }
}

function writePendingSaves(saves) {
  localStorage.setItem(PENDING_SAVES_KEY, JSON.stringify(saves));
}

function newSaveId() {
  if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function removePendingSave(saveId) {
  writePendingSaves(readPendingSaves().filter((save) => save.id !== saveId));
}

async function sendRecord(save) {
  return api("POST", `/api/workspaces/${wsId}/records`, {
    page_id: pageId,
    content: save.content,
    save_id: save.id,
    origin_epoch_id: save.originEpochId,
  });
}

async function submitRecord(content) {
  const save = {
    id: newSaveId(), content,
    originEpochId: lastState && lastState.current_epoch ? lastState.current_epoch.id : "",
  };
  try {
    return await sendRecord(save);
  } catch (e) {
    if (e.status) throw e;
    writePendingSaves([...readPendingSaves(), save]);
    throw e;
  }
}

async function flushPendingSaves() {
  for (const save of readPendingSaves()) {
    try {
      await sendRecord(save);
      removePendingSave(save.id);
    } catch (e) {
      if (!e.status) return;
      removePendingSave(save.id);
    }
  }
}

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.message || res.statusText);
    err.status = res.status;
    err.code = data.error;
    throw err;
  }
  return data;
}

async function ensureWorkspace() {
  const list = await api("GET", "/api/workspaces");
  const select = $("ws-select");
  select.innerHTML = "";
  for (const w of list.workspaces) {
    const opt = document.createElement("option");
    opt.value = w.id;
    opt.textContent = `${w.name}（纪元 #${w.epoch_number} · ${w.epoch_version}）`;
    select.appendChild(opt);
  }
  if (wsId && list.workspaces.some((w) => w.id === wsId)) {
    select.value = wsId;
    return;
  }
  if (list.workspaces.length > 0) {
    wsId = list.workspaces[0].id;
  } else {
    const ws = await api("POST", "/api/workspaces", { name: "默认工作区" });
    wsId = ws.id;
    await ensureWorkspace();
    return;
  }
  localStorage.setItem("fs_ws_id", wsId);
  select.value = wsId;
}

async function openPage() {
  const r = await api("POST", `/api/workspaces/${wsId}/pages`);
  pageId = r.page_id;
}

async function refresh() {
  try {
    const s = await api("GET", `/api/workspaces/${wsId}/state?page_id=${pageId}`);
    lastState = s;
    render(s);
  } catch (e) {
    showBanner(`状态读取失败：${e.message}`, false);
  }
}

function render(s) {
  $("epoch").textContent = `纪元 #${s.current_epoch.number} · 版本 ${s.current_epoch.version}`;
  const mig = s.migration;
  const phase = mig ? mig.phase : "idle";
  $("phase").textContent = PHASE_LABELS[phase] || phase;

  if (mig && mig.phase !== "idle") {
    const cand = mig.candidate_epoch ? `候选纪元 #${mig.candidate_epoch.number}` : "无候选";
    $("migration-info").textContent =
      `目标版本 ${mig.target_version || "—"} · ${cand} · 已复制 ${mig.copied}/${mig.total}` +
      (mig.error ? ` · ${mig.error}` : "");
    $("mig-progress").max = Math.max(mig.total, 1);
    $("mig-progress").value = mig.copied;
  } else {
    $("migration-info").textContent = "当前无迁移。发起迁移后：复制 → 校验 → 原子发布。";
    $("mig-progress").value = 0;
  }

  const me = s.pages.find((p) => p.id === pageId);
  const meOk = me && me.state === "active";
  $("me").textContent = me
    ? `${pageId.slice(0, 8)}…（${PAGE_STATE_LABELS[me.state]}）`
    : "未注册";
  $("rec-content").disabled = !meOk;
  $("btn-add").disabled = !meOk;
  if (me && me.state === "invalidated") {
    showBanner("本页面持有的纪元围栏已失效，迟到保存会被拒绝。请重新载入以读取新纪元。", true);
  } else if (me && me.state === "closed") {
    showBanner("本页面会话已关闭。请重新载入。", true);
  }

  const ul = $("records");
  ul.innerHTML = "";
  for (const r of s.records) {
    const li = document.createElement("li");
    li.textContent = `#${r.seq} ${r.content}`;
    ul.appendChild(li);
  }
  if (s.records.length === 0) {
    const li = document.createElement("li");
    li.className = "muted";
    li.textContent = "（当前纪元暂无记录）";
    ul.appendChild(li);
  }

  const tbody = $("pages");
  tbody.innerHTML = "";
  for (const p of s.pages.slice(0, 12)) {
    const tr = document.createElement("tr");
    if (p.id === pageId) tr.className = "me";
    const state = PAGE_STATE_LABELS[p.state] || p.state;
    tr.innerHTML =
      `<td>${p.id.slice(0, 8)}…${p.id === pageId ? "（本页）" : ""}</td>` +
      `<td>纪元 #${p.epoch_number}</td>` +
      `<td class="st-${p.state}">${state}</td>` +
      `<td>${(p.last_seen || "").slice(11, 19)}</td>`;
    tbody.appendChild(tr);
  }
}

function showBanner(msg, withReload) {
  const b = $("banner");
  b.innerHTML = "";
  b.appendChild(document.createTextNode(msg + " "));
  if (withReload) {
    const btn = document.createElement("button");
    btn.textContent = "重新载入";
    btn.onclick = () => location.reload();
    b.appendChild(btn);
  }
  b.classList.remove("hidden");
}

function hideBanner() {
  $("banner").classList.add("hidden");
}

async function guard(fn) {
  try {
    await fn();
    hideBanner();
  } catch (e) {
    showBanner(`${e.status === 409 ? "操作被拒绝" : "操作失败"}：${e.message}`, e.status === 409);
  } finally {
    await refresh();
  }
}

const migCall = (action, body = {}) =>
  api("POST", `/api/workspaces/${wsId}/migration/${action}`, { page_id: pageId, ...body });

async function boot() {
  await ensureWorkspace();
  await openPage();
  await refresh();
  await flushPendingSaves();
  await refresh();
  setInterval(refresh, 2000);

  window.addEventListener("pagehide", () => {
    if (wsId && pageId) {
      navigator.sendBeacon(`/api/workspaces/${wsId}/pages/${pageId}/close`, new Blob([]));
    }
  });

  $("ws-create").onclick = () =>
    guard(async () => {
      const name = $("ws-name").value.trim();
      if (!name) return;
      const ws = await api("POST", "/api/workspaces", { name });
      localStorage.setItem("fs_ws_id", ws.id);
      location.reload();
    });
  $("ws-select").onchange = () => {
    localStorage.setItem("fs_ws_id", $("ws-select").value);
    location.reload();
  };

  $("btn-add").onclick = () =>
    guard(async () => {
      const content = $("rec-content").value.trim();
      if (!content) return;
      await submitRecord(content);
      $("rec-content").value = "";
    });

  $("btn-start").onclick = () =>
    guard(async () => {
      const tv = $("target-version").value.trim() ||
        `v${(lastState ? lastState.current_epoch.number : 1) + 1}`;
      await migCall("start", { target_version: tv });
    });
  $("btn-copy").onclick = () => guard(() => migCall("copy", { batch_size: 1 }));
  $("btn-validate").onclick = () => guard(() => migCall("validate"));
  $("btn-publish").onclick = () => guard(() => migCall("publish"));

  $("btn-auto").onclick = () =>
    guard(async () => {
      const tv = $("target-version").value.trim() ||
        `v${(lastState ? lastState.current_epoch.number : 1) + 1}`;
      let s = await migCall("start", { target_version: tv });
      while (s.migration && s.migration.phase === "copying") {
        s = await migCall("copy", { batch_size: 2 });
      }
      await migCall("validate");
      await migCall("publish");
    });
}

boot().catch((e) => showBanner(`初始化失败：${e.message}`, false));
