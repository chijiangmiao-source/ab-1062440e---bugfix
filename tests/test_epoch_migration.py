"""纪元迁移状态机的单元测试。"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import ApiError, Store  # noqa: E402


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db_path, page_ttl_seconds=45)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    # ------------------------------------------------------------ 辅助

    def make_ws(self, name="样地A", record_count=0):
        ws = self.store.create_workspace(name)
        page = self.store.open_page(ws["id"])
        for i in range(record_count):
            self.store.add_record(ws["id"], page["page_id"], f"观测记录-{i + 1}")
        return ws, page["page_id"]

    def drive_to(self, ws_id, page_id, target_phase, version="v2", batch=1, start=True):
        """把迁移推进到指定阶段。start=False 表示迁移已发起，直接续推。"""
        if start:
            self.store.start_migration(ws_id, page_id, version)
        if target_phase == "copying":
            return
        while True:
            s = self.store.copy_batch(ws_id, page_id, batch)
            if s["migration"]["phase"] == "validating":
                break
        if target_phase == "validating":
            return
        self.store.validate_migration(ws_id, page_id)
        if target_phase == "publishing":
            return
        self.store.publish_migration(ws_id, page_id)

    def assert_api_error(self, status, code, fn, *args, **kwargs):
        with self.assertRaises(ApiError) as ctx:
            fn(*args, **kwargs)
        self.assertEqual(ctx.exception.status, status)
        self.assertEqual(ctx.exception.code, code)
        return ctx.exception

    # ------------------------------------------------------------ 基础

    def test_workspace_starts_at_epoch_one(self):
        ws, _ = self.make_ws()
        self.assertEqual(ws["current_epoch"]["number"], 1)
        self.assertEqual(ws["current_epoch"]["version"], "v1")
        self.assertEqual(ws["records"], [])

    def test_add_and_read_records(self):
        ws, page = self.make_ws(record_count=3)
        s = self.store.get_state(ws["id"])
        self.assertEqual([r["content"] for r in s["records"]],
                         ["观测记录-1", "观测记录-2", "观测记录-3"])
        self.assertEqual([r["seq"] for r in s["records"]], [1, 2, 3])

    def test_write_requires_active_page(self):
        ws, page = self.make_ws()
        self.store.close_page(ws["id"], page)
        self.assert_api_error(409, "page_not_active",
                              self.store.add_record, ws["id"], page, "迟到记录")

    # ------------------------------------------------------------ 迁移主流程

    def test_full_migration_and_stale_write_rejected(self):
        """两个页面打开同一工作区：迁移完成后，另一页的迟到保存被拒绝。"""
        ws, page_a = self.make_ws(record_count=4)
        page_b = self.store.open_page(ws["id"])["page_id"]

        self.store.start_migration(ws["id"], page_a, "v2")
        # 发布前：迁移进行中，另一页的保存被拒绝
        self.assert_api_error(409, "migration_in_progress",
                              self.store.add_record, ws["id"], page_b, "迟到记录")
        # 复制 -> 校验 -> 发布
        self.drive_to(ws["id"], page_a, "publishing", start=False)
        self.store.publish_migration(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(s["current_epoch"]["version"], "v2")
        self.assertEqual([r["content"] for r in s["records"]],
                         [f"观测记录-{i}" for i in range(1, 5)])
        # 两个旧页面均已失效
        states = {p["id"]: p["state"] for p in s["pages"]}
        self.assertEqual(states[page_a], "invalidated")
        self.assertEqual(states[page_b], "invalidated")
        # 发布后：旧页面的迟到保存仍被拒绝并提示重新载入
        err = self.assert_api_error(409, "page_not_active",
                                    self.store.add_record, ws["id"], page_b, "迟到记录")
        self.assertIn("重新载入", err.message)
        # 重开页面后只能读到新纪元，且可继续写入
        page_c = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_c, "新纪元记录")
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 5)
        self.assertEqual(s["records"][-1]["content"], "新纪元记录")

    def test_concurrent_migration_creates_no_second_candidate(self):
        ws, page_a = self.make_ws(record_count=2)
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.start_migration(ws["id"], page_a, "v2")
        self.assert_api_error(409, "migration_active",
                              self.store.start_migration, ws["id"], page_b, "v2b")
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 1)

    def test_reads_never_come_from_candidate(self):
        """复制进行中读取到的仍是完整旧纪元，候选的部分数据不可见。"""
        ws, page_a = self.make_ws(record_count=5)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)  # 只复制 2/5
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 5)
        self.assertEqual(s["migration"]["copied"], 2)

    # ------------------------------------------------------------ 中断恢复

    def test_copy_interruption_recycles_candidate(self):
        """复制阶段页面关闭：候选被安全回收，绝不展示部分复制数据。"""
        ws, page_a = self.make_ws(record_count=5)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)
        self.store.close_page(ws["id"], page_a)  # 模拟页面在复制之间关闭

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        self.assertIsNone(s["migration"]["candidate_epoch"])
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 5)  # 完整旧纪元
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 0)

    def test_validating_resumes_same_candidate(self):
        """校验阶段页面关闭：后来页面续用同一候选并完成迁移。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "validating")
        cand_before = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]
        self.store.close_page(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "validating")
        self.assertEqual(s["migration"]["candidate_epoch"], cand_before)  # 同一候选

        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.validate_migration(ws["id"], page_b)
        self.store.publish_migration(ws["id"], page_b)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_publishing_completes_after_owner_close(self):
        """发布阶段页面关闭：恢复时把原子发布补齐。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "publishing")
        self.store.close_page(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "published")
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_crash_recovery_on_reopen_store(self):
        """模拟进程在复制中途崩溃：重开存储后候选被回收，数据不残缺。"""
        ws, page_a = self.make_ws(record_count=4)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 1)
        self.store.close()  # 模拟进程崩溃（事务已提交到 copying 阶段）

        self.store = Store(self.db_path, page_ttl_seconds=45)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 4)

    # ------------------------------------------------------------ 校验与持久化

    def test_validate_mismatch_marks_failed_and_allows_retry(self):
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "validating")
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        # 人为破坏候选内容
        self.store.conn.execute(
            "UPDATE records SET content='被篡改' WHERE epoch_id=? AND seq=1", (cand,))
        s = self.store.validate_migration(ws["id"], page_a)
        self.assertEqual(s["migration"]["phase"], "failed")
        # 失败后可重新发起：旧候选被回收，新候选唯一
        self.store.start_migration(ws["id"], page_a, "v2")
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 1)
        self.drive_to(ws["id"], page_a, "published", start=False)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_persistence_after_publish(self):
        """发布后重开存储：纪元、记录、页面失效状态一致。"""
        ws, page_a = self.make_ws(record_count=4)
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.drive_to(ws["id"], page_a, "published")
        before = self.store.get_state(ws["id"])
        self.store.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        after = self.store.get_state(ws["id"])
        self.assertEqual(after["current_epoch"], before["current_epoch"])
        self.assertEqual([r["content"] for r in after["records"]],
                         [r["content"] for r in before["records"]])
        states = {p["id"]: p["state"] for p in after["pages"]}
        self.assertEqual(states[page_a], "invalidated")
        self.assertEqual(states[page_b], "invalidated")
        self.assertEqual(after["migration"]["phase"], "published")

    # ------------------------------------------------ 稳定保存标识（离线恢复）

    def _commit_without_receipt(self, ws_id, page, content, save_id, epoch_id):
        """模拟「已提交但未获回执」：直接落库并建立稳定标识，客户端未收到结果。"""
        return self.store.add_record(ws_id, page, content, save_id, epoch_id)

    def test_committed_save_replays_once_after_migration(self):
        """已提交未获回执：迁移后恢复只能稳定返回同一结果，绝不重复记录。"""
        ws, page_a = self.make_ws(record_count=2)
        epoch1 = ws["current_epoch"]["id"]
        page_b = self.store.open_page(ws["id"])["page_id"]

        # 页B 的保存已在迁移前落库，但客户端未收到回执 -> 进入本地保留
        r1 = self._commit_without_receipt(ws["id"], page_b, "离线观测-X", "save-1", epoch1)
        self.assertFalse(r1["replayed"])
        self.assertEqual(r1["seq"], 3)
        before = self.store.get_state(ws["id"])
        self.assertEqual(len(before["records"]), 3)

        # 页A 完成目标版本迁移并发布
        self.drive_to(ws["id"], page_a, "published")

        # 重开页B（持有新纪元围栏）后恢复该保存：
        # 旧页面已失效，需开新页面；恢复请求携带原 save_id 与旧纪元
        page_c = self.store.open_page(ws["id"])["page_id"]
        r2 = self.store.add_record(ws["id"], page_c, "离线观测-X", "save-1", epoch1)
        self.assertTrue(r2["replayed"])
        # 稳定返回当初接受的业务结果：同一 seq，记录 ID 指向新纪元中的同一条
        self.assertEqual(r2["seq"], r1["seq"])
        self.assertEqual(r2["content"], "离线观测-X")

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        # 恰好一次：新纪元只有复制来的 3 条，没有第 4 条重复记录
        self.assertEqual(len(s["records"]), 3)
        self.assertEqual(s["records"][-1]["content"], "离线观测-X")
        # 再恢复一次仍是同一结果，仍不新增
        r3 = self.store.add_record(ws["id"], page_c, "离线观测-X", "save-1", epoch1)
        self.assertTrue(r3["replayed"])
        self.assertEqual(r3["id"], r2["id"])
        self.assertEqual(len(self.store.get_state(ws["id"])["records"]), 3)

    def test_uncommitted_old_epoch_save_rejected_after_reopen(self):
        """未提交即断线：请求从未到达服务端，迁移后重开放送必须被拒绝。"""
        ws, page_a = self.make_ws(record_count=2)
        epoch1 = ws["current_epoch"]["id"]
        page_b = self.store.open_page(ws["id"])["page_id"]

        # 页A 完成迁移（页B 的保存从未到达服务端，服务端无任何记录）
        self.drive_to(ws["id"], page_a, "published")
        epoch2 = self.store.get_state(ws["id"])["current_epoch"]["id"]
        self.assertNotEqual(epoch1, epoch2)

        # 重开页B（新围栏 = 新纪元），恢复一个指向旧纪元、从未被接受的保存
        page_c = self.store.open_page(ws["id"])["page_id"]
        err = self.assert_api_error(
            409, "stale_epoch",
            self.store.add_record, ws["id"], page_c, "断线观测-Y", "save-2", epoch1)
        self.assertIn("旧纪元", err.message)
        # 不能借新页面变成新纪元写入
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 2)
        self.assertFalse(any(r["content"] == "断线观测-Y" for r in s["records"]))
        refs = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM save_refs WHERE save_id=?", ("save-2",)).fetchone()["c"]
        self.assertEqual(refs, 0)

    def test_same_save_id_different_content_conflicts(self):
        """相同稳定保存标识但内容不同：必须明确冲突，不能当作重放或新记录。"""
        ws, page = self.make_ws(record_count=0)
        epoch = ws["current_epoch"]["id"]
        self.store.add_record(ws["id"], page, "内容A", "save-9", epoch)
        err = self.assert_api_error(
            409, "save_id_conflict",
            self.store.add_record, ws["id"], page, "内容B", "save-9", epoch)
        self.assertEqual(err.extra["save_id"], "save-9")
        # 冲突不产生新记录，仍只有最初那一条
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 1)
        self.assertEqual(s["records"][0]["content"], "内容A")

    def test_conflict_same_save_id_after_migration(self):
        """迁移后以同标识、不同内容恢复：明确冲突，且不写入新纪元。"""
        ws, page_a = self.make_ws(record_count=1)
        epoch1 = ws["current_epoch"]["id"]
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_b, "原稿内容", "save-7", epoch1)
        self.drive_to(ws["id"], page_a, "published")

        page_c = self.store.open_page(ws["id"])["page_id"]
        self.assert_api_error(
            409, "save_id_conflict",
            self.store.add_record, ws["id"], page_c, "被篡改内容", "save-7", epoch1)
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 2)
        self.assertFalse(any(r["content"] == "被篡改内容" for r in s["records"]))

    def test_save_refs_survive_copy_interruption_and_recycle(self):
        """复制中断回收候选：已接受保存的标识不丢失，重试仍稳定重放。"""
        ws, page_a = self.make_ws(record_count=3)
        epoch1 = ws["current_epoch"]["id"]
        r1 = self.store.add_record(ws["id"], page_a, "待复制记录", "save-k", epoch1)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 1)
        self.store.close_page(ws["id"], page_a)  # 复制中断 -> 回收候选

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        # 仍在旧纪元：标识保留，重试稳定返回同一结果
        page_b = self.store.open_page(ws["id"])["page_id"]
        r2 = self.store.add_record(ws["id"], page_b, "待复制记录", "save-k", epoch1)
        self.assertTrue(r2["replayed"])
        self.assertEqual(r2["id"], r1["id"])
        self.assertEqual(len(self.store.get_state(ws["id"])["records"]), 4)

        # 重新发起并完成迁移，标识应正确改指新纪元
        self.drive_to(ws["id"], page_b, "published")
        page_c = self.store.open_page(ws["id"])["page_id"]
        r3 = self.store.add_record(ws["id"], page_c, "待复制记录", "save-k", epoch1)
        self.assertTrue(r3["replayed"])
        self.assertEqual(r3["seq"], r1["seq"])
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 4)

    def test_replay_from_invalidated_old_page_still_returns_result(self):
        """已接受保存的重试来自已失效旧页面，仍稳定返回结果而非被围栏拒绝。"""
        ws, page_a = self.make_ws(record_count=2)
        epoch1 = ws["current_epoch"]["id"]
        page_b = self.store.open_page(ws["id"])["page_id"]
        r1 = self.store.add_record(ws["id"], page_b, "旧页离线保存", "save-z", epoch1)
        self.drive_to(ws["id"], page_a, "published")

        # 页B 已被失效；它直接重试自己那条已落库的保存
        states = {p["id"]: p["state"] for p in self.store.get_state(ws["id"])["pages"]}
        self.assertEqual(states[page_b], "invalidated")
        r2 = self.store.add_record(ws["id"], page_b, "旧页离线保存", "save-z", epoch1)
        self.assertTrue(r2["replayed"])
        self.assertEqual(r2["seq"], r1["seq"])
        self.assertEqual(len(self.store.get_state(ws["id"])["records"]), 3)

        # 但已失效旧页面的「新」保存（无已接受标识）仍按围栏拒绝
        self.assert_api_error(
            409, "page_not_active",
            self.store.add_record, ws["id"], page_b, "旧页另一条新保存", "save-new", epoch1)

    def test_normal_new_records_without_save_id_unaffected(self):
        """无 save_id 的普通新记录仍是独立写入，保持既有行为。"""
        ws, page = self.make_ws(record_count=1)
        a = self.store.add_record(ws["id"], page, "普通记录A")
        b = self.store.add_record(ws["id"], page, "普通记录B")
        self.assertNotEqual(a["id"], b["id"])
        self.assertFalse(a["replayed"])
        self.assertEqual([a["seq"], b["seq"]], [2, 3])


if __name__ == "__main__":
    unittest.main()
