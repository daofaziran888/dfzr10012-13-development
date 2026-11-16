"""检查点恢复链路回归测试：准入、唯一恢复锁、重复提交、回退与作业侧消费。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import threading
import unittest

from app.services import checkpoint as checkpoint_service
from app.services import jobops as jobops_service
from app.services import restore
from app.store import store

CHECKPOINT_ID = 2
REQUEST_NO = "restore-02"


class RestoreFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.checkpoints = checkpoint_service.CheckpointService()
        self.jobs = jobops_service.JobopsService()

    def admit(self, request_no: str = REQUEST_NO, entry_id: int = CHECKPOINT_ID, **extra):
        values = {"恢复请求号": request_no, **extra}
        return self.checkpoints.run_action(entry_id, "恢复状态", values)

    def test_manifest_keeps_target_instance_seconds_and_unique_lock(self) -> None:
        result, message = self.admit(恢复耗时=42)
        self.assertIsNotNone(result, message)
        manifest = result["manifest"]
        self.assertEqual(manifest["检查点ID"], "checkpoint-02")
        self.assertEqual(manifest["保存路径"], "/ckpt/02")
        self.assertEqual(manifest["状态大小"], "128MB")
        self.assertEqual(manifest["目标实例"], "JOBO-0002")
        self.assertEqual(manifest["恢复耗时"], 42)
        self.assertEqual(manifest["恢复耗时单位"], "秒")
        self.assertEqual(manifest["状态"], "待处理")
        self.assertTrue(manifest["恢复锁"].startswith("LOCK-"))
        self.assertEqual(manifest["恢复锁状态"], "已占用")
        self.assertEqual(manifest["路径引用"], "已占用")
        self.assertEqual(len(store.rows(restore.MANIFEST_MODULE)), 1)
        self.assertEqual(len(store.rows(restore.PATH_MODULE)), 1)

    def test_duplicate_request_returns_same_manifest_and_instance(self) -> None:
        first, _ = self.admit()
        second, message = self.admit()
        self.assertIsNotNone(second, message)
        self.assertEqual(first["manifest"]["恢复锁"], second["manifest"]["恢复锁"])
        self.assertEqual(first["manifest"]["目标实例"], "JOBO-0002")
        self.assertEqual(second["manifest"]["目标实例"], "JOBO-0002")
        self.assertEqual(len(store.rows(restore.MANIFEST_MODULE)), 1)
        pending = [
            row for row in store.rows("jobops")
            if row.get("检查点ID") == "checkpoint-02" and row.get("status") == "待启动"
        ]
        self.assertEqual([row["实例编号"] for row in pending], ["JOBO-0002"])

    def test_second_request_same_checkpoint_is_rejected(self) -> None:
        self.admit()
        result, message = self.admit("restore-02-other")
        self.assertIsNone(result)
        self.assertIn("同一检查点只允许一个恢复实例", message)
        self.assertEqual(len(store.rows(restore.MANIFEST_MODULE)), 1)

    def test_expired_checkpoint_has_no_admission(self) -> None:
        result, message = self.admit("restore-03", entry_id=3)
        self.assertIsNone(result)
        self.assertIn("已过期", message)
        self.assertEqual(store.rows(restore.MANIFEST_MODULE), [])

    def test_restore_seconds_must_be_numeric(self) -> None:
        result, message = self.admit(恢复耗时="很快就完成")
        self.assertIsNone(result)
        self.assertIn("恢复耗时", message)
        self.assertEqual(store.rows(restore.MANIFEST_MODULE), [])

    def test_status_flow_pending_running_restored_archived(self) -> None:
        result, _ = self.admit()
        lock = result["manifest"]["恢复锁"]
        self.assertEqual(self.checkpoints.run_manifest_action(lock, "完成恢复")[0], None)
        self.assertEqual(self.checkpoints.run_manifest_action(lock, "开始恢复")[0]["状态"], "运行中")
        self.assertEqual(self.checkpoints.run_manifest_action(lock, "完成恢复")[0]["状态"], "已恢复")
        self.assertEqual(self.checkpoints.get_entry(CHECKPOINT_ID)["status"], "已恢复")
        archived = self.checkpoints.run_manifest_action(lock, "归档恢复")[0]
        self.assertEqual(archived["状态"], "已归档")
        self.assertEqual(archived["恢复锁状态"], "已释放")
        self.assertEqual(archived["路径引用"], "已释放")
        self.assertEqual(store.rows(restore.PATH_MODULE), [])
        self.assertEqual(self.checkpoints.run_manifest_action(lock, "归档恢复")[0], None)

    def test_failure_releases_manifest_and_path_reference_together(self) -> None:
        result, _ = self.admit()
        lock = result["manifest"]["恢复锁"]
        self.checkpoints.run_manifest_action(lock, "开始恢复")
        manifest, message = self.checkpoints.run_manifest_action(lock, "恢复失败", "目标实例启动超时")
        self.assertIsNotNone(manifest, message)
        self.assertEqual(manifest["状态"], "已回退")
        self.assertEqual(manifest["路径引用"], "已释放")
        self.assertEqual(store.rows(restore.PATH_MODULE), [])
        # 清单与路径引用一起退回后，检查点可以重新发起恢复。
        retry, retry_message = self.admit("restore-02-retry")
        self.assertIsNotNone(retry, retry_message)
        self.assertEqual(retry["manifest"]["目标实例"], "JOBO-0002")
        self.assertEqual(len(store.rows(restore.ROLLBACK_MODULE)), 1)

    def test_jobops_consumes_same_manifest_and_keeps_own_record(self) -> None:
        result, _ = self.admit()
        lock = result["manifest"]["恢复锁"]
        record, message = self.jobs.consume_manifest(lock)
        self.assertIsNotNone(record, message)
        self.assertEqual(record["恢复锁"], lock)
        self.assertEqual(record["目标实例"], "JOBO-0002")
        self.assertEqual(record["消费状态"], "已接收")
        again, message = self.jobs.consume_manifest(lock)
        self.assertEqual(again["id"], record["id"], message)
        self.assertEqual(len(store.rows(restore.CONSUMPTION_MODULE)), 1)
        self.assertEqual(self.jobs.find_consumption(lock)["处理记录"], record["处理记录"])

        self.checkpoints.run_manifest_action(lock, "开始恢复")
        synced = self.jobs.find_consumption(lock)
        self.assertEqual(synced["清单状态"], "运行中")

    def test_starting_instance_requires_manifest_and_free_path(self) -> None:
        result, _ = self.admit()
        lock = result["manifest"]["恢复锁"]
        instance = restore._store_instance("JOBO-0002")
        entry, message = self.jobs.run_action(instance["id"], "启动作业")
        self.assertIsNotNone(entry, message)
        self.assertEqual(entry["status"], "运行中")
        self.assertEqual(self.checkpoints.get_manifest(lock)["状态"], "运行中")

        # 直接改内部清单，模拟清单被回退/归档后再尝试启动实例。
        restore.find_manifest(lock)["状态"] = restore.STATUS_ROLLED_BACK
        blocked, blocked_message = restore.check_instance_start(instance)
        self.assertFalse(blocked)
        self.assertIn("不能启动", blocked_message)

    def test_two_instances_cannot_hold_same_save_path(self) -> None:
        # 另一个检查点复用同一保存路径时，准入直接拒绝。
        extra, missing = self.checkpoints.create_entry({
            "记录编号": "CHEC-0004",
            "流作业名": "订单流作业",
            "检查点ID": "checkpoint-02-copy",
            "保存路径": "/ckpt/02",
            "状态大小": "128MB",
        })
        self.assertEqual(missing, [])
        first, message = self.admit()
        self.assertIsNotNone(first, message)
        second, message = self.admit("restore-04", entry_id=extra["id"])
        self.assertIsNone(second)
        self.assertIn("/ckpt/02", message)

        holders = [
            row for row in store.rows("jobops")
            if row.get("保存路径") == "/ckpt/02" and row.get("status") in {"待启动", "运行中", "已暂停"}
        ]
        self.assertEqual([row["实例编号"] for row in holders], ["JOBO-0002"])

    def test_concurrent_restore_only_grants_one_lock(self) -> None:
        results: list[tuple[dict | None, str]] = []
        barrier = threading.Barrier(8)

        def worker(index: int) -> None:
            barrier.wait()
            results.append(self.admit(f"restore-02-{index}"))

        threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        granted = [payload for payload, _ in results if payload is not None]
        self.assertEqual(len(granted), 1)
        self.assertEqual(len(store.rows(restore.MANIFEST_MODULE)), 1)
        self.assertEqual(len(store.rows(restore.PATH_MODULE)), 1)


if __name__ == "__main__":
    unittest.main()
