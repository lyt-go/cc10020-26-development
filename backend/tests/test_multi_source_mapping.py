"""多来源字段映射 → 取值明细 → 质量校验 → 质检台账 链路测试。

场景（题面）：
- 同一目标字段接收两个来源：账面列=900、复核列=1100，两者均可解析；
- 目标字段取值明细保留候选值、取值口径与来源；
- 并发运行同一映射版本只产生一个实际业务值（单飞合并）；
- 质量校验跨模块读取目标字段取值明细，按采用金额执行上限检查，
  阻断结果不同步到下游，但写入质检台账；
- 质量规则上限 1000 时，可从输出与质量结果核对选择的值及其影响；
- 质检台账能查到实际转换值及未采用的候选值。
"""
from __future__ import annotations

import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.data_quality_rule import LEDGER_KEY, DataQualityRuleService
from app.services.data_transform import DataTransformService

TRANSFORM_KEY = "data_transform"
QUALITY_KEY = "data_quality_rule"


class MultiSourceTransformTestCase(unittest.TestCase):
    """多来源字段映射与质量阻断完整链路。"""

    def setUp(self):
        # test_data_chains 的隔离夹具会 reload 服务模块并把其 json_store 指向临时库，
        # 夹具结束后不回绑；这里在每个用例前把两个服务重新绑定到当前全局 store，
        # 保证本文件无论在什么执行顺序下都使用同一个持久化实例。
        import app.persistence
        import app.services.data_quality_rule as dq_mod
        import app.services.data_transform as dt_mod

        self.store = app.persistence.json_store
        dt_mod.json_store = self.store
        dq_mod.json_store = self.store
        self.transform_svc = DataTransformService()
        self.quality_svc = DataQualityRuleService()

        self.store.truncate(TRANSFORM_KEY)
        self.store.truncate(QUALITY_KEY)
        self.store.truncate(LEDGER_KEY)

        # 转换规则：目标金额同时接收账面列（900）与复核列（1100），取最大值
        self.rule = self.store.insert(TRANSFORM_KEY, {
            "规则编号": "TRANS-MULTI-001",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": (
                '{"目标金额": {"sources": ['
                '{"source": "账面列", "caliber": "账面口径"}, '
                '{"source": "复核列", "caliber": "复核口径"}], '
                '"strategy": "max"}}'
            ),
            "转换函数": "passthrough",
            "默认值填充": "使用默认值",
            "异常策略": "跳过",
            "规则版本": "v-multi-1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

        # 质量规则：目标金额上限 1000，超限阻断
        self.store.insert(QUALITY_KEY, {
            "规则编号": "QR-UPPER-1000",
            "数据来源": "data_transform",
            "校验字段": "目标金额",
            "校验表达式": "is_numeric",
            "阈值下界": "0",
            "阈值上界": "1000",
            "阻断策略": "阻断",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

        self.source_data = {"账面列": "900", "复核列": "1100"}

    # ------------------------------------------------------------------
    # 1. 两个来源都可解析，取值明细保留候选值、口径、来源
    # ------------------------------------------------------------------
    def test_candidates_and_value_details_preserved(self):
        result = self.transform_svc.apply_transform(self.rule["id"], self.source_data)
        self.assertTrue(result["ok"], result.get("errors"))

        details = result["field_value_details"]
        self.assertEqual(len(details), 1)
        detail = details[0]
        self.assertEqual(detail["target_field"], "目标金额")
        self.assertEqual(detail["selection_strategy"], "max")

        candidates = detail["candidates"]
        self.assertEqual(len(candidates), 2)
        self.assertEqual(
            [(c["source_path"], c["caliber"], c["converted_value"], c["parseable"])
             for c in candidates],
            [("账面列", "账面口径", "900", True),
             ("复核列", "复核口径", "1100", True)],
        )

        # max 策略：采用复核列 1100
        self.assertEqual(detail["adopted"], "1100")
        self.assertEqual(detail["adopted_source"], "复核列")
        self.assertEqual(detail["adopted_caliber"], "复核口径")
        self.assertEqual(result["data"]["目标金额"], "1100")
        # 未采用候选（账面列 900）仍保留在明细里
        rejected = [c for c in candidates if not c["adopted"]]
        self.assertEqual([(c["source_path"], c["converted_value"]) for c in rejected],
                         [("账面列", "900")])

    # ------------------------------------------------------------------
    # 2. 上限 1000：采用值 1100 超限 → 阻断，质量结果指向本次使用字段
    # ------------------------------------------------------------------
    def test_upper_limit_blocks_adopted_value(self):
        result = self.transform_svc.apply_transform(self.rule["id"], self.source_data)
        quality = result["quality_check"]

        self.assertTrue(quality["blocked"])
        self.assertFalse(quality["passed"])
        self.assertEqual(quality["blocked_rule"], "QR-UPPER-1000")

        check_detail = quality["details"][0]
        self.assertEqual(check_detail["field"], "目标金额")
        # 质检指向本次实际使用的来源字段与口径
        self.assertEqual(check_detail["value_source"], "复核列")
        self.assertEqual(check_detail["value_caliber"], "复核口径")
        self.assertEqual(check_detail["checked_value"], 1100.0)
        self.assertIn("超过上限 1000", check_detail["range_result"]["message"])

    # ------------------------------------------------------------------
    # 3. 阻断结果写入质检台账，台账含实际值与未采用候选
    # ------------------------------------------------------------------
    def test_quality_ledger_records_adopted_and_rejected(self):
        result = self.transform_svc.apply_transform(self.rule["id"], self.source_data)
        ledger_id = result["quality_check"]["ledger_entry_id"]

        ledger_page = self.quality_svc.query_quality_ledger(blocked=True)
        self.assertEqual(ledger_page["total"], 1)

        entry = self.quality_svc.get_ledger_entry(ledger_id)
        self.assertIsNotNone(entry)
        self.assertTrue(entry["blocked"])
        self.assertEqual(entry["outcome"], "blocked")
        self.assertEqual(entry["blocked_field"], "目标金额")
        # 实际转换的数据值
        self.assertEqual(entry["actual_data"]["目标金额"], "1100")

        field_row = entry["field_value_details"][0]
        self.assertEqual(field_row["adopted_value"], "1100")
        self.assertEqual(field_row["adopted_source"], "复核列")
        # 未采用的候选值仍可在台账查到
        self.assertEqual(
            [(c["source_path"], c["converted_value"]) for c in field_row["rejected_candidates"]],
            [("账面列", "900")],
        )
        # 完整候选同样保留
        self.assertEqual(len(field_row["candidates"]), 2)
        self.assertTrue(any("超过上限 1000" in msg for msg in entry["failure_messages"]))

    # ------------------------------------------------------------------
    # 4. 同一映射版本并发执行：单飞合并，只产生一个实际业务值/一条台账
    # ------------------------------------------------------------------
    def test_concurrent_same_version_single_flight(self):
        outputs: list[dict] = []
        errors: list[BaseException] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            try:
                barrier.wait()  # 8 个线程同时进入，确保竞争同一个单飞槽位
                outputs.append(
                    self.transform_svc.apply_transform(self.rule["id"], dict(self.source_data))
                )
            except BaseException as exc:  # pragma: no cover - 防御
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(outputs), 8)
        # 每个并发调用拿到同一个 run_id 与同一个业务值
        run_ids = {o["run_id"] for o in outputs}
        self.assertEqual(len(run_ids), 1)
        self.assertTrue(all(o["data"]["目标金额"] == "1100" for o in outputs))
        # 仅一个领导调用真正执行，其余标记为并发合并
        self.assertEqual(sum(1 for o in outputs if o["deduped"]), 7)
        # 台账只登记一次
        ledger_page = self.quality_svc.query_quality_ledger()
        self.assertEqual(ledger_page["total"], 1)

    # ------------------------------------------------------------------
    # 5. 幂等键相同的并发调用也走单飞；不同输入的顺序调用各自独立执行
    # ------------------------------------------------------------------
    def test_idempotency_key_dedup_and_independent_runs(self):
        barrier = threading.Barrier(2)
        outputs: list[dict] = []

        def concurrent_worker() -> None:
            barrier.wait()
            outputs.append(self.transform_svc.apply_transform(
                self.rule["id"], self.source_data, idempotency_key="batch-001"
            ))

        threads = [threading.Thread(target=concurrent_worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len({o["run_id"] for o in outputs}), 1)
        self.assertEqual(sum(1 for o in outputs if o["deduped"]), 1)

        # 顺序执行的不同输入互不影响，各自产生独立业务值与台账记录
        another = self.transform_svc.apply_transform(
            self.rule["id"], {"账面列": "100", "复核列": "200"}
        )
        self.assertFalse(another["deduped"])
        self.assertEqual(another["data"]["目标金额"], "200")
        self.assertEqual(self.quality_svc.query_quality_ledger()["total"], 2)

    # ------------------------------------------------------------------
    # 6. priority 策略：采用首个可解析候选（账面列 900）→ 通过上限
    # ------------------------------------------------------------------
    def test_priority_strategy_passes_within_limit(self):
        self.store.update(TRANSFORM_KEY, self.rule["id"], {
            "字段映射": (
                '{"目标金额": {"sources": ['
                '{"source": "账面列", "caliber": "账面口径"}, '
                '{"source": "复核列", "caliber": "复核口径"}], '
                '"strategy": "priority"}}'
            ),
        })
        result = self.transform_svc.apply_transform(self.rule["id"], self.source_data)
        detail = result["field_value_details"][0]
        self.assertEqual(detail["adopted"], "900")
        self.assertEqual(detail["adopted_source"], "账面列")
        self.assertNotIn("blocked", result["quality_check"])
        self.assertTrue(result["quality_check"]["passed"])
        # 未采用候选变为复核列 1100
        self.assertEqual(
            [(c["source_path"], c["converted_value"]) for c in detail["candidates"] if not c["adopted"]],
            [("复核列", "1100")],
        )

    # ------------------------------------------------------------------
    # 7. 单来源传统写法保持向后兼容
    # ------------------------------------------------------------------
    def test_legacy_single_source_mapping_compatible(self):
        self.store.update(TRANSFORM_KEY, self.rule["id"], {
            "字段映射": '{"目标金额": "账面列"}',
        })
        result = self.transform_svc.apply_transform(self.rule["id"], self.source_data)
        detail = result["field_value_details"][0]
        self.assertEqual(len(detail["candidates"]), 1)
        self.assertEqual(detail["adopted"], "900")
        self.assertEqual(detail["adopted_source"], "账面列")
        self.assertEqual(detail["selection_strategy"], "priority")

    # ------------------------------------------------------------------
    # 8. 一个来源缺失、另一个可解析：仍能选出业务值
    # ------------------------------------------------------------------
    def test_unparseable_source_falls_back(self):
        result = self.transform_svc.apply_transform(
            self.rule["id"], {"账面列": None, "复核列": "1100"}
        )
        detail = result["field_value_details"][0]
        candidates = detail["candidates"]
        self.assertFalse(candidates[0]["parseable"])
        self.assertTrue(candidates[1]["parseable"])
        self.assertEqual(detail["adopted"], "1100")
        self.assertEqual(detail["adopted_source"], "复核列")


if __name__ == "__main__":
    unittest.main(verbosity=2)
