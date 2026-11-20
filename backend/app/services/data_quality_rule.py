"""接入质量校验领域服务：字段级校验、完整性检查、阈值告警与阻断策略。

提供转换后数据的质量校验能力，包括必填字段检查、类型校验、
数值范围校验和完整性评分。

跨模块取值明细：
    data_transform 在调用 :meth:`validate_record` 时通过 ``context`` 传入
    「目标字段取值明细」（field_value_details）。本模块跨模块读取该明细，
    使每条校验都指向本次实际采用的来源字段，并按“采用的金额”执行上限检查；
    明细中的候选值、取值口径、来源以及未采用候选会一并写入质检台账。

阻断与台账：
    阻断策略为「阻断」且校验不通过时，结果不会同步到下游（返回 ``blocked=True``），
    但阻断事实及完整取值明细仍会登记到质检台账（json_store 持久化分区
    ``quality_ledger``），台账中可查到实际转换的数据值与未采用的候选值。

跨模块关联：data_transform（转换后校验）、batch_ingest（批量质量检查）。
"""
from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 质检台账在 json_store 中的持久化分区键
LEDGER_KEY = "quality_ledger"


class DataQualityRuleService(BaseModuleService):
    MODULE = "data_quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "数据来源", "校验字段", "校验表达式", "阈值下界", "阈值上界", "阻断策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "数据来源", "校验字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self) -> None:
        super().__init__()
        self._validation_history: list[dict[str, Any]] = []
        self._history_lock = threading.Lock()

    def validate_record(
        self,
        record: dict[str, Any],
        source_module: str = "",
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """对单条记录执行质量校验。

        校验流程：
        1. 加载与该来源相关的已生效规则
        2. 逐条规则执行校验表达式
        3. 检查数值范围（阈值上下界，按实际采用值执行）
        4. 根据阻断策略决定是否拒绝，并登记质检台账

        Args:
            record: 转换后的目标记录（实际业务值）。
            source_module: 触发校验的来源模块（如 data_transform）。
            context: 跨模块上下文，可包含
                ``transform_rule_id`` 与 ``field_value_details``
                （目标字段取值明细：候选值、取值口径、来源、采用值）。

        Returns:
            { passed, checks, failed, details, score, blocked?, ledger_entry? }
        """
        context = context or {}
        field_details = {
            item.get("target_field"): item
            for item in context.get("field_value_details", [])
            if isinstance(item, dict)
        }

        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and (str(r.get("数据来源", "")) in (source_module, "", "ALL", "*")
                 or source_module == ""
                 or not source_module)
        ]

        if not matching:
            return {"passed": True, "checks": 0, "failed": 0, "details": [], "score": 100}

        results = []
        total_passed = 0
        total_failed = 0
        blocking_failure: dict[str, Any] | None = None

        for rule in matching:
            field = str(rule.get("校验字段", ""))
            expression = str(rule.get("校验表达式", ""))
            block_policy = str(rule.get("阻断策略", "警告"))
            try:
                lower = float(rule.get("阈值下界", "0") or "0")
                upper = float(rule.get("阈值上界", "0") or "0")
            except ValueError:
                lower = 0
                upper = 0

            check_result = self._execute_check(record, field, expression)
            range_check = self._check_range(record, field, lower, upper, field_details)

            passed = check_result.get("passed", True) and range_check.get("passed", True)
            detail = {
                "rule_no": rule.get("规则编号"),
                "field": field,
                "expression": expression,
                "passed": passed,
                "expr_result": check_result,
                "range_result": range_check,
                "block_policy": block_policy,
                # 指向本次实际使用的来源字段与取值口径
                "value_source": range_check.get("value_source"),
                "value_caliber": range_check.get("value_caliber"),
                "checked_value": range_check.get("checked_value", record.get(field)),
            }
            results.append(detail)

            if passed:
                total_passed += 1
            else:
                total_failed += 1
                if block_policy == "阻断" and blocking_failure is None:
                    blocking_failure = {
                        "rule": rule,
                        "detail": detail,
                        "total_failed": total_failed,
                        "total_passed": total_passed,
                        "results": list(results),
                        "matching_count": len(matching),
                    }

        score = max(0, 100 - total_failed * 10)
        ledger_entry = self._record_ledger(
            record=record,
            source_module=source_module,
            context=context,
            check_details=results,
            total_passed=total_passed,
            total_failed=total_failed,
            blocking_failure=blocking_failure,
            score=score,
        )

        with self._history_lock:
            self._validation_history.append({
                "record_preview": str(record)[:100],
                "checks": len(matching),
                "passed": total_passed,
                "failed": total_failed,
                "blocked": blocking_failure is not None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        if blocking_failure is not None:
            return {
                "passed": False,
                "checks": blocking_failure["matching_count"],
                "failed": total_failed,
                "passed_checks": total_passed,
                "details": results,
                "score": score,
                "blocked": True,
                "blocked_rule": blocking_failure["rule"].get("规则编号"),
                "ledger_entry_id": ledger_entry["id"],
                "ledger_entry": ledger_entry,
            }

        return {
            "passed": total_failed == 0,
            "checks": len(matching),
            "failed": total_failed,
            "passed_checks": total_passed,
            "details": results,
            "score": score,
            "ledger_entry_id": ledger_entry["id"],
            "ledger_entry": ledger_entry,
        }

    # ------------------------------------------------------------------
    # 质检台账
    # ------------------------------------------------------------------
    def _record_ledger(
        self,
        *,
        record: dict[str, Any],
        source_module: str,
        context: dict[str, Any],
        check_details: list[dict[str, Any]],
        total_passed: int,
        total_failed: int,
        blocking_failure: dict[str, Any] | None,
        score: int,
    ) -> dict[str, Any]:
        """把一次校验（含阻断）登记到质检台账，保留采用值与全部候选值。"""
        field_value_details = context.get("field_value_details", [])
        # 台账行：实际转换的数据值 + 候选值/取值口径/来源 + 未采用候选
        field_rows = []
        for item in field_value_details:
            field_rows.append({
                "target_field": item.get("target_field"),
                "adopted_value": item.get("adopted"),
                "adopted_source": item.get("adopted_source"),
                "adopted_caliber": item.get("adopted_caliber"),
                "selection_strategy": item.get("selection_strategy"),
                "strategy_label": item.get("strategy_label"),
                "candidates": [
                    {
                        "source_path": c.get("source_path"),
                        "caliber": c.get("caliber"),
                        "raw_value": c.get("raw_value"),
                        "converted_value": c.get("converted_value"),
                        "parseable": c.get("parseable"),
                        "adopted": c.get("adopted", False),
                    }
                    for c in item.get("candidates", [])
                ],
                "rejected_candidates": [
                    {
                        "source_path": c.get("source_path"),
                        "caliber": c.get("caliber"),
                        "converted_value": c.get("converted_value"),
                    }
                    for c in item.get("candidates", [])
                    if not c.get("adopted", False)
                ],
            })

        failed_details = [d for d in check_details if not d.get("passed")]
        entry = {
            "ledger_no": f"QL-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_module": source_module,
            "transform_rule_id": context.get("transform_rule_id"),
            "outcome": "blocked" if blocking_failure is not None else (
                "warning" if total_failed else "passed"
            ),
            "blocked": blocking_failure is not None,
            "passed": total_failed == 0,
            "checks_passed": total_passed,
            "checks_failed": total_failed,
            "score": score,
            "blocked_rule": blocking_failure["rule"].get("规则编号") if blocking_failure else None,
            "blocked_field": blocking_failure["detail"].get("field") if blocking_failure else None,
            "actual_data": record,
            "field_value_details": field_rows,
            "check_details": check_details,
            "failure_messages": [
                self._summarize_failure(detail) for detail in failed_details
            ],
        }
        return json_store.insert(LEDGER_KEY, entry)

    @staticmethod
    def _summarize_failure(detail: dict[str, Any]) -> str:
        """汇总一条失败校验的可读原因。"""
        messages = []
        range_result = detail.get("range_result") or {}
        expr_result = detail.get("expr_result") or {}
        if not range_result.get("passed", True):
            messages.append(range_result.get("message", "范围校验失败"))
        if not expr_result.get("passed", True):
            messages.append(expr_result.get("message", "表达式校验失败"))
        return f"规则 {detail.get('rule_no')} 字段 {detail.get('field')}: {'；'.join(messages)}"

    def query_quality_ledger(
        self,
        *,
        blocked: bool | None = None,
        target_field: str | None = None,
        source_module: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        """查询质检台账，支持按阻断状态、目标字段、来源模块过滤。"""
        rows = json_store.get_all(LEDGER_KEY)
        if blocked is not None:
            rows = [r for r in rows if bool(r.get("blocked")) == blocked]
        if source_module:
            rows = [r for r in rows if str(r.get("source_module", "")) == source_module]
        if target_field:
            rows = [
                r for r in rows
                if any(
                    str(f.get("target_field", "")) == target_field
                    for f in r.get("field_value_details", [])
                )
            ]
        rows = sorted(rows, key=lambda r: int(r.get("id", 0)), reverse=True)
        total = len(rows)
        start = (max(page, 1) - 1) * size
        return {
            "items": rows[start:start + size],
            "total": total,
            "page": page,
            "size": size,
        }

    def get_ledger_entry(self, entry_id: int) -> dict[str, Any] | None:
        """按台账记录ID读取完整台账（含采用值与未采用候选值）。"""
        return json_store.get_by_id(LEDGER_KEY, entry_id)

    # ------------------------------------------------------------------
    # 校验执行
    # ------------------------------------------------------------------
    def _execute_check(self, record: dict[str, Any], field: str, expression: str) -> dict[str, Any]:
        """执行校验表达式。

        支持的表达式操作：
        - "not_empty": 非空检查
        - "is_numeric": 是否为数字
        - "pattern:xxx": 正则匹配
        - "in:val1,val2": 值域检查
        - "len_gt:N": 长度大于N
        """
        value = record.get(field)
        field_present = field in record

        if expression == "not_empty":
            if not field_present or value is None or str(value).strip() == "":
                return {"passed": False, "message": f"字段 {field} 为空"}
            return {"passed": True, "message": "通过"}

        if expression == "is_numeric":
            try:
                float(str(value))
                return {"passed": True, "message": "通过"}
            except (ValueError, TypeError):
                return {"passed": False, "message": f"字段 {field} 不是数字"}

        if expression.startswith("pattern:"):
            pattern = expression.split(":", 1)[1]
            if not re.match(pattern, str(value)):
                return {"passed": False, "message": f"字段 {field} 不匹配模式 {pattern}"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("in:"):
            allowed = expression.split(":", 1)[1].split(",")
            if str(value) not in allowed:
                return {"passed": False, "message": f"字段 {field} 不在允许值 {allowed} 中"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("len_gt:"):
            min_len = int(expression.split(":", 1)[1])
            if len(str(value or "")) < min_len:
                return {"passed": False, "message": f"字段 {field} 长度小于 {min_len}"}
            return {"passed": True, "message": "通过"}

        return {"passed": True, "message": "未识别表达式，默认通过"}

    def _check_range(
        self,
        record: dict[str, Any],
        field: str,
        lower: float,
        upper: float,
        field_details: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """检查字段值是否在阈值范围内。

        跨模块读取目标字段取值明细时，范围检查按本次实际采用值执行，
        并回传该值的来源字段与取值口径，使质检能指向本次使用的字段。
        """
        field_details = field_details or {}
        if lower == 0 and upper == 0:
            return {
                "passed": True,
                "message": "无范围限制",
                "checked_value": record.get(field),
                "value_source": field_details.get(field, {}).get("adopted_source"),
                "value_caliber": field_details.get(field, {}).get("adopted_caliber"),
            }

        value = record.get(field)
        detail = field_details.get(field) or {}
        try:
            num_value = float(str(value))
        except (ValueError, TypeError):
            return {
                "passed": True,
                "message": "非数值字段跳过范围检查",
                "checked_value": value,
                "value_source": detail.get("adopted_source"),
                "value_caliber": detail.get("adopted_caliber"),
            }

        source_hint = detail.get("adopted_source")
        caliber_hint = detail.get("adopted_caliber")
        hint = f"（采用来源 {source_hint} / {caliber_hint}）" if source_hint else ""

        if lower > 0 and num_value < lower:
            return {
                "passed": False,
                "message": f"采用值 {num_value} 低于阈值 {lower}{hint}",
                "checked_value": num_value,
                "value_source": source_hint,
                "value_caliber": caliber_hint,
            }
        if upper > 0 and num_value > upper:
            return {
                "passed": False,
                "message": f"采用值 {num_value} 超过上限 {upper}{hint}",
                "checked_value": num_value,
                "value_source": source_hint,
                "value_caliber": caliber_hint,
            }

        return {
            "passed": True,
            "message": f"采用值 {num_value} 在范围 [{lower}, {upper}] 内{hint}",
            "checked_value": num_value,
            "value_source": source_hint,
            "value_caliber": caliber_hint,
        }

    def validate_batch(self, records: list[dict[str, Any]], source_module: str = "") -> dict[str, Any]:
        """批量质量校验。"""
        results = []
        passed_count = 0
        failed_count = 0
        blocked_count = 0

        for record in records:
            r = self.validate_record(record, source_module)
            if r.get("blocked"):
                blocked_count += 1
                failed_count += 1
            elif r["passed"]:
                passed_count += 1
            else:
                failed_count += 1
            results.append(r)

        avg_score = sum(r.get("score", 0) for r in results) / max(len(results), 1)

        return {
            "ok": blocked_count == 0,
            "total": len(records),
            "passed": passed_count,
            "failed": failed_count,
            "blocked": blocked_count,
            "average_score": round(avg_score, 2),
            "results": results,
        }

    def get_quality_report(self) -> dict[str, Any]:
        """获取质量校验报告。"""
        with self._history_lock:
            history = list(self._validation_history[-100:])
        rules = json_store.get_all(self.MODULE)
        active_rules = sum(1 for r in rules if r.get("status") == "已生效")
        ledger_rows = json_store.get_all(LEDGER_KEY)

        return {
            "active_rules": active_rules,
            "total_checks": len(history),
            "recent_failures": sum(1 for h in history if h.get("failed", 0) > 0),
            "recent_blocks": sum(1 for h in history if h.get("blocked")),
            "ledger_total": len(ledger_rows),
            "ledger_blocked": sum(1 for r in ledger_rows if r.get("blocked")),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出接入质量校验的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "接入质量校验数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_validations": len(self._validation_history),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "ledger_total": len(json_store.get_all(LEDGER_KEY)),
            "ledger_blocked": sum(
                1 for r in json_store.get_all(LEDGER_KEY) if r.get("blocked")
            ),
            "operation": self.OPERATION_TERM,
        }
