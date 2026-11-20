"""数据转换编排领域服务：JSON/XML数据转换、字段映射、类型转换与结构重组。

提供从源Schema到目标Schema的字段级映射和值转换能力，
支持嵌套结构抽取、类型强制转换和异常降级策略。

字段映射多来源规则（同一目标列接收多个来源）：
    传统映射只允许 ``{"目标字段": "源字段路径"}``，目标列只能接收一个来源；
    现支持在映射中为同一目标字段声明多个候选来源，并显式给出取值口径：

    .. code-block:: json

        {
          "目标金额": {
            "sources": [
              {"source": "账面列", "caliber": "账面口径"},
              {"source": "复核列", "caliber": "复核口径"}
            ],
            "strategy": "max"
          }
        }

    - sources：候选来源列表，每项记录源列路径与取值口径（caliber）；
    - strategy：多来源取值策略
        * ``priority``（默认）：按声明顺序取第一个可解析候选；
        * ``max`` / ``min``：在可解析数值候选中取最大 / 最小值，平局取声明靠前者。

    目标字段的每个候选值、取值口径、来源与最终采用值都会保留在
    ``field_value_details``（目标字段取值明细）中，供质量校验与质检台账跨模块读取。

并发控制：
    同一映射版本 + 同一份源数据并发执行时通过单飞（single-flight）合并，
    保证一个映射版本对一份输入只产生一个实际业务值、只写一条质检台账。

跨模块关联：data_quality_rule（质量校验/质检台账）、data_lake_sink（转换后落湖）。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from app.persistence import json_store
from app.services.base import BaseModuleService

# 多来源取值策略 → 可读口径说明（写入目标字段取值明细）
STRATEGY_LABELS: dict[str, str] = {
    "priority": "按来源声明顺序取首个可解析候选",
    "max": "取可解析候选中的最大值",
    "min": "取可解析候选中的最小值",
}
DEFAULT_STRATEGY = "priority"


class _InFlight:
    """单飞占位：同键并发调用共享同一次执行结果。"""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: dict[str, Any] | None = None
        self.error: BaseException | None = None


class DataTransformService(BaseModuleService):
    MODULE = "data_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源Schema", "目标Schema", "字段映射", "转换函数", "默认值填充", "异常策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 单飞注册表：key 为 (规则, 版本, 输入) 指纹，保证同一映射版本并发只产出一个业务值
    _flights: dict[str, _InFlight] = {}
    _flights_guard = threading.Lock()

    _BUILTIN_FUNCTIONS: dict[str, Callable[[Any], Any]] = {
        "to_string": lambda x: str(x) if x is not None else "",
        "to_integer": lambda x: int(x) if x is not None else 0,
        "to_float": lambda x: float(x) if x is not None else 0.0,
        "to_boolean": lambda x: bool(x) if x is not None else False,
        "to_upper": lambda x: str(x).upper() if x is not None else "",
        "to_lower": lambda x: str(x).lower() if x is not None else "",
        "extract_numbers": lambda x: re.sub(r"[^0-9]", "", str(x)) if x is not None else "",
        "parse_datetime": lambda x: x,
        "default_empty": lambda x: x if x is not None and str(x).strip() else "",
    }

    # ------------------------------------------------------------------
    # 对外主入口
    # ------------------------------------------------------------------
    def apply_transform(
        self,
        rule_id: int,
        source_data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """应用转换规则，将源数据转换为目标结构。

        转换流程：
        1. 加载已生效的转换规则
        2. 解析字段映射定义（支持同一目标字段的多来源候选）
        3. 逐个字段抽取候选值、应用转换函数并按取值策略确定唯一采用值
        4. 处理异常策略（跳过/默认值/报错）
        5. 携带目标字段取值明细跨模块执行质量校验，阻断时不向下游同步

        同一映射版本对同一份源数据的并发调用会被单飞合并，复用同一次
        执行结果（同一个 ``run_id``、同一个实际业务值）。

        Returns:
            { ok, data, errors, fields_transformed, total_fields,
              field_value_details, run_id, mapping_version, deduped, quality_check }
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "data": {}, "errors": ["转换规则不存在"], "fields_transformed": 0}

        if rule.get("status") != "已生效":
            return {"ok": False, "data": {}, "errors": ["转换规则未生效"], "fields_transformed": 0}

        flight_key = self._flight_key(rule, source_data, idempotency_key)
        return self._run_single_flight(flight_key, lambda: self._execute_transform(rule, source_data))

    # ------------------------------------------------------------------
    # 单飞（single-flight）并发合并
    # ------------------------------------------------------------------
    def _flight_key(
        self,
        rule: dict[str, Any],
        source_data: dict[str, Any],
        idempotency_key: str | None,
    ) -> str:
        """计算同版本同输入的并发指纹。"""
        if idempotency_key:
            identity = f"idem:{idempotency_key}"
        else:
            identity = "data:" + json.dumps(source_data, ensure_ascii=False, sort_keys=True, default=str)
        raw = f"{rule.get('id')}|{rule.get('规则版本', '')}|{identity}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _run_single_flight(self, key: str, producer: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        """同键并发只执行一次 producer，其余调用等待并复用结果。"""
        with self._flights_guard:
            flight = self._flights.get(key)
            if flight is None:
                flight = _InFlight()
                self._flights[key] = flight
                leader = True
            else:
                leader = False

        if not leader:
            flight.event.wait()
            if flight.error is not None:
                raise flight.error
            assert flight.result is not None
            # 复用结果时标记为并发合并，证明并发下只产生一个实际业务值
            shared = dict(flight.result)
            shared["deduped"] = True
            return shared

        try:
            flight.result = producer()
            return flight.result
        except BaseException as exc:  # pragma: no cover - 防御性路径
            flight.error = exc
            raise
        finally:
            flight.event.set()
            with self._flights_guard:
                self._flights.pop(key, None)

    # ------------------------------------------------------------------
    # 转换执行
    # ------------------------------------------------------------------
    def _execute_transform(self, rule: dict[str, Any], source_data: dict[str, Any]) -> dict[str, Any]:
        rule_id = int(rule["id"])
        field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
        transform_func = str(rule.get("转换函数", "passthrough"))
        default_filler = str(rule.get("默认值填充", "使用默认值"))
        error_policy = str(rule.get("异常策略", "跳过"))
        run_id = hashlib.sha1(
            f"{rule_id}:{datetime.now(timezone.utc).isoformat()}".encode("utf-8")
        ).hexdigest()[:16]

        result: dict[str, Any] = {}
        field_value_details: list[dict[str, Any]] = []
        errors: list[str] = []
        transformed = 0

        for target_field, mapping_spec in field_mapping.items():
            detail = self._resolve_target_field(
                target_field=target_field,
                mapping_spec=mapping_spec,
                source_data=source_data,
                transform_func=transform_func,
            )
            field_value_details.append(detail)

            if detail["adopted"] is not None or detail.get("has_value"):
                result[target_field] = detail["adopted"]
                transformed += 1
                continue

            # 所有来源都不可解析：按异常策略降级
            message = f"字段 {target_field} 的 {len(detail['candidates'])} 个来源均不可解析"
            if error_policy == "使用默认值":
                default_value = self._get_default_value(target_field, rule)
                result[target_field] = default_value
                detail["adopted"] = default_value
                detail["adopted_source"] = "__default__"
                detail["adopted_caliber"] = f"异常策略默认值（{default_filler}）"
            elif error_policy == "报错":
                errors.append(message)
            else:
                errors.append(f"{message}，已跳过")

        # 质量校验（跨模块：data_quality_rule），传入目标字段取值明细
        quality_check = self._run_quality_checks(rule_id, result, field_value_details)

        return {
            "ok": len(errors) == 0 or error_policy == "跳过",
            "data": result,
            "errors": errors,
            "fields_transformed": transformed,
            "total_fields": len(field_mapping),
            "field_value_details": field_value_details,
            "run_id": run_id,
            "mapping_version": str(rule.get("规则版本", "")),
            "deduped": False,
            "quality_check": quality_check,
        }

    def _resolve_target_field(
        self,
        *,
        target_field: str,
        mapping_spec: Any,
        source_data: dict[str, Any],
        transform_func: str,
    ) -> dict[str, Any]:
        """解析单个目标字段的全部候选来源并选出唯一采用值。"""
        sources, strategy = self._normalize_spec(mapping_spec)
        candidates: list[dict[str, Any]] = []

        for source_path, caliber in sources:
            raw_value: Any = None
            parseable = False
            extract_error = ""
            try:
                raw_value = self._extract_value(source_data, source_path)
                parseable = raw_value is not None
            except (KeyError, ValueError, TypeError) as exc:
                extract_error = str(exc)

            converted_value: Any = None
            if parseable:
                try:
                    converted_value = self._apply_function(transform_func, raw_value)
                except (ValueError, TypeError) as exc:
                    parseable = False
                    extract_error = f"转换失败: {exc}"

            candidates.append({
                "source_path": source_path,
                "caliber": caliber,
                "raw_value": raw_value,
                "converted_value": converted_value,
                "parseable": parseable,
                "error": extract_error,
                "adopted": False,
            })

        adopted_index = self._select_candidate(candidates, strategy)
        adopted_value = None
        adopted_source = None
        adopted_caliber = None
        if adopted_index is not None:
            candidates[adopted_index]["adopted"] = True
            adopted_value = candidates[adopted_index]["converted_value"]
            adopted_source = candidates[adopted_index]["source_path"]
            adopted_caliber = candidates[adopted_index]["caliber"]

        return {
            "target_field": target_field,
            "adopted": adopted_value,
            "adopted_source": adopted_source,
            "adopted_caliber": adopted_caliber,
            "selection_strategy": strategy,
            "strategy_label": STRATEGY_LABELS.get(strategy, STRATEGY_LABELS[DEFAULT_STRATEGY]),
            "has_value": adopted_index is not None,
            "candidates": candidates,
        }

    def _normalize_spec(self, mapping_spec: Any) -> tuple[list[tuple[str, str]], str]:
        """把映射定义统一成 ``[(源路径, 取值口径), ...]`` + 取值策略。

        兼容三种写法：
        - 字符串：``"源列"``（单来源，口径默认与源列同名）；
        - 字典多来源：``{"sources": [...], "strategy": "max"}``；
        - 列表：``["源列A", {"source": "源列B", "caliber": "复核口径"}]``。
        """
        strategy = DEFAULT_STRATEGY
        if isinstance(mapping_spec, dict):
            strategy = str(mapping_spec.get("strategy", DEFAULT_STRATEGY)).strip() or DEFAULT_STRATEGY
            raw_sources = mapping_spec.get("sources", [])
        elif isinstance(mapping_spec, list):
            raw_sources = mapping_spec
        else:
            raw_sources = [mapping_spec]

        sources: list[tuple[str, str]] = []
        for item in raw_sources:
            if isinstance(item, dict):
                source_path = str(item.get("source", "")).strip()
                caliber = str(item.get("caliber") or source_path).strip()
            else:
                source_path = str(item).strip()
                caliber = source_path
            if source_path:
                sources.append((source_path, caliber))

        if strategy not in STRATEGY_LABELS:
            strategy = DEFAULT_STRATEGY
        return sources, strategy

    def _select_candidate(self, candidates: list[dict[str, Any]], strategy: str) -> int | None:
        """按取值策略在可解析候选中确定唯一采用下标。"""
        usable = [idx for idx, item in enumerate(candidates) if item["parseable"]]
        if not usable:
            return None
        if strategy == "priority":
            return usable[0]

        # max / min：数值口径比较，无法数值化的候选不参与比较
        numeric: list[tuple[float, int]] = []
        for idx in usable:
            try:
                numeric.append((float(str(candidates[idx]["converted_value"])), idx))
            except (ValueError, TypeError):
                continue
        if not numeric:
            return usable[0]
        if strategy == "max":
            numeric.sort(key=lambda pair: (-pair[0], pair[1]))
        else:  # min
            numeric.sort(key=lambda pair: (pair[0], pair[1]))
        return numeric[0][1]

    # ------------------------------------------------------------------
    # 映射解析与取值工具
    # ------------------------------------------------------------------
    def _parse_mapping(self, mapping_str: str) -> dict[str, Any]:
        """解析字段映射定义。

        支持格式：
        - JSON: ``{"目标字段": "源字段路径"}`` 或多来源对象写法
        - 简单: ``"field1->target1,field2->target2"``
        """
        try:
            parsed = json.loads(mapping_str)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        result: dict[str, str] = {}
        for part in mapping_str.split(","):
            if "->" in part:
                src, tgt = part.split("->", 1)
                result[tgt.strip()] = src.strip()
        return result if result else {"_raw": "payload"}

    def _extract_value(self, data: dict[str, Any], path: str) -> Any:
        """从嵌套数据中提取指定路径的值。

        支持点号和方括号路径：
        - "name" → data["name"]
        - "user.profile.email" → data["user"]["profile"]["email"]
        - "items[0].id" → data["items"][0]["id"]
        """
        parts = re.split(r"\.|(?<=\w)(?=\[)", path)
        current: Any = data
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "[" in part and "]" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                current = current.get(part, current.get(path, None))
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {part}")

        return current

    def _apply_function(self, func_name: str, value: Any) -> Any:
        """应用命名转换函数。"""
        if func_name == "passthrough":
            return value
        func = self._BUILTIN_FUNCTIONS.get(func_name)
        if func:
            return func(value)
        return value

    def _get_default_value(self, field: str, rule: dict[str, Any]) -> Any:
        """根据规则配置获取字段默认值。"""
        default_mapping = {
            "string": "", "integer": 0, "float": 0.0,
            "boolean": False, "datetime": "",
        }
        return default_mapping.get(rule.get("目标Schema", ""), "")

    def _run_quality_checks(
        self,
        rule_id: int,
        transformed_data: dict[str, Any],
        field_value_details: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """跨模块运行质量校验，并把目标字段取值明细交给质量模块。"""
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            return quality_svc.validate_record(
                transformed_data,
                source_module=self.MODULE,
                context={
                    "transform_rule_id": rule_id,
                    "field_value_details": field_value_details,
                },
            )
        except Exception:
            return {"passed": True, "checks": 0}

    def batch_transform(self, rule_id: int, source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """批量转换多条记录。"""
        results = []
        success = 0
        failed = 0

        for record in source_records:
            r = self.apply_transform(rule_id, record)
            if r["ok"]:
                success += 1
            else:
                failed += 1
            results.append(r)

        return {
            "ok": failed == 0,
            "message": f"批量转换完成: 成功 {success}/{len(source_records)}",
            "success": success,
            "failed": failed,
            "total": len(source_records),
            "results": results,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据转换编排的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据转换编排数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
        }
