import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ===== 多来源字段映射与质检台账 =====

export type ValueCandidate = {
  source_path: string
  caliber: string
  raw_value: string | number | null
  converted_value: string | number | null
  parseable: boolean
  adopted: boolean
  error?: string
}

export type FieldValueDetail = {
  target_field: string
  adopted: string | number | null
  adopted_source: string | null
  adopted_caliber: string | null
  selection_strategy: string
  strategy_label: string
  has_value: boolean
  candidates: ValueCandidate[]
}

export type RangeResult = {
  passed: boolean
  message: string
  checked_value?: number | string | null
  value_source?: string | null
  value_caliber?: string | null
}

export type QualityCheckDetail = {
  rule_no: string
  field: string
  expression: string
  passed: boolean
  block_policy: string
  checked_value?: number | string | null
  value_source?: string | null
  value_caliber?: string | null
  range_result: RangeResult
  expr_result: { passed: boolean; message: string }
}

export type TransformRunResult = {
  ok: boolean
  data: Record<string, string | number | null>
  errors: string[]
  fields_transformed: number
  total_fields: number
  run_id: string
  mapping_version: string
  deduped: boolean
  field_value_details: FieldValueDetail[]
  quality_check: {
    passed: boolean
    blocked?: boolean
    blocked_rule?: string
    score: number
    ledger_entry_id?: number
    details: QualityCheckDetail[]
  }
}

export type LedgerFieldRow = {
  target_field: string
  adopted_value: string | number | null
  adopted_source: string | null
  adopted_caliber: string | null
  selection_strategy: string
  strategy_label: string
  candidates: ValueCandidate[]
  rejected_candidates: {
    source_path: string
    caliber: string
    converted_value: string | number | null
  }[]
}

export type QualityLedgerEntry = {
  id: number
  ledger_no: string
  timestamp: string
  source_module: string
  transform_rule_id: number | null
  outcome: string
  blocked: boolean
  passed: boolean
  score: number
  blocked_rule: string | null
  blocked_field: string | null
  actual_data: Record<string, string | number | null>
  field_value_details: LedgerFieldRow[]
  check_details: QualityCheckDetail[]
  failure_messages: string[]
}

export async function runTransform(
  ruleId: number,
  sourceData: Record<string, unknown>,
  idempotencyKey?: string,
): Promise<TransformRunResult> {
  const response = await request('/api/pipeline/transform/run', {
    method: 'POST',
    body: JSON.stringify({
      rule_id: ruleId,
      source_data: sourceData,
      idempotency_key: idempotencyKey ?? null,
    }),
  })
  if (!response.ok) throw new Error('转换执行失败')
  return await response.json()
}

export type LedgerQuery = {
  blocked?: boolean | null
  target_field?: string
  source_module?: string
  page?: number
  size?: number
}

export async function fetchQualityLedger(query: LedgerQuery = {}): Promise<{
  items: QualityLedgerEntry[]
  total: number
  page: number
  size: number
}> {
  const params = new URLSearchParams()
  if (query.blocked === true) params.set('blocked', 'true')
  if (query.blocked === false) params.set('blocked', 'false')
  if (query.target_field) params.set('target_field', query.target_field)
  if (query.source_module) params.set('source_module', query.source_module)
  params.set('page', String(query.page ?? 1))
  params.set('size', String(query.size ?? 20))
  const response = await request(`/api/pipeline/quality-ledger?${params.toString()}`)
  if (!response.ok) throw new Error('质检台账读取失败')
  return await response.json()
}

export async function fetchQualityLedgerEntry(id: number): Promise<QualityLedgerEntry> {
  const response = await request(`/api/pipeline/quality-ledger/${id}`)
  if (!response.ok) throw new Error('质检台账明细读取失败')
  return await response.json()
}
