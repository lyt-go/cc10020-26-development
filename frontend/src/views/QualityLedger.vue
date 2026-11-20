<template>
  <section class="page" data-module="quality-ledger">
    <header class="page-head">
      <div>
        <h2>质检台账</h2>
        <p class="page-desc">
          跨模块读取数据转换的目标字段取值明细：可核对实际转换的数据值、采用来源与取值口径，
          以及未采用的候选值；阻断记录同样登记，可追溯质量影响。
        </p>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>阻断状态</span>
        <select v-model="blockedFilter" data-testid="filter-blocked">
          <option value="">全部</option>
          <option value="true">仅阻断</option>
          <option value="false">未阻断</option>
        </select>
      </label>
      <label class="filter-item">
        <span>目标字段</span>
        <input v-model="targetField" data-testid="filter-target-field" placeholder="如 目标金额" />
      </label>
      <button class="btn" data-testid="query-ledger" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置</button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table" data-testid="ledger-table">
      <thead>
        <tr>
          <th>台账编号</th>
          <th>结果</th>
          <th>阻断规则/字段</th>
          <th>实际转换值</th>
          <th>采用来源 / 口径</th>
          <th>未采用候选值</th>
          <th>失败原因</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="entry in entries" :key="entry.id" :data-testid="`ledger-row-${entry.id}`">
          <td>
            {{ entry.ledger_no }}
            <div class="muted">{{ formatTime(entry.timestamp) }}</div>
          </td>
          <td>
            <span :class="entry.blocked ? 'tag-reject' : entry.passed ? 'tag-adopt' : 'tag-warn'">
              {{ outcomeLabel(entry.outcome) }}
            </span>
            <div class="muted">评分 {{ entry.score }}</div>
          </td>
          <td>
            <template v-if="entry.blocked">
              {{ entry.blocked_rule }}
              <div class="muted">{{ entry.blocked_field }}</div>
            </template>
            <span v-else class="muted">—</span>
          </td>
          <td>
            <div v-for="(value, key) in entry.actual_data" :key="key">
              {{ key }} = {{ value }}
            </div>
          </td>
          <td>
            <div v-for="row in entry.field_value_details" :key="row.target_field">
              <strong>{{ row.target_field }}</strong>
              <div class="muted">{{ row.adopted_source }} / {{ row.adopted_caliber }} = {{ row.adopted_value }}</div>
            </div>
          </td>
          <td>
            <div v-for="row in entry.field_value_details" :key="`r-${row.target_field}`">
              <template v-if="row.rejected_candidates.length">
                <span v-for="c in row.rejected_candidates" :key="c.source_path" class="tag-reject">
                  {{ c.source_path }}（{{ c.caliber }}）= {{ c.converted_value }}
                </span>
              </template>
              <span v-else class="muted">无</span>
            </div>
          </td>
          <td>
            <div v-for="(msg, i) in entry.failure_messages" :key="i" class="muted">{{ msg }}</div>
            <span v-if="!entry.failure_messages.length" class="muted">—</span>
          </td>
        </tr>
        <tr v-if="!entries.length">
          <td colspan="7" class="empty-state">暂无质检台账记录，先到转换工作台执行一次转换。</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条台账记录</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchQualityLedger, type QualityLedgerEntry } from '@/api/backend-service'

const entries = ref<QualityLedgerEntry[]>([])
const total = ref(0)
const blockedFilter = ref('')
const targetField = ref('')
const errorMessage = ref('')

function outcomeLabel(outcome: string): string {
  if (outcome === 'blocked') return '阻断'
  if (outcome === 'warning') return '警告'
  return '通过'
}

function formatTime(iso: string): string {
  return iso ? iso.replace('T', ' ').slice(0, 19) : ''
}

function resetFilters() {
  blockedFilter.value = ''
  targetField.value = ''
  void reload()
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await fetchQualityLedger({
      blocked: blockedFilter.value === '' ? null : blockedFilter.value === 'true',
      target_field: targetField.value.trim() || undefined,
      size: 50,
    })
    entries.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '质检台账读取失败'
  }
}

onMounted(reload)
</script>
