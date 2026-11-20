<template>
  <section class="page" data-module="transform-workbench">
    <header class="page-head">
      <div>
        <h2>多来源字段映射转换工作台</h2>
        <p class="page-desc">
          同一目标字段接收多个来源列，保留每个候选值、取值口径与来源，按取值策略确定唯一业务值，
          并由质量校验按采用金额执行上限检查。
        </p>
      </div>
    </header>

    <div class="wb-grid">
      <article class="wb-card">
        <h3>① 转换规则与源数据</h3>
        <label class="wb-field">
          <span>转换规则 ID（data_transform 已生效规则）</span>
          <input v-model.number="ruleId" data-testid="rule-id" type="number" min="1" />
        </label>
        <label class="wb-field">
          <span>幂等键（可选，相同键并发只产生一个业务值）</span>
          <input v-model="idempotencyKey" data-testid="idempotency-key" placeholder="留空则按源数据指纹单飞" />
        </label>
        <label class="wb-field">
          <span>多来源映射示例（实际口径以规则 ID 对应的已生效规则为准）</span>
          <textarea v-model="mappingText" class="wb-code" rows="9" data-testid="mapping-text" readonly></textarea>
        </label>
        <label class="wb-field">
          <span>源数据（账面列 / 复核列）</span>
          <textarea v-model="sourceText" class="wb-code" rows="6" data-testid="source-text"></textarea>
        </label>
        <div class="page-actions">
          <button class="btn primary" data-testid="run-transform" type="button" :disabled="loading" @click="execute">
            {{ loading ? '转换中…' : '执行转换并质检' }}
          </button>
          <button class="btn ghost" type="button" @click="loadDemo">载入 900/1100 示例</button>
        </div>
        <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>
      </article>

      <article class="wb-card">
        <h3>② 转换输出与质量结论</h3>
        <template v-if="result">
          <div class="result-banner" :class="result.quality_check.blocked ? 'banner-block' : 'banner-pass'">
            <strong>
              {{ result.quality_check.blocked ? '已阻断，结果不同步下游' : '质量校验通过' }}
            </strong>
            <span v-if="result.deduped" class="dedup-tag">并发单飞合并 · 复用同一 run_id</span>
          </div>
          <dl class="dag-detail">
            <dt>run_id</dt><dd>{{ result.run_id }}</dd>
            <dt>映射版本</dt><dd>{{ result.mapping_version }}</dd>
            <dt>实际业务值</dt><dd>{{ formatJson(result.data) }}</dd>
            <dt>质量评分</dt><dd>{{ result.quality_check.score }}</dd>
          </dl>

          <h4>目标字段取值明细（候选值 / 取值口径 / 来源）</h4>
          <table class="data-table" data-testid="value-detail-table">
            <thead>
              <tr>
                <th>目标字段</th>
                <th>来源列</th>
                <th>取值口径</th>
                <th>候选值</th>
                <th>可解析</th>
                <th>是否采用</th>
              </tr>
            </thead>
            <tbody>
              <template v-for="detail in result.field_value_details" :key="detail.target_field">
                <tr v-for="(candidate, idx) in detail.candidates" :key="detail.target_field + idx">
                  <td v-if="idx === 0" :rowspan="detail.candidates.length">
                    {{ detail.target_field }}
                    <div class="muted">{{ detail.strategy_label }}</div>
                    <div class="muted">采用：{{ detail.adopted_source }} / {{ detail.adopted_caliber }} = {{ detail.adopted }}</div>
                  </td>
                  <td>{{ candidate.source_path }}</td>
                  <td>{{ candidate.caliber }}</td>
                  <td>{{ candidate.converted_value }}</td>
                  <td>{{ candidate.parseable ? '是' : '否' }}</td>
                  <td>
                    <span :class="candidate.adopted ? 'tag-adopt' : 'tag-reject'">
                      {{ candidate.adopted ? '采用' : '未采用候选' }}
                    </span>
                  </td>
                </tr>
              </template>
            </tbody>
          </table>

          <h4>质量校验（按采用金额执行上限检查）</h4>
          <table class="data-table" data-testid="quality-table">
            <thead>
              <tr><th>规则</th><th>校验字段</th><th>采用值</th><th>指向来源</th><th>口径</th><th>结果</th></tr>
            </thead>
            <tbody>
              <tr v-for="d in result.quality_check.details" :key="String(d.rule_no) + d.field">
                <td>{{ d.rule_no }}</td>
                <td>{{ d.field }}</td>
                <td>{{ d.checked_value }}</td>
                <td>{{ d.value_source ?? '—' }}</td>
                <td>{{ d.value_caliber ?? '—' }}</td>
                <td>
                  <span :class="d.passed ? 'tag-adopt' : 'tag-reject'">
                    {{ d.passed ? '通过' : d.block_policy === '阻断' ? '阻断' : '警告' }}
                  </span>
                  <div class="muted">{{ d.range_result.message }}</div>
                </td>
              </tr>
            </tbody>
          </table>
        </template>
        <p v-else class="muted">执行转换后，这里展示实际业务值、候选取值明细与质量结论。</p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  runTransform,
  type TransformRunResult,
} from '@/api/backend-service'

const DEMO_MAPPING = JSON.stringify(
  {
    目标金额: {
      sources: [
        { source: '账面列', caliber: '账面口径' },
        { source: '复核列', caliber: '复核口径' },
      ],
      strategy: 'max',
    },
  },
  null,
  2,
)
const DEMO_SOURCE = JSON.stringify({ 账面列: '900', 复核列: '1100' }, null, 2)

const ruleId = ref(1)
const idempotencyKey = ref('')
const mappingText = ref(DEMO_MAPPING)
const sourceText = ref(DEMO_SOURCE)
const loading = ref(false)
const errorMessage = ref('')
const result = ref<TransformRunResult | null>(null)

function loadDemo() {
  mappingText.value = DEMO_MAPPING
  sourceText.value = DEMO_SOURCE
  errorMessage.value = ''
}

function formatJson(value: unknown): string {
  return JSON.stringify(value)
}

async function execute() {
  loading.value = true
  errorMessage.value = ''
  try {
    const sourceData = JSON.parse(sourceText.value) as Record<string, unknown>
    result.value = await runTransform(
      ruleId.value,
      sourceData,
      idempotencyKey.value.trim() || undefined,
    )
  } catch (error) {
    errorMessage.value = error instanceof SyntaxError
      ? '源数据不是合法 JSON'
      : error instanceof Error
        ? error.message
        : '转换执行失败'
  } finally {
    loading.value = false
  }
}
</script>
