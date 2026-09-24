<script setup>
import { computed, inject, onMounted, onUnmounted, ref } from 'vue'

const props = defineProps({
  api: { type: Object, default: () => ({}) },
})
const emit = defineEmits(['close'])

const toast = inject('moviepilot:toast', null)
const apiBase = 'plugin/FFprobeMediaInfoPersistence/failed-extractions'
const state = ref('pending')
const reason = ref('')
const page = ref(1)
const pageSize = ref(50)
const pageSizes = ref([50, 100, 300, 500, 1000])
const pageCount = ref(1)
const total = ref(0)
const rows = ref([])
const reasons = ref([])
const counts = ref({ pending: 0, handled: 0, abnormal_size: 0, abnormal_size_pending: 0 })
const progressText = ref('所有任务后台运行，关闭此页面不影响执行；删除记录仅移除当前页面的运行记录；异常大小栏是 ffprobe 读取后，json信息的 Size < 1MB 的文件记录')
const progress = ref({ total: 0, running: false })
const retryRunning = ref(false)
const loading = ref(false)
const actionRunning = ref(false)
const error = ref('')
const notice = ref('')
const selectedIds = ref(new Set())
const requestedPage = ref('')
const currentFilterIds = ref(new Set())
const pageRoot = ref(null)
const helpDialog = ref(false)
let dialogStyleSnapshot = []

const statusTabs = computed(() => [
  { key: 'pending', label: '未处理', count: counts.value.pending || 0, color: 'warning' },
  { key: 'handled', label: '已处理', count: counts.value.handled || 0, color: 'primary' },
  { key: 'abnormal_size', label: '异常大小已生成', count: counts.value.abnormal_size || 0, color: 'success' },
  { key: 'abnormal_size_pending', label: '异常大小未生成', count: counts.value.abnormal_size_pending || 0, color: 'error' },
])

const selectedCount = computed(() => selectedIds.value.size)
const pageIds = computed(() => rows.value.map(row => String(row.id)))
const pageAllSelected = computed(() => pageIds.value.length > 0 && pageIds.value.every(id => selectedIds.value.has(id)))
const currentFilterAllSelected = computed(() => (
  currentFilterIds.value.size > 0
  && [...currentFilterIds.value].every(id => selectedIds.value.has(id))
))

function unwrap(response) {
  const body = response && Object.prototype.hasOwnProperty.call(response, 'success')
    ? response
    : (response?.data ?? response)
  if (body?.success === false) throw new Error(body.message || '请求失败')
  return body?.data ?? body ?? {}
}

function queryString(params) {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') query.set(key, String(value))
  }
  const text = query.toString()
  return text ? `?${text}` : ''
}

function reasonColor(value) {
  if (!value) return 'primary'
  if (value.includes('超时')) return 'warning'
  if (value.includes('失败')) return 'error'
  return 'info'
}

function retainLocalSelection() {
  // 翻页、筛选或手动刷新均不写入后端，也不丢弃已在其他页勾选的记录。
  selectedIds.value = new Set(selectedIds.value)
}

async function loadPage({ resetPage = false } = {}) {
  if (resetPage) page.value = 1
  loading.value = true
  error.value = ''
  try {
    const payload = unwrap(await props.api.get(
      `${apiBase}/list${queryString({ state: state.value, reason: reason.value, page: page.value, page_size: pageSize.value })}`,
    ))
    const nextRows = Array.isArray(payload.records) ? payload.records : []
    retainLocalSelection()
    rows.value = nextRows
    page.value = Number(payload.page) || 1
    pageSize.value = Number(payload.page_size) || 50
    pageSizes.value = Array.isArray(payload.page_sizes) ? payload.page_sizes : pageSizes.value
    pageCount.value = Math.max(1, Number(payload.page_count) || 1)
    total.value = Number(payload.total) || 0
    reasons.value = Array.isArray(payload.reasons) ? payload.reasons : []
    counts.value = payload.counts || counts.value
    progressText.value = String(payload.progress_text || progressText.value)
    progress.value = payload.progress || { total: 0, running: false }
    retryRunning.value = Boolean(payload.progress?.running)
    requestedPage.value = String(page.value)
  } catch (requestError) {
    error.value = requestError?.message || '加载失败提取记录失败'
  } finally {
    loading.value = false
  }
}

function setSelected(id, checked) {
  const next = new Set(selectedIds.value)
  if (checked) next.add(String(id))
  else next.delete(String(id))
  selectedIds.value = next
}

function togglePageSelection() {
  const next = new Set(selectedIds.value)
  if (pageAllSelected.value) pageIds.value.forEach(id => next.delete(id))
  else pageIds.value.forEach(id => next.add(id))
  selectedIds.value = next
}

async function selectCurrentFilter() {
  actionRunning.value = true
  error.value = ''
  try {
    const next = new Set(selectedIds.value)
    if (currentFilterAllSelected.value) {
      currentFilterIds.value.forEach(id => next.delete(id))
      currentFilterIds.value = new Set()
      selectedIds.value = next
      return
    }
    const payload = unwrap(await props.api.get(
      `${apiBase}/ids${queryString({ state: state.value, reason: reason.value })}`,
    ))
    const ids = new Set((Array.isArray(payload.ids) ? payload.ids : []).map(String))
    ids.forEach(id => next.add(id))
    currentFilterIds.value = ids
    selectedIds.value = next
  } catch (requestError) {
    error.value = requestError?.message || '全选当前筛选失败'
  } finally {
    actionRunning.value = false
  }
}

function switchState(nextState) {
  state.value = nextState
  reason.value = ''
  currentFilterIds.value = new Set()
  loadPage({ resetPage: true })
}

function switchReason(nextReason) {
  reason.value = nextReason
  currentFilterIds.value = new Set()
  loadPage({ resetPage: true })
}

function switchPage(nextPage) {
  const target = Math.min(Math.max(1, Number(nextPage) || 1), pageCount.value)
  if (target !== page.value) {
    page.value = target
    loadPage()
  }
}

function jumpToPage() {
  switchPage(requestedPage.value)
}

function pageRange() {
  const values = new Set([1, pageCount.value])
  for (let candidate = page.value - 2; candidate <= page.value + 2; candidate += 1) {
    if (candidate >= 1 && candidate <= pageCount.value) values.add(candidate)
  }
  return [...values].sort((left, right) => left - right)
}

async function submitSelected(action) {
  if (!selectedCount.value || actionRunning.value) return
  actionRunning.value = true
  error.value = ''
  notice.value = ''
  try {
    const result = unwrap(await props.api.post(`${apiBase}/${action}`, {
      ids: [...selectedIds.value],
    }))
    notice.value = result.message || (action === 'retry' ? '已提交后台重新提取' : '已删除插件记录')
    toast?.success?.(notice.value)
    selectedIds.value = new Set()
    currentFilterIds.value = new Set()
    await loadPage()
  } catch (requestError) {
    error.value = requestError?.message || (action === 'retry' ? '重新提取提交失败' : '删除记录失败')
  } finally {
    actionRunning.value = false
  }
}

const deleteDialog = ref(false)

function requestDelete() {
  if (selectedCount.value && !actionRunning.value && !retryRunning.value) {
    deleteDialog.value = true
  }
}

async function confirmDelete() {
  deleteDialog.value = false
  await submitSelected('delete')
}

function rowDetail(row) {
  if (row.category === 'abnormal_size') return `原始 Size: ${row.raw_size}（已按 0 写入 JSON）`
  if (row.category === 'abnormal_size_pending') return `原始 Size: ${row.raw_size}（未生成 JSON）`
  return row.reason || '未知错误'
}

function setInlineStyle(element, property, value) {
  const prior = {
    element,
    property,
    value: element.style.getPropertyValue(property),
    priority: element.style.getPropertyPriority(property),
  }
  dialogStyleSnapshot.push(prior)
  element.style.setProperty(property, value, 'important')
}

function expandHostDialog() {
  const root = pageRoot.value
  const content = root?.closest?.('.v-overlay__content')
  if (!content) return
  setInlineStyle(content, 'width', 'min(calc(100vw - 32px), calc(80rem + 48px))')
  setInlineStyle(content, 'max-width', 'calc(80rem + 48px)')
  const card = root.closest?.('.v-card')
  if (card) setInlineStyle(card, 'width', '100%')
}

function restoreHostDialog() {
  for (const item of dialogStyleSnapshot.reverse()) {
    item.element.style.setProperty(item.property, item.value, item.priority)
  }
  dialogStyleSnapshot = []
}

onMounted(() => {
  loadPage()
  requestAnimationFrame(expandHostDialog)
})
onUnmounted(restoreHostDialog)
</script>

<template>
  <div ref="pageRoot" class="ffprobe-records plugin-root">
    <v-btn class="plugin-close-button" icon="mdi-close" variant="text" density="comfortable" aria-label="关闭" @click="emit('close')" />
    <v-alert v-if="progress.total" type="info" variant="tonal" density="compact" class="mb-3">
      {{ progressText }}
    </v-alert>
    <v-alert v-if="notice" type="success" variant="tonal" density="compact" class="mb-3">
      {{ notice }}
    </v-alert>
    <v-alert v-if="error" type="error" variant="tonal" density="compact" class="mb-3">
      {{ error }}
    </v-alert>

    <div class="d-flex flex-wrap align-center ga-2 mb-3">
      <div class="d-flex flex-wrap ga-2">
        <v-btn
          v-for="item in statusTabs"
          :key="item.key"
          size="small"
          :variant="state === item.key ? 'tonal' : 'text'"
          :color="item.color"
          :disabled="loading || actionRunning"
          @click="switchState(item.key)"
        >{{ item.label }}（{{ item.count }}）</v-btn>
      </div>
      <v-spacer />
      <v-btn class="usage-help-button" size="small" color="warning" variant="tonal" prepend-icon="mdi-information-outline" @click="helpDialog = true">使用说明</v-btn>
    </div>

    <div class="d-flex flex-wrap ga-2 mb-3">
      <v-btn size="x-small" :variant="reason ? 'text' : 'tonal'" :color="reasonColor('')" @click="switchReason('')">全部原因</v-btn>
      <v-btn
        v-for="item in reasons"
        :key="item"
        size="x-small"
        :variant="reason === item ? 'tonal' : 'text'"
        :color="reasonColor(item)"
        @click="switchReason(item)"
      >{{ item }}</v-btn>
    </div>

    <div class="d-flex flex-wrap align-center ga-2 mb-3">
      <span class="text-body-2">共 {{ total }} 条，当前第 {{ page }} / {{ pageCount }} 页</span>
      <v-select v-model="pageSize" class="page-size" density="compact" hide-details label="每页" :items="pageSizes" @update:model-value="loadPage({ resetPage: true })" />
      <v-btn size="x-small" variant="text" :disabled="page <= 1 || loading" @click="switchPage(page - 1)">上一页</v-btn>
      <template v-for="(number, index) in pageRange()" :key="number">
        <span v-if="index && number - pageRange()[index - 1] > 1" class="text-caption">…</span>
        <v-btn size="x-small" :variant="number === page ? 'tonal' : 'text'" :color="number === page ? 'primary' : undefined" :disabled="loading" @click="switchPage(number)">{{ number }}</v-btn>
      </template>
      <v-btn size="x-small" variant="text" :disabled="page >= pageCount || loading" @click="switchPage(page + 1)">下一页</v-btn>
      <v-text-field v-model="requestedPage" class="goto-page" density="compact" hide-details label="前往页码" type="number" min="1" :max="pageCount" @keyup.enter="jumpToPage" />
      <v-btn size="x-small" variant="text" :disabled="loading" @click="jumpToPage">前往</v-btn>
    </div>

    <div class="d-flex flex-wrap align-center ga-2 mb-4">
      <span class="text-body-2">已选 {{ selectedCount }} 项</span>
      <v-btn color="primary" size="small" :disabled="!selectedCount || actionRunning || retryRunning" @click="submitSelected('retry')">重新提取</v-btn>
      <v-btn color="error" variant="tonal" size="small" :disabled="!selectedCount || actionRunning || retryRunning" @click="requestDelete">删除记录</v-btn>
      <v-btn size="small" variant="text" :disabled="!total || actionRunning || retryRunning" @click="selectCurrentFilter">{{ currentFilterAllSelected ? '取消全选当前筛选' : '全选当前筛选' }}</v-btn>
      <v-btn size="small" variant="text" :disabled="loading || actionRunning" @click="loadPage">刷新进度</v-btn>
    </div>

    <v-progress-linear v-if="loading" indeterminate color="primary" class="mb-3" />
    <v-alert v-if="!loading && !rows.length" type="success" variant="tonal" density="compact">当前筛选条件下没有失败提取记录</v-alert>
    <v-table v-else density="compact" class="records-table">
      <thead>
        <tr>
          <th><v-checkbox-btn :model-value="pageAllSelected" density="compact" hide-details :disabled="retryRunning" @update:model-value="togglePageSelection" /></th>
          <th>首次记录时间</th>
          <th>失败原因 / 大小信息</th>
          <th>尝试次数</th>
          <th>目标文件</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.id">
          <td><v-checkbox-btn :model-value="selectedIds.has(String(row.id))" density="compact" hide-details :disabled="retryRunning" @update:model-value="checked => setSelected(row.id, checked)" /></td>
          <td>{{ row.first_failed_at || '-' }}</td>
          <td>{{ rowDetail(row) }}</td>
          <td>{{ row.attempt_count || 0 }}</td>
          <td class="path-cell">{{ row.destination || '-' }}</td>
        </tr>
      </tbody>
    </v-table>

    <v-dialog v-model="deleteDialog" max-width="30rem" persistent>
      <v-card title="删除记录确认">
        <v-card-text>确定仅删除选中的 {{ selectedCount }} 条插件记录吗？不会删除媒体文件、MediaInfo JSON 或 MoviePilot 整理历史</v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="deleteDialog = false">取消</v-btn>
          <v-btn color="error" variant="flat" @click="confirmDelete">删除记录</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <v-dialog v-model="helpDialog" max-width="38rem">
      <v-card title="使用说明">
        <v-card-text>
          所有任务后台运行，关闭此页面不影响执行；删除记录仅移除当前页面的运行记录；异常大小栏是 <strong>ffprobe</strong> 读取后，json信息的 Size &lt; 1MB 的文件记录
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="helpDialog = false">关闭</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<style scoped>
.ffprobe-records { box-sizing: border-box; display: flex; flex-direction: column; position: relative; height: min(88vh, 60rem); min-width: 0; overflow: hidden; padding: 20px 24px 24px; }
.plugin-close-button { position: absolute; top: 8px; right: 8px; z-index: 2; }
.usage-help-button { margin-right: 36px; }
.records-table { display: flex; flex: 1 1 auto; min-height: 0; }
.records-table :deep(.v-table__wrapper) { flex: 1 1 auto; min-height: 0; overflow-y: auto; }
.records-table :deep(thead th) { background: rgb(var(--v-theme-surface)); position: sticky; top: 0; z-index: 1; }
.page-size { width: 116px; }
.goto-page { width: 112px; }
.path-cell { max-width: 520px; overflow-wrap: anywhere; }
@media (max-width: 600px) {
  .ffprobe-records { padding: 16px; }
  .usage-help-button { margin-right: 32px; }
}
</style>
