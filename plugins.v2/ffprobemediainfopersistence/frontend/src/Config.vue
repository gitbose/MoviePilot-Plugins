<script setup>
import { onMounted, onUnmounted, ref } from 'vue'

const props = defineProps({
  initialConfig: { type: Object, default: () => ({}) },
})
const emit = defineEmits(['save', 'close', 'layout'])
const configRoot = ref(null)
let dialogStyleSnapshot = []

const config = ref({
  enabled: false,
  cleanup_moved_source_json: true,
  fallback_probe: true,
  overwrite_json: false,
  allow_abnormal_size_json: true,
  fallback_workers: 3,
  fallback_timeout: 10,
  filter_match_mode: 'all',
  transfer_methods: '',
  destination_roots: '',
  ...props.initialConfig,
})

function save() {
  const workers = Math.min(10, Math.max(1, Number(config.value.fallback_workers) || 3))
  const timeout = Math.min(300, Math.max(1, Number(config.value.fallback_timeout) || 10))
  emit('save', { ...config.value, fallback_workers: workers, fallback_timeout: timeout })
}

function setInlineStyle(element, property, value) {
  dialogStyleSnapshot.push({
    element,
    property,
    value: element.style.getPropertyValue(property),
    priority: element.style.getPropertyPriority(property),
  })
  element.style.setProperty(property, value, 'important')
}

function expandHostDialog() {
  const root = configRoot.value
  const content = root?.closest?.('.v-overlay__content')
  if (!content) return
  setInlineStyle(content, 'width', 'min(calc(100vw - 32px), calc(60rem + 48px))')
  setInlineStyle(content, 'max-width', 'calc(60rem + 48px)')
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
  emit('layout', { maxWidth: 'calc(60rem + 48px)' })
  requestAnimationFrame(expandHostDialog)
})
onUnmounted(restoreHostDialog)
</script>

<template>
  <div ref="configRoot" class="ffprobe-config plugin-root">
    <v-btn class="plugin-close-button" icon="mdi-close" variant="text" density="comfortable" aria-label="关闭" @click="emit('close')" />
    <div class="config-scroll">
      <v-row>
      <v-col cols="12" md="6">
        <v-switch v-model="config.enabled" label="启用插件" hint="开启后监听媒体整理事件并写入 MediaInfo JSON。" persistent-hint />
      </v-col>
      <v-col cols="12" md="6">
        <v-switch v-model="config.cleanup_moved_source_json" label="清理已搬离源文件的同名 MediaInfo JSON" hint="整理完成 10 秒后，媒体文件若被删除，则删除同目录下严格同名的 JSON 文件。" persistent-hint />
      </v-col>
      </v-row>
      <div class="config-lower">
      <v-row>
        <v-col cols="12" md="4">
          <v-switch v-model="config.fallback_probe" label="上游缓存缺失时主动提取" hint="仅缓存未命中时，对整理后的目标文件执行 ffprobe；任务在后台运行。" persistent-hint />
        </v-col>
        <v-col cols="12" md="4">
          <v-switch v-model="config.overwrite_json" label="覆盖同名 JSON" hint="目标目录已有同名 JSON 时" persistent-hint />
        </v-col>
        <v-col cols="12" md="4">
          <v-switch v-model="config.allow_abnormal_size_json" label="生成异常大小 JSON" hint="当ffprobe读取到的 Size &lt; 1M 时，Size值写为0" persistent-hint />
        </v-col>
      </v-row>
      <v-row>
        <v-col cols="12" md="4">
          <v-text-field v-model.number="config.fallback_workers" label="主动提取并发数" type="number" min="1" max="10" hint="范围 1–10；驱动为网盘时不建议设置过大。" persistent-hint />
        </v-col>
        <v-col cols="12" md="4">
          <v-text-field v-model.number="config.fallback_timeout" label="主动提取超时（秒）" type="number" min="1" max="300" hint="范围 1–300 秒。" persistent-hint />
        </v-col>
        <v-col cols="12" md="4">
          <v-select v-model="config.filter_match_mode" label="生成 JSON 匹配条件" :items="[{ title: '同时匹配', value: 'all' }, { title: '任一匹配', value: 'any' }]" hint="两类条件都填写时，按此方式组合；留空条件不参与判断。" persistent-hint />
        </v-col>
      </v-row>
      <v-row>
        <v-col cols="12">
          <v-textarea v-model="config.transfer_methods" label="限定整理方式（可选，一行一个）" placeholder="复制&#10;移动&#10;硬链接&#10;软链接" rows="3" hint="可填写：复制、移动、硬链接、软链接。留空表示不限制整理方式。" persistent-hint />
        </v-col>
      </v-row>
      <v-row>
        <v-col cols="12">
          <v-textarea v-model="config.destination_roots" label="限定整理目标路径（可选，一行一个）" placeholder="/media/电影&#10;/media/剧集" rows="3" hint="只有最终文件在任一填写目录下才生成 JSON；留空表示不限制。填写 MP 容器内路径。" persistent-hint />
        </v-col>
      </v-row>
      <v-alert type="info" variant="tonal" density="compact" class="mb-3">使用说明：优先复用“ffprobe命名补充”已获取的缓存，缓存命中后立即后台写入 JSON，最多 32 个并发。仅缓存缺失时才按“主动提取”配置对最终目标文件运行 ffprobe。上游 ffprobe 未请求章节，因此输出 JSON 的 Chapters 为空。</v-alert>
      <v-alert type="warning" variant="tonal" density="compact">JSON清理：文件整理完成后延迟 10 秒检查，若媒体文件已不存在，则清理媒体文件目录下严格同名的 -mediainfo.json 文件。</v-alert>
      </div>
    </div>
    <div class="d-flex justify-end mt-5"><v-btn color="primary" @click="save">保存</v-btn></div>
  </div>
</template>

<style scoped>
.ffprobe-config { box-sizing: border-box; display: flex; flex-direction: column; position: relative; height: min(calc(100vh - 48px), 62.5rem); overflow: hidden; padding: 20px 24px 24px; }
.plugin-close-button { position: absolute; top: 8px; right: 8px; z-index: 2; }
.config-scroll { flex: 1 1 auto; min-height: 0; margin-top: 32px; overflow-x: hidden; overflow-y: auto; }
.ffprobe-config :deep(.v-messages__message) { line-height: 1rem; }
.config-lower { margin-top: -10px; }
@media (max-width: 600px) {
  .ffprobe-config { padding: 16px; }
}
</style>
