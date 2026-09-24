import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {resolveComponent:_resolveComponent,createVNode:_createVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,renderList:_renderList,Fragment:_Fragment,createElementBlock:_createElementBlock,createElementVNode:_createElementVNode,withKeys:_withKeys} = await importShared('vue');


const _hoisted_1 = { class: "d-flex flex-wrap align-center ga-2 mb-3" };
const _hoisted_2 = { class: "d-flex flex-wrap ga-2" };
const _hoisted_3 = { class: "d-flex flex-wrap ga-2 mb-3" };
const _hoisted_4 = { class: "d-flex flex-wrap align-center ga-2 mb-3" };
const _hoisted_5 = { class: "text-body-2" };
const _hoisted_6 = {
  key: 0,
  class: "text-caption"
};
const _hoisted_7 = { class: "d-flex flex-wrap align-center ga-2 mb-4" };
const _hoisted_8 = { class: "text-body-2" };
const _hoisted_9 = { class: "path-cell" };

const {computed,inject,onMounted,onUnmounted,ref} = await importShared('vue');


const apiBase = 'plugin/FFprobeMediaInfoPersistence/failed-extractions';

const _sfc_main = {
  __name: 'Page',
  props: {
  api: { type: Object, default: () => ({}) },
},
  emits: ['close'],
  setup(__props, { emit: __emit }) {

const props = __props;
const emit = __emit;

const toast = inject('moviepilot:toast', null);
const state = ref('pending');
const reason = ref('');
const page = ref(1);
const pageSize = ref(50);
const pageSizes = ref([50, 100, 300, 500, 1000]);
const pageCount = ref(1);
const total = ref(0);
const rows = ref([]);
const reasons = ref([]);
const counts = ref({ pending: 0, handled: 0, abnormal_size: 0, abnormal_size_pending: 0 });
const progressText = ref('所有任务后台运行，关闭此页面不影响执行；删除记录仅移除当前页面的运行记录；异常大小栏是 ffprobe 读取后，json信息的 Size < 1MB 的文件记录');
const progress = ref({ total: 0, running: false });
const retryRunning = ref(false);
const loading = ref(false);
const actionRunning = ref(false);
const error = ref('');
const notice = ref('');
const selectedIds = ref(new Set());
const requestedPage = ref('');
const currentFilterIds = ref(new Set());
const pageRoot = ref(null);
const helpDialog = ref(false);
let dialogStyleSnapshot = [];

const statusTabs = computed(() => [
  { key: 'pending', label: '未处理', count: counts.value.pending || 0, color: 'warning' },
  { key: 'handled', label: '已处理', count: counts.value.handled || 0, color: 'primary' },
  { key: 'abnormal_size', label: '异常大小已生成', count: counts.value.abnormal_size || 0, color: 'success' },
  { key: 'abnormal_size_pending', label: '异常大小未生成', count: counts.value.abnormal_size_pending || 0, color: 'error' },
]);

const selectedCount = computed(() => selectedIds.value.size);
const pageIds = computed(() => rows.value.map(row => String(row.id)));
const pageAllSelected = computed(() => pageIds.value.length > 0 && pageIds.value.every(id => selectedIds.value.has(id)));
const currentFilterAllSelected = computed(() => (
  currentFilterIds.value.size > 0
  && [...currentFilterIds.value].every(id => selectedIds.value.has(id))
));

function unwrap(response) {
  const body = response && Object.prototype.hasOwnProperty.call(response, 'success')
    ? response
    : (response?.data ?? response);
  if (body?.success === false) throw new Error(body.message || '请求失败')
  return body?.data ?? body ?? {}
}

function queryString(params) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') query.set(key, String(value));
  }
  const text = query.toString();
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
  selectedIds.value = new Set(selectedIds.value);
}

async function loadPage({ resetPage = false } = {}) {
  if (resetPage) page.value = 1;
  loading.value = true;
  error.value = '';
  try {
    const payload = unwrap(await props.api.get(
      `${apiBase}/list${queryString({ state: state.value, reason: reason.value, page: page.value, page_size: pageSize.value })}`,
    ));
    const nextRows = Array.isArray(payload.records) ? payload.records : [];
    retainLocalSelection();
    rows.value = nextRows;
    page.value = Number(payload.page) || 1;
    pageSize.value = Number(payload.page_size) || 50;
    pageSizes.value = Array.isArray(payload.page_sizes) ? payload.page_sizes : pageSizes.value;
    pageCount.value = Math.max(1, Number(payload.page_count) || 1);
    total.value = Number(payload.total) || 0;
    reasons.value = Array.isArray(payload.reasons) ? payload.reasons : [];
    counts.value = payload.counts || counts.value;
    progressText.value = String(payload.progress_text || progressText.value);
    progress.value = payload.progress || { total: 0, running: false };
    retryRunning.value = Boolean(payload.progress?.running);
    requestedPage.value = String(page.value);
  } catch (requestError) {
    error.value = requestError?.message || '加载失败提取记录失败';
  } finally {
    loading.value = false;
  }
}

function setSelected(id, checked) {
  const next = new Set(selectedIds.value);
  if (checked) next.add(String(id));
  else next.delete(String(id));
  selectedIds.value = next;
}

function togglePageSelection() {
  const next = new Set(selectedIds.value);
  if (pageAllSelected.value) pageIds.value.forEach(id => next.delete(id));
  else pageIds.value.forEach(id => next.add(id));
  selectedIds.value = next;
}

async function selectCurrentFilter() {
  actionRunning.value = true;
  error.value = '';
  try {
    const next = new Set(selectedIds.value);
    if (currentFilterAllSelected.value) {
      currentFilterIds.value.forEach(id => next.delete(id));
      currentFilterIds.value = new Set();
      selectedIds.value = next;
      return
    }
    const payload = unwrap(await props.api.get(
      `${apiBase}/ids${queryString({ state: state.value, reason: reason.value })}`,
    ));
    const ids = new Set((Array.isArray(payload.ids) ? payload.ids : []).map(String));
    ids.forEach(id => next.add(id));
    currentFilterIds.value = ids;
    selectedIds.value = next;
  } catch (requestError) {
    error.value = requestError?.message || '全选当前筛选失败';
  } finally {
    actionRunning.value = false;
  }
}

function switchState(nextState) {
  state.value = nextState;
  reason.value = '';
  currentFilterIds.value = new Set();
  loadPage({ resetPage: true });
}

function switchReason(nextReason) {
  reason.value = nextReason;
  currentFilterIds.value = new Set();
  loadPage({ resetPage: true });
}

function switchPage(nextPage) {
  const target = Math.min(Math.max(1, Number(nextPage) || 1), pageCount.value);
  if (target !== page.value) {
    page.value = target;
    loadPage();
  }
}

function jumpToPage() {
  switchPage(requestedPage.value);
}

function pageRange() {
  const values = new Set([1, pageCount.value]);
  for (let candidate = page.value - 2; candidate <= page.value + 2; candidate += 1) {
    if (candidate >= 1 && candidate <= pageCount.value) values.add(candidate);
  }
  return [...values].sort((left, right) => left - right)
}

async function submitSelected(action) {
  if (!selectedCount.value || actionRunning.value) return
  actionRunning.value = true;
  error.value = '';
  notice.value = '';
  try {
    const result = unwrap(await props.api.post(`${apiBase}/${action}`, {
      ids: [...selectedIds.value],
    }));
    notice.value = result.message || (action === 'retry' ? '已提交后台重新提取' : '已删除插件记录');
    toast?.success?.(notice.value);
    selectedIds.value = new Set();
    currentFilterIds.value = new Set();
    await loadPage();
  } catch (requestError) {
    error.value = requestError?.message || (action === 'retry' ? '重新提取提交失败' : '删除记录失败');
  } finally {
    actionRunning.value = false;
  }
}

const deleteDialog = ref(false);

function requestDelete() {
  if (selectedCount.value && !actionRunning.value && !retryRunning.value) {
    deleteDialog.value = true;
  }
}

async function confirmDelete() {
  deleteDialog.value = false;
  await submitSelected('delete');
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
  };
  dialogStyleSnapshot.push(prior);
  element.style.setProperty(property, value, 'important');
}

function expandHostDialog() {
  const root = pageRoot.value;
  const content = root?.closest?.('.v-overlay__content');
  if (!content) return
  setInlineStyle(content, 'width', 'min(calc(100vw - 32px), calc(80rem + 48px))');
  setInlineStyle(content, 'max-width', 'calc(80rem + 48px)');
  const card = root.closest?.('.v-card');
  if (card) setInlineStyle(card, 'width', '100%');
}

function restoreHostDialog() {
  for (const item of dialogStyleSnapshot.reverse()) {
    item.element.style.setProperty(item.property, item.value, item.priority);
  }
  dialogStyleSnapshot = [];
}

onMounted(() => {
  loadPage();
  requestAnimationFrame(expandHostDialog);
});
onUnmounted(restoreHostDialog);

return (_ctx, _cache) => {
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_alert = _resolveComponent("v-alert");
  const _component_v_spacer = _resolveComponent("v-spacer");
  const _component_v_select = _resolveComponent("v-select");
  const _component_v_text_field = _resolveComponent("v-text-field");
  const _component_v_progress_linear = _resolveComponent("v-progress-linear");
  const _component_v_checkbox_btn = _resolveComponent("v-checkbox-btn");
  const _component_v_table = _resolveComponent("v-table");
  const _component_v_card_text = _resolveComponent("v-card-text");
  const _component_v_card_actions = _resolveComponent("v-card-actions");
  const _component_v_card = _resolveComponent("v-card");
  const _component_v_dialog = _resolveComponent("v-dialog");

  return (_openBlock(), _createElementBlock("div", {
    ref_key: "pageRoot",
    ref: pageRoot,
    class: "ffprobe-records plugin-root"
  }, [
    _createVNode(_component_v_btn, {
      class: "plugin-close-button",
      icon: "mdi-close",
      variant: "text",
      density: "comfortable",
      "aria-label": "关闭",
      onClick: _cache[0] || (_cache[0] = $event => (emit('close')))
    }),
    (progress.value.total)
      ? (_openBlock(), _createBlock(_component_v_alert, {
          key: 0,
          type: "info",
          variant: "tonal",
          density: "compact",
          class: "mb-3"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(progressText.value), 1)
          ]),
          _: 1
        }))
      : _createCommentVNode("", true),
    (notice.value)
      ? (_openBlock(), _createBlock(_component_v_alert, {
          key: 1,
          type: "success",
          variant: "tonal",
          density: "compact",
          class: "mb-3"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(notice.value), 1)
          ]),
          _: 1
        }))
      : _createCommentVNode("", true),
    (error.value)
      ? (_openBlock(), _createBlock(_component_v_alert, {
          key: 2,
          type: "error",
          variant: "tonal",
          density: "compact",
          class: "mb-3"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(error.value), 1)
          ]),
          _: 1
        }))
      : _createCommentVNode("", true),
    _createElementVNode("div", _hoisted_1, [
      _createElementVNode("div", _hoisted_2, [
        (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(statusTabs.value, (item) => {
          return (_openBlock(), _createBlock(_component_v_btn, {
            key: item.key,
            size: "small",
            variant: state.value === item.key ? 'tonal' : 'text',
            color: item.color,
            disabled: loading.value || actionRunning.value,
            onClick: $event => (switchState(item.key))
          }, {
            default: _withCtx(() => [
              _createTextVNode(_toDisplayString(item.label) + "（" + _toDisplayString(item.count) + "）", 1)
            ]),
            _: 2
          }, 1032, ["variant", "color", "disabled", "onClick"]))
        }), 128))
      ]),
      _createVNode(_component_v_spacer),
      _createVNode(_component_v_btn, {
        class: "usage-help-button",
        size: "small",
        color: "warning",
        variant: "tonal",
        "prepend-icon": "mdi-information-outline",
        onClick: _cache[1] || (_cache[1] = $event => (helpDialog.value = true))
      }, {
        default: _withCtx(() => [...(_cache[13] || (_cache[13] = [
          _createTextVNode("使用说明", -1)
        ]))]),
        _: 1
      })
    ]),
    _createElementVNode("div", _hoisted_3, [
      _createVNode(_component_v_btn, {
        size: "x-small",
        variant: reason.value ? 'text' : 'tonal',
        color: reasonColor(''),
        onClick: _cache[2] || (_cache[2] = $event => (switchReason('')))
      }, {
        default: _withCtx(() => [...(_cache[14] || (_cache[14] = [
          _createTextVNode("全部原因", -1)
        ]))]),
        _: 1
      }, 8, ["variant", "color"]),
      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(reasons.value, (item) => {
        return (_openBlock(), _createBlock(_component_v_btn, {
          key: item,
          size: "x-small",
          variant: reason.value === item ? 'tonal' : 'text',
          color: reasonColor(item),
          onClick: $event => (switchReason(item))
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(item), 1)
          ]),
          _: 2
        }, 1032, ["variant", "color", "onClick"]))
      }), 128))
    ]),
    _createElementVNode("div", _hoisted_4, [
      _createElementVNode("span", _hoisted_5, "共 " + _toDisplayString(total.value) + " 条，当前第 " + _toDisplayString(page.value) + " / " + _toDisplayString(pageCount.value) + " 页", 1),
      _createVNode(_component_v_select, {
        modelValue: pageSize.value,
        "onUpdate:modelValue": [
          _cache[3] || (_cache[3] = $event => ((pageSize).value = $event)),
          _cache[4] || (_cache[4] = $event => (loadPage({ resetPage: true })))
        ],
        class: "page-size",
        density: "compact",
        "hide-details": "",
        label: "每页",
        items: pageSizes.value
      }, null, 8, ["modelValue", "items"]),
      _createVNode(_component_v_btn, {
        size: "x-small",
        variant: "text",
        disabled: page.value <= 1 || loading.value,
        onClick: _cache[5] || (_cache[5] = $event => (switchPage(page.value - 1)))
      }, {
        default: _withCtx(() => [...(_cache[15] || (_cache[15] = [
          _createTextVNode("上一页", -1)
        ]))]),
        _: 1
      }, 8, ["disabled"]),
      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(pageRange(), (number, index) => {
        return (_openBlock(), _createElementBlock(_Fragment, { key: number }, [
          (index && number - pageRange()[index - 1] > 1)
            ? (_openBlock(), _createElementBlock("span", _hoisted_6, "…"))
            : _createCommentVNode("", true),
          _createVNode(_component_v_btn, {
            size: "x-small",
            variant: number === page.value ? 'tonal' : 'text',
            color: number === page.value ? 'primary' : undefined,
            disabled: loading.value,
            onClick: $event => (switchPage(number))
          }, {
            default: _withCtx(() => [
              _createTextVNode(_toDisplayString(number), 1)
            ]),
            _: 2
          }, 1032, ["variant", "color", "disabled", "onClick"])
        ], 64))
      }), 128)),
      _createVNode(_component_v_btn, {
        size: "x-small",
        variant: "text",
        disabled: page.value >= pageCount.value || loading.value,
        onClick: _cache[6] || (_cache[6] = $event => (switchPage(page.value + 1)))
      }, {
        default: _withCtx(() => [...(_cache[16] || (_cache[16] = [
          _createTextVNode("下一页", -1)
        ]))]),
        _: 1
      }, 8, ["disabled"]),
      _createVNode(_component_v_text_field, {
        modelValue: requestedPage.value,
        "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((requestedPage).value = $event)),
        class: "goto-page",
        density: "compact",
        "hide-details": "",
        label: "前往页码",
        type: "number",
        min: "1",
        max: pageCount.value,
        onKeyup: _withKeys(jumpToPage, ["enter"])
      }, null, 8, ["modelValue", "max"]),
      _createVNode(_component_v_btn, {
        size: "x-small",
        variant: "text",
        disabled: loading.value,
        onClick: jumpToPage
      }, {
        default: _withCtx(() => [...(_cache[17] || (_cache[17] = [
          _createTextVNode("前往", -1)
        ]))]),
        _: 1
      }, 8, ["disabled"])
    ]),
    _createElementVNode("div", _hoisted_7, [
      _createElementVNode("span", _hoisted_8, "已选 " + _toDisplayString(selectedCount.value) + " 项", 1),
      _createVNode(_component_v_btn, {
        color: "primary",
        size: "small",
        disabled: !selectedCount.value || actionRunning.value || retryRunning.value,
        onClick: _cache[8] || (_cache[8] = $event => (submitSelected('retry')))
      }, {
        default: _withCtx(() => [...(_cache[18] || (_cache[18] = [
          _createTextVNode("重新提取", -1)
        ]))]),
        _: 1
      }, 8, ["disabled"]),
      _createVNode(_component_v_btn, {
        color: "error",
        variant: "tonal",
        size: "small",
        disabled: !selectedCount.value || actionRunning.value || retryRunning.value,
        onClick: requestDelete
      }, {
        default: _withCtx(() => [...(_cache[19] || (_cache[19] = [
          _createTextVNode("删除记录", -1)
        ]))]),
        _: 1
      }, 8, ["disabled"]),
      _createVNode(_component_v_btn, {
        size: "small",
        variant: "text",
        disabled: !total.value || actionRunning.value || retryRunning.value,
        onClick: selectCurrentFilter
      }, {
        default: _withCtx(() => [
          _createTextVNode(_toDisplayString(currentFilterAllSelected.value ? '取消全选当前筛选' : '全选当前筛选'), 1)
        ]),
        _: 1
      }, 8, ["disabled"]),
      _createVNode(_component_v_btn, {
        size: "small",
        variant: "text",
        disabled: loading.value || actionRunning.value,
        onClick: loadPage
      }, {
        default: _withCtx(() => [...(_cache[20] || (_cache[20] = [
          _createTextVNode("刷新进度", -1)
        ]))]),
        _: 1
      }, 8, ["disabled"])
    ]),
    (loading.value)
      ? (_openBlock(), _createBlock(_component_v_progress_linear, {
          key: 3,
          indeterminate: "",
          color: "primary",
          class: "mb-3"
        }))
      : _createCommentVNode("", true),
    (!loading.value && !rows.value.length)
      ? (_openBlock(), _createBlock(_component_v_alert, {
          key: 4,
          type: "success",
          variant: "tonal",
          density: "compact"
        }, {
          default: _withCtx(() => [...(_cache[21] || (_cache[21] = [
            _createTextVNode("当前筛选条件下没有失败提取记录", -1)
          ]))]),
          _: 1
        }))
      : (_openBlock(), _createBlock(_component_v_table, {
          key: 5,
          density: "compact",
          class: "records-table"
        }, {
          default: _withCtx(() => [
            _createElementVNode("thead", null, [
              _createElementVNode("tr", null, [
                _createElementVNode("th", null, [
                  _createVNode(_component_v_checkbox_btn, {
                    "model-value": pageAllSelected.value,
                    density: "compact",
                    "hide-details": "",
                    disabled: retryRunning.value,
                    "onUpdate:modelValue": togglePageSelection
                  }, null, 8, ["model-value", "disabled"])
                ]),
                _cache[22] || (_cache[22] = _createElementVNode("th", null, "首次记录时间", -1)),
                _cache[23] || (_cache[23] = _createElementVNode("th", null, "失败原因 / 大小信息", -1)),
                _cache[24] || (_cache[24] = _createElementVNode("th", null, "尝试次数", -1)),
                _cache[25] || (_cache[25] = _createElementVNode("th", null, "目标文件", -1))
              ])
            ]),
            _createElementVNode("tbody", null, [
              (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(rows.value, (row) => {
                return (_openBlock(), _createElementBlock("tr", {
                  key: row.id
                }, [
                  _createElementVNode("td", null, [
                    _createVNode(_component_v_checkbox_btn, {
                      "model-value": selectedIds.value.has(String(row.id)),
                      density: "compact",
                      "hide-details": "",
                      disabled: retryRunning.value,
                      "onUpdate:modelValue": checked => setSelected(row.id, checked)
                    }, null, 8, ["model-value", "disabled", "onUpdate:modelValue"])
                  ]),
                  _createElementVNode("td", null, _toDisplayString(row.first_failed_at || '-'), 1),
                  _createElementVNode("td", null, _toDisplayString(rowDetail(row)), 1),
                  _createElementVNode("td", null, _toDisplayString(row.attempt_count || 0), 1),
                  _createElementVNode("td", _hoisted_9, _toDisplayString(row.destination || '-'), 1)
                ]))
              }), 128))
            ])
          ]),
          _: 1
        })),
    _createVNode(_component_v_dialog, {
      modelValue: deleteDialog.value,
      "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((deleteDialog).value = $event)),
      "max-width": "30rem",
      persistent: ""
    }, {
      default: _withCtx(() => [
        _createVNode(_component_v_card, { title: "删除记录确认" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card_text, null, {
              default: _withCtx(() => [
                _createTextVNode("确定仅删除选中的 " + _toDisplayString(selectedCount.value) + " 条插件记录吗？不会删除媒体文件、MediaInfo JSON 或 MoviePilot 整理历史", 1)
              ]),
              _: 1
            }),
            _createVNode(_component_v_card_actions, null, {
              default: _withCtx(() => [
                _createVNode(_component_v_spacer),
                _createVNode(_component_v_btn, {
                  variant: "text",
                  onClick: _cache[9] || (_cache[9] = $event => (deleteDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[26] || (_cache[26] = [
                    _createTextVNode("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode(_component_v_btn, {
                  color: "error",
                  variant: "flat",
                  onClick: confirmDelete
                }, {
                  default: _withCtx(() => [...(_cache[27] || (_cache[27] = [
                    _createTextVNode("删除记录", -1)
                  ]))]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"]),
    _createVNode(_component_v_dialog, {
      modelValue: helpDialog.value,
      "onUpdate:modelValue": _cache[12] || (_cache[12] = $event => ((helpDialog).value = $event)),
      "max-width": "38rem"
    }, {
      default: _withCtx(() => [
        _createVNode(_component_v_card, { title: "使用说明" }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card_text, null, {
              default: _withCtx(() => [...(_cache[28] || (_cache[28] = [
                _createTextVNode(" 所有任务后台运行，关闭此页面不影响执行；删除记录仅移除当前页面的运行记录；异常大小栏是 ", -1),
                _createElementVNode("strong", null, "ffprobe", -1),
                _createTextVNode(" 读取后，json信息的 Size < 1MB 的文件记录 ", -1)
              ]))]),
              _: 1
            }),
            _createVNode(_component_v_card_actions, null, {
              default: _withCtx(() => [
                _createVNode(_component_v_spacer),
                _createVNode(_component_v_btn, {
                  variant: "text",
                  onClick: _cache[11] || (_cache[11] = $event => (helpDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[29] || (_cache[29] = [
                    _createTextVNode("关闭", -1)
                  ]))]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"])
  ], 512))
}
}

};
const Page = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-b6b79d3f"]]);

export { Page as default };
