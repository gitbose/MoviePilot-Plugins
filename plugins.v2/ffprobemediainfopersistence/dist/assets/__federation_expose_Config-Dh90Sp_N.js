import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createTextVNode:_createTextVNode,createElementVNode:_createElementVNode,openBlock:_openBlock,createElementBlock:_createElementBlock} = await importShared('vue');


const _hoisted_1 = { class: "config-scroll" };
const _hoisted_2 = { class: "config-lower" };
const _hoisted_3 = { class: "d-flex justify-end mt-5" };

const {onMounted,onUnmounted,ref} = await importShared('vue');



const _sfc_main = {
  __name: 'Config',
  props: {
  initialConfig: { type: Object, default: () => ({}) },
},
  emits: ['save', 'close', 'layout'],
  setup(__props, { emit: __emit }) {

const props = __props;
const emit = __emit;
const configRoot = ref(null);
let dialogStyleSnapshot = [];

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
});

function save() {
  const workers = Math.min(10, Math.max(1, Number(config.value.fallback_workers) || 3));
  const timeout = Math.min(300, Math.max(1, Number(config.value.fallback_timeout) || 10));
  emit('save', { ...config.value, fallback_workers: workers, fallback_timeout: timeout });
}

function setInlineStyle(element, property, value) {
  dialogStyleSnapshot.push({
    element,
    property,
    value: element.style.getPropertyValue(property),
    priority: element.style.getPropertyPriority(property),
  });
  element.style.setProperty(property, value, 'important');
}

function expandHostDialog() {
  const root = configRoot.value;
  const content = root?.closest?.('.v-overlay__content');
  if (!content) return
  setInlineStyle(content, 'width', 'min(calc(100vw - 32px), calc(60rem + 48px))');
  setInlineStyle(content, 'max-width', 'calc(60rem + 48px)');
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
  emit('layout', { maxWidth: 'calc(60rem + 48px)' });
  requestAnimationFrame(expandHostDialog);
});
onUnmounted(restoreHostDialog);

return (_ctx, _cache) => {
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_switch = _resolveComponent("v-switch");
  const _component_v_col = _resolveComponent("v-col");
  const _component_v_row = _resolveComponent("v-row");
  const _component_v_text_field = _resolveComponent("v-text-field");
  const _component_v_select = _resolveComponent("v-select");
  const _component_v_textarea = _resolveComponent("v-textarea");
  const _component_v_alert = _resolveComponent("v-alert");

  return (_openBlock(), _createElementBlock("div", {
    ref_key: "configRoot",
    ref: configRoot,
    class: "ffprobe-config plugin-root"
  }, [
    _createVNode(_component_v_btn, {
      class: "plugin-close-button",
      icon: "mdi-close",
      variant: "text",
      density: "comfortable",
      "aria-label": "关闭",
      onClick: _cache[0] || (_cache[0] = $event => (emit('close')))
    }),
    _createElementVNode("div", _hoisted_1, [
      _createVNode(_component_v_row, null, {
        default: _withCtx(() => [
          _createVNode(_component_v_col, {
            cols: "12",
            md: "6"
          }, {
            default: _withCtx(() => [
              _createVNode(_component_v_switch, {
                modelValue: config.value.enabled,
                "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((config.value.enabled) = $event)),
                label: "启用插件",
                hint: "开启后监听媒体整理事件并写入 MediaInfo JSON。",
                "persistent-hint": ""
              }, null, 8, ["modelValue"])
            ]),
            _: 1
          }),
          _createVNode(_component_v_col, {
            cols: "12",
            md: "6"
          }, {
            default: _withCtx(() => [
              _createVNode(_component_v_switch, {
                modelValue: config.value.cleanup_moved_source_json,
                "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((config.value.cleanup_moved_source_json) = $event)),
                label: "清理已搬离源文件的同名 MediaInfo JSON",
                hint: "整理完成 10 秒后，媒体文件若被删除，则删除同目录下严格同名的 JSON 文件。",
                "persistent-hint": ""
              }, null, 8, ["modelValue"])
            ]),
            _: 1
          })
        ]),
        _: 1
      }),
      _createElementVNode("div", _hoisted_2, [
        _createVNode(_component_v_row, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: config.value.fallback_probe,
                  "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((config.value.fallback_probe) = $event)),
                  label: "上游缓存缺失时主动提取",
                  hint: "仅缓存未命中时，对整理后的目标文件执行 ffprobe；任务在后台运行。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: config.value.overwrite_json,
                  "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((config.value.overwrite_json) = $event)),
                  label: "覆盖同名 JSON",
                  hint: "目标目录已有同名 JSON 时",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: config.value.allow_abnormal_size_json,
                  "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((config.value.allow_abnormal_size_json) = $event)),
                  label: "生成异常大小 JSON",
                  hint: "当ffprobe读取到的 Size < 1M 时，Size值写为0",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: config.value.fallback_workers,
                  "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((config.value.fallback_workers) = $event)),
                  modelModifiers: { number: true },
                  label: "主动提取并发数",
                  type: "number",
                  min: "1",
                  max: "10",
                  hint: "范围 1–10；驱动为网盘时不建议设置过大。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: config.value.fallback_timeout,
                  "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((config.value.fallback_timeout) = $event)),
                  modelModifiers: { number: true },
                  label: "主动提取超时（秒）",
                  type: "number",
                  min: "1",
                  max: "300",
                  hint: "范围 1–300 秒。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_select, {
                  modelValue: config.value.filter_match_mode,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = $event => ((config.value.filter_match_mode) = $event)),
                  label: "生成 JSON 匹配条件",
                  items: [{ title: '同时匹配', value: 'all' }, { title: '任一匹配', value: 'any' }],
                  hint: "两类条件都填写时，按此方式组合；留空条件不参与判断。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, { cols: "12" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.transfer_methods,
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = $event => ((config.value.transfer_methods) = $event)),
                  label: "限定整理方式（可选，一行一个）",
                  placeholder: "复制\n移动\n硬链接\n软链接",
                  rows: "3",
                  hint: "可填写：复制、移动、硬链接、软链接。留空表示不限制整理方式。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_row, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, { cols: "12" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_textarea, {
                  modelValue: config.value.destination_roots,
                  "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((config.value.destination_roots) = $event)),
                  label: "限定整理目标路径（可选，一行一个）",
                  placeholder: "/media/电影\n/media/剧集",
                  rows: "3",
                  hint: "只有最终文件在任一填写目录下才生成 JSON；留空表示不限制。填写 MP 容器内路径。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_alert, {
          type: "info",
          variant: "tonal",
          density: "compact",
          class: "mb-3"
        }, {
          default: _withCtx(() => [...(_cache[11] || (_cache[11] = [
            _createTextVNode("使用说明：优先复用“ffprobe命名补充”已获取的缓存，缓存命中后立即后台写入 JSON，最多 32 个并发。仅缓存缺失时才按“主动提取”配置对最终目标文件运行 ffprobe。上游 ffprobe 未请求章节，因此输出 JSON 的 Chapters 为空。", -1)
          ]))]),
          _: 1
        }),
        _createVNode(_component_v_alert, {
          type: "warning",
          variant: "tonal",
          density: "compact"
        }, {
          default: _withCtx(() => [...(_cache[12] || (_cache[12] = [
            _createTextVNode("JSON清理：文件整理完成后延迟 10 秒检查，若媒体文件已不存在，则清理媒体文件目录下严格同名的 -mediainfo.json 文件。", -1)
          ]))]),
          _: 1
        })
      ])
    ]),
    _createElementVNode("div", _hoisted_3, [
      _createVNode(_component_v_btn, {
        color: "primary",
        onClick: save
      }, {
        default: _withCtx(() => [...(_cache[13] || (_cache[13] = [
          _createTextVNode("保存", -1)
        ]))]),
        _: 1
      })
    ])
  ], 512))
}
}

};
const Config = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-03f91872"]]);

export { Config as default };
