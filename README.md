# ffprobe媒体信息持久化

这是一个可独立发布的 MoviePilot V2 兼容插件仓库。GitHub 新建空仓库后，将本目录内的全部内容上传到仓库根目录即可。

```text
GitHub 仓库根目录/
├── package.v2.json
├── package.v2.entry.json
├── README.md
└── plugins.v2/
    └── ffprobemediainfopersistence/
        ├── __init__.py
        └── README.md
```

`package.v2.json` 是 MoviePilot 读取的正式市场索引。`package.v2.entry.json` 只是相同内容的单条目备份文件，可保留或删除，不影响插件加载。

推送 GitHub 后，在 MoviePilot 的插件市场添加你的 GitHub 仓库地址并同步；搜索“ffprobe媒体信息持久化”、安装并启用。

本插件优先复用“ffprobe命名补充”的内存缓存。缓存命中时立即后台写 JSON；缓存未命中时，最多使用 3 个后台任务进行各 10 秒的 ffprobe 兜底探测。
