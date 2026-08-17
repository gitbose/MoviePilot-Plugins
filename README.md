# ffprobe媒体信息持久化

这是一个 MoviePilot V2 兼容插件。

本插件优先复用“ffprobe命名补充”的内存缓存。缓存命中时立即后台写 JSON；缓存未命中时，最多使用 3 个后台任务进行各 10 秒的 ffprobe 兜底探测。
