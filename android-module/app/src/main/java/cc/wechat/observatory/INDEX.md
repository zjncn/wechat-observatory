<!-- AUTO-DOC: Update me when files in this folder change -->

# Observatory Module

该目录承载 Android 侧微信适配逻辑和配置入口。发送链路支持按 `wxid + kind` 分 lane 并发执行并保留同 lane 顺序；入站图片链路会排除微信缩略图，并在 8.0.76/8.0.77 上触发大图/原图下载后再上报；入站群聊在手机内解析 `msgSource` 为安全的 `at_self` 标记；语音只允许按消息记录精确绑定，禁止跨会话使用“最近文件”兜底；另提供实验性的撤回和源消息绑定收款分支。

## Files

| File | Role | Function |
|------|------|----------|
| HookEntry.java | Runtime | Hook 微信数据库、上传消息，调度原图下载、媒体重试、outbox 并发发送、撤回与转账收款 |
| InboundImagePolicy.java | Media | 过滤 `th_` 等微信缩略图并生成仅指向大图/原图的候选文件名 |
| InboundMentionPolicy.java | Routing | 在手机内解析群消息 `msgSource`，仅输出当前账号是否为 @目标 |
| TenpaySignFallback.java | Payment | 在 Android 分身环境无法解析 Tenpay JNI 时，为模块构造的转账请求提供线程隔离的原生兼容签名 |
| TransferMessageParser.java | Payment | 纯 Java 解析 `wcpayinfo`，校验待收状态、分币金额、收付款人和确认参数 |
| WeChatVersionSymbols.java | Compatibility | 按运行时结构签名选择微信 8.0.75/8.0.76/8.0.77 混淆符号、发送方法与原图下载能力 |
| SettingsActivity.java | UI | 暴露模块轮询与出站并发配置 |
| BridgeConfigProvider.java | Config | 向微信进程导出可读取的模块设置 |
| BridgeConfigReceiver.java | Config | 接收外部配置广播并刷新镜像配置 |
