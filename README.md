# 测量仪器控制通道中的成组读数采集

这是用于内部数据接入的 Node.js 项目。当前提供可独立启动的本地来源、单次原文读取接口、来源契约测试, 以及面向采集站的成组采集服务: 一组读数绑定本组确认过的通道配置与单位, 断线可恢复, 校验通过后才对下游开放。浏览器页面和仪器网关使用合成业务资料, 不连接真实账户或设备。

## 环境与命令

Node.js 22.22.1 或更新的 22.x。依赖由 package-lock.json 锁定。

```sh
npm ci --ignore-scripts
npm run build
npm test
npm start
```

`npm start` 在标准输出报告来源地址。默认绑定本机回环地址并使用动态端口; 可通过 `HOST` 与 `PORT` 指定监听地址和端口, 例如 `HOST=127.0.0.1 PORT=8080 npm start`。按 Ctrl+C 关闭来源; 动态端口避免与同机其他接入站冲突。`npm run demo` 在同一进程启动来源, 读取一次目录后退出, 不代表持续接收业务已经完成。

## 成组采集服务

```sh
SOURCE_URL=http://127.0.0.1:8080 npm run start:acquisition   # 或 GATEWAY_HOST/GATEWAY_PORT 直连网关
npm run demo:recovery                                        # 用复位/截断/异步通知开关演示恢复行为
```

采集服务把一组读数绑定到本组确认过的配置与单位: 按网关协议重组分片文本回答, 定长二进制块的边界只由块头长度决定; 命令与回答一一对应, 异步通知不占回答位置; 配置与读取在设备锁内执行, 停止采集即放锁; 断线后值班可区分"没有测量/结果待确认/已完成", 恢复只补读不重发触发。原始块、配置依据、接收进度与异常写入审计日志(默认 `.local/acquisition-journal.jsonl`)。语义详见 `docs/acquisition-contract.md`。

```sh
docker build -t instrument-blocks:local .
docker run --rm --network none instrument-blocks:local
docker run --rm --network none instrument-blocks:local npm run demo
```

依赖在镜像构建阶段安装，运行阶段不需要公网。完整来源测试和一次目录演示通常数秒即可结束，适合在90秒以内查看真实输出。浏览器在无界面模式执行，页面交互不依赖人工鼠标。

## 目录

- `src/source.mjs`：本地来源生命周期与HTTP入口。
- `src/client.mjs`：目录读取及原始字节获取。
- `src/instrument.mjs`：模拟仪器网关(SCPI 通道、定长块、分片、截断、异步通知开关)与单次型号识别。
- `src/acquire.mjs`：成组采集核心——帧解析、命令/回答对应、设备锁、组状态机与断线恢复、审计日志。
- `src/acquisition-http.mjs` / `src/acquisition-serve.mjs`：面向值班人员的采集 HTTP 接口与启动入口。
- `src/recovery-demo.mjs`：用网关开关演示恢复行为。
- `fixtures/source.json`：来源初始状态。
- `docs/source-contract.md`：来源协议范围、控制与领域语义。
- `docs/acquisition-contract.md`：成组采集的帧规则、状态机、恢复语义与值班接口。
- `test/source.test.mjs`：实际来源集成测试。
- `test/acquire.test.mjs`：成组采集与恢复行为测试。

运行时状态仅用于来源模拟，重启来源会回到 fixtures 中的值。接入业务自己的持久状态请使用 `.local/` 或明确的外部路径。来源管理入口 `/state` 和 `/control` 是本地操作设施，不能作为绕过浏览器页面的业务写入捷径。
