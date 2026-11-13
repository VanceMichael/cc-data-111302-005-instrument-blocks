# 来源协议

本地TCP仪器支持*IDN?、CONF:UNIT V|MV、INIT、FETCH?、STAT?。每条命令以LF结束。INIT递增trigger并返回OK；FETCH?返回两个大端float64的IEEE488.2定长块（#216后16字节和LF）。数值绑定最近触发时unit。notify时普通状态行以!STATUS开头插入回答前；truncate时FETCH只发部分块再断线。/control 控制复位世代、截断和分块大小。源码probe只识别型号，不提供成组采集。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
