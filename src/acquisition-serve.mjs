import {createAcquisitionServer} from './acquisition-http.mjs';

// 仪器网关地址: 优先 GATEWAY_HOST/GATEWAY_PORT, 否则从来源服务 SOURCE_URL 的 /state 发现
async function resolveEndpoint() {
  if (process.env.GATEWAY_PORT) return {host: process.env.GATEWAY_HOST ?? '127.0.0.1', port: Number(process.env.GATEWAY_PORT)};
  if (process.env.SOURCE_URL) {
    const response = await fetch(process.env.SOURCE_URL + '/state', {signal: AbortSignal.timeout(3000)});
    if (!response.ok) throw new Error(`来源状态读取失败: ${response.status}`);
    const state = await response.json();
    if (!state.endpoint?.port) throw new Error('来源状态中没有仪器网关地址');
    return state.endpoint;
  }
  throw new Error('请用 GATEWAY_HOST/GATEWAY_PORT 或 SOURCE_URL 指定仪器网关');
}

const endpoint = await resolveEndpoint();
const server = await createAcquisitionServer({
  endpoint,
  journalPath: process.env.JOURNAL ?? '.local/acquisition-journal.jsonl',
  lockTtlMs: process.env.LOCK_TTL_MS ? Number(process.env.LOCK_TTL_MS) : undefined,
  host: process.env.HOST ?? '127.0.0.1',
  port: Number(process.env.PORT ?? 0)
});
console.log(JSON.stringify({acquisition: server.url, gateway: endpoint, status: '成组采集服务就绪'}));
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await server.close();
  process.exit(0);
}
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
