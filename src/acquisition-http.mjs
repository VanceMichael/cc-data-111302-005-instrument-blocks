import http from 'node:http';
import {AcquisitionService, BusyError, NotFoundError, StateConflictError, ClientInputError, MEASUREMENT_LABELS} from './acquire.mjs';

function send(response, status, value) {
  response.writeHead(status, {'Content-Type': 'application/json; charset=utf-8'});
  response.end(JSON.stringify(value));
}
function statusOf(error) {
  if (error instanceof BusyError) return 409;
  if (error instanceof StateConflictError) return 409;
  if (error instanceof NotFoundError) return 404;
  if (error instanceof ClientInputError) return 400;
  return 500;
}
// 面向值班人员的成组采集 HTTP 接口。采集结果(含失败)是数据, 用 200 返回组记录;
// 只有调用本身有问题(未知组、参数无效、设备忙、状态冲突)才用 4xx。
export async function createAcquisitionServer({endpoint, journalPath, lockTtlMs, commandTimeoutMs, host = '127.0.0.1', port = 0} = {}) {
  const service = await new AcquisitionService({endpoint, journalPath, lockTtlMs, commandTimeoutMs}).open();
  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://acquisition.local');
      let text = '';
      for await (const chunk of request) {
        text += chunk;
        if (text.length > 1024 * 1024) return send(response, 413, {error: '请求过大'});
      }
      let body = {};
      if (text) { try { body = JSON.parse(text); } catch { return send(response, 400, {error: 'JSON无效'}); } }
      if (url.pathname === '/health') return send(response, 200, {status: 'ok'});
      if (url.pathname === '/lock' && request.method === 'GET') return send(response, 200, service.lockStatus());
      if (url.pathname === '/labels' && request.method === 'GET') return send(response, 200, MEASUREMENT_LABELS);
      if (url.pathname === '/groups' && request.method === 'GET') return send(response, 200, {groups: service.list()});
      if (url.pathname === '/groups' && request.method === 'POST') {
        const group = await service.runGroup({unit: body.unit, owner: body.owner});
        return send(response, 200, {group});
      }
      const match = /^\/groups\/([^/]+)(\/recover|\/result)?$/.exec(url.pathname);
      if (match) {
        const [, id, action] = match;
        if (!action && request.method === 'GET') return send(response, 200, {group: service.get(id)});
        if (action === '/recover' && request.method === 'POST') return send(response, 200, {group: await service.recoverGroup(id)});
        if (action === '/result' && request.method === 'GET') return send(response, 200, {result: service.result(id)});
      }
      if (url.pathname === '/stop' && request.method === 'POST') {
        await service.stop();
        return send(response, 200, {status: '已停止', lock: service.lockStatus()});
      }
      return send(response, 404, {error: '资源不存在'});
    } catch (error) {
      return send(response, statusOf(error), {error: error.message, kind: error.name});
    }
  });
  await new Promise(resolve => server.listen(port, host, resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    service,
    close: async () => { await service.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
  };
}
