import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import {mkdtemp} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createSource} from '../src/source.mjs';
import {createGatewayParser, DeviceLock, AcquisitionService, BusyError, StateConflictError} from '../src/acquire.mjs';
import {createAcquisitionServer} from '../src/acquisition-http.mjs';

async function withSource(callback) {
  const source = await createSource();
  try { return await callback(source); } finally { await source.close(); }
}
async function control(source, body) {
  const response = await fetch(source.url + '/control', {method: 'POST', body: JSON.stringify(body)});
  assert.equal(response.status, 200);
  return response.json();
}
async function tempJournal() {
  return join(await mkdtemp(join(tmpdir(), 'acq-test-')), 'journal.jsonl');
}
async function newService(source, journalPath) {
  return new AcquisitionService({endpoint: source.state.endpoint, journalPath: journalPath ?? await tempJournal()}).open();
}
// 模拟同事从另一条连接操作仪器
async function colleague(source, lines, responses = lines.length) {
  await new Promise((resolve, reject) => {
    const socket = net.connect(source.state.endpoint.port, source.state.endpoint.host);
    let received = 0;
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('同事操作超时')); }, 3000);
    socket.on('connect', () => { for (const line of lines) socket.write(line + '\n'); });
    socket.on('data', chunk => {
      for (const byte of chunk) if (byte === 0x0a) received += 1;
      if (received >= responses) { clearTimeout(timer); socket.end(); resolve(); }
    });
    socket.on('error', error => { clearTimeout(timer); reject(error); });
  });
}

test('帧解析: 分片文本、含换行的定长块与异步通知各归其位', () => {
  const events = [];
  const parser = createGatewayParser({
    onLine: text => events.push(['line', text]),
    onBlock: (payload, meta) => events.push(['block', payload, meta]),
    onNotify: text => events.push(['notify', text])
  });
  const payload = Buffer.alloc(16);
  payload.writeDoubleBE(1, 0);
  payload.writeDoubleBE(2, 8);
  payload[7] = 10; // 负载内部故意含换行字节
  const wire = Buffer.concat([Buffer.from('!STATUS 1\nOK 1\n#216'), payload, Buffer.from('\n')]);
  for (const byte of wire) parser.feed(Buffer.from([byte])); // 逐字节分片
  assert.deepEqual(events.filter(e => e[0] === 'notify'), [['notify', '!STATUS 1']]);
  assert.deepEqual(events.filter(e => e[0] === 'line'), [['line', 'OK 1']]);
  const blocks = events.filter(e => e[0] === 'block');
  assert.equal(blocks.length, 1);
  assert.equal(blocks[0][1].length, 16);
  assert.ok(blocks[0][1].equals(payload));
  assert.equal(blocks[0][2].header, '#216');
});

test('帧解析: 块未收齐时的接收进度由块头给出', () => {
  const parser = createGatewayParser({onLine() {}, onBlock() {}, onNotify() {}});
  parser.feed(Buffer.from('#216'));
  parser.feed(Buffer.from([1, 2, 3, 4, 10]));
  assert.deepEqual(parser.progress(), {kind: 'block', expectedBytes: 20, receivedBytes: 9, payloadBytes: 16});
});

// 仪器负载第 7 字节被写为 0x0A, 第一个 float64 解码为 1.0000000000000022
function assertReadings(values) {
  assert.equal(values.length, 2);
  assert.ok(Math.abs(values[0] - 1) < 1e-12, `第一个读数应约为 1, 实际 ${values[0]}`);
  assert.equal(values[1], 2);
}

test('成组采集绑定确认单位并保留原始块与配置依据', () => withSource(async source => {
  const service = await newService(source);
  const group = await service.runGroup({unit: 'V', owner: 'test'});
  assert.equal(group.state, 'completed');
  assert.equal(group.measurement, 'completed');
  assertReadings(group.readings.values);
  assert.equal(group.readings.unit, 'V');
  assert.equal(group.basis.identity, 'LOCAL,MEASURE,17,1');
  assert.equal(group.basis.confirmedUnit, 'V');
  assert.equal(group.basis.triggerId, 1);
  assert.equal(group.blocks.length, 1);
  assert.match(group.blocks[0].sha256, /^[0-9a-f]{64}$/);
  assert.ok(group.progress.some(p => p.stage === 'trigger'));
  const result = service.result(group.id);
  assertReadings(result.readings);
  assert.equal(result.unit, 'V');
  await service.close();
}));

test('异步通知不占用命令的回答位置', () => withSource(async source => {
  await control(source, {notify: true});
  const service = await newService(source);
  const group = await service.runGroup({unit: 'V'});
  assert.equal(group.state, 'completed');
  assert.ok(group.notifications.length > 0);
  assert.ok(group.notifications.every(text => text.startsWith('!STATUS')));
  assert.equal(group.basis.triggerId, 1);
  assertReadings(group.readings.values);
  await service.close();
}));

test('同事仅切换单位不改写本组读数标注', () => withSource(async source => {
  const service = await newService(source);
  const group = await service.runGroup({unit: 'V', hooks: {afterTrigger: () => colleague(source, ['CONF:UNIT MV'])}});
  assert.equal(group.state, 'completed');
  assert.equal(group.readings.unit, 'V');
  assertReadings(group.readings.values);
  await service.close();
}));

test('同事在确认单位后触发前改单位, 校验失败不误标', () => withSource(async source => {
  const service = await newService(source);
  const group = await service.runGroup({unit: 'V', hooks: {afterConfirm: () => colleague(source, ['CONF:UNIT MV'])}});
  assert.equal(group.state, 'failed');
  assert.equal(group.measurement, 'none');
  assert.match(group.exceptions.at(-1).message, /单位/);
  assert.throws(() => service.result(group.id), StateConflictError);
  await service.close();
}));

test('同事抢先触发被识别, 不把两次测量合为一次', () => withSource(async source => {
  const service = await newService(source);
  const group = await service.runGroup({unit: 'V', hooks: {afterTrigger: () => colleague(source, ['CONF:UNIT MV', 'INIT'], 2)}});
  assert.equal(group.state, 'failed');
  assert.equal(group.measurement, 'none');
  assert.match(group.exceptions.at(-1).message, /触发序号/);
  assert.throws(() => service.result(group.id), StateConflictError);
  await service.close();
}));

test('截断断线标记结果待确认, 恢复不重发触发且跨重启可续', () => withSource(async source => {
  const journalPath = await tempJournal();
  await control(source, {truncate: true});
  const serviceA = await newService(source, journalPath);
  const group = await serviceA.runGroup({unit: 'V'});
  assert.equal(group.state, 'pending');
  assert.equal(group.measurement, 'pending');
  const failure = group.exceptions.at(-1);
  assert.equal(failure.stage, 'fetch');
  assert.deepEqual(failure.progress, {kind: 'block', expectedBytes: 20, receivedBytes: 9, payloadBytes: 16});
  assert.throws(() => serviceA.result(group.id), StateConflictError);
  await serviceA.close();

  // 服务重启后从审计日志恢复组状态, 值班仍看到"结果待确认"
  const serviceB = await newService(source, journalPath);
  assert.equal(serviceB.get(group.id).measurement, 'pending');
  await control(source, {truncate: false});
  const recovered = await serviceB.recoverGroup(group.id);
  assert.equal(recovered.state, 'completed');
  assert.equal(recovered.basis.triggerId, 1);
  assert.equal((await control(source, {})).trigger, 1, '恢复不得重发 INIT');
  assertReadings(recovered.readings.values);
  assert.equal(serviceB.result(group.id).unit, 'V');
  await serviceB.close();
}));

test('设备复位后恢复判定为没有测量', () => withSource(async source => {
  const service = await newService(source);
  await control(source, {truncate: true});
  const group = await service.runGroup({unit: 'V'});
  assert.equal(group.measurement, 'pending');
  const state = await control(source, {});
  await control(source, {generation: state.generation + 1, trigger: 0, truncate: false});
  const recovered = await service.recoverGroup(group.id);
  assert.equal(recovered.state, 'failed');
  assert.equal(recovered.measurement, 'none');
  assert.match(recovered.exceptions.at(-1).message, /复位|世代/);
  assert.throws(() => service.result(group.id), StateConflictError);
  await service.close();
}));

test('两个作业不能穿插, 停止采集释放设备锁', () => withSource(async source => {
  const service = await newService(source);
  let hookEntered; const entered = new Promise(resolve => { hookEntered = resolve; });
  let releaseGate; const gate = new Promise(resolve => { releaseGate = resolve; });
  const first = service.runGroup({unit: 'V', hooks: {afterTrigger: () => { hookEntered(); return gate; }}});
  await entered;
  assert.equal(service.lockStatus().held, true);
  await assert.rejects(service.runGroup({unit: 'V'}), BusyError);
  await service.stop();
  assert.equal(service.lockStatus().held, false);
  releaseGate();
  const stopped = await first;
  assert.equal(stopped.state, 'pending', '触发后被停止的组保持待确认');
  assert.match(stopped.exceptions.at(-1).message, /停止|关闭/);
  const second = await service.runGroup({unit: 'V'});
  assert.equal(second.state, 'completed');
  const recovered = await service.recoverGroup(stopped.id);
  assert.equal(recovered.state, 'failed');
  assert.match(recovered.exceptions.at(-1).message, /触发序号/);
  await service.close();
}));

test('设备锁租约到期自动释放, 非持有者不能解锁', async () => {
  const lock = new DeviceLock();
  lock.acquire('job-a', 40);
  assert.throws(() => lock.acquire('job-b', 40), BusyError);
  lock.release('job-b');
  assert.equal(lock.status().held, true, '非持有者释放无效');
  await delay(60);
  assert.equal(lock.status().held, false, '租约到期自动释放');
  lock.acquire('job-b', 40);
  assert.equal(lock.status().owner, 'job-b');
});

test('HTTP 接口面向值班人员区分三种测量状态', () => withSource(async source => {
  const server = await createAcquisitionServer({endpoint: source.state.endpoint, journalPath: await tempJournal()});
  try {
    const api = async (path, options = {}) => {
      const response = await fetch(server.url + path, {method: options.method ?? 'GET', body: options.body === undefined ? undefined : JSON.stringify(options.body)});
      return {status: response.status, value: await response.json()};
    };
    assert.equal((await api('/health')).value.status, 'ok');
    assert.equal((await api('/groups', {method: 'POST', body: {unit: 'BAD\nINIT'}})).status, 400);
    assert.equal((await api('/groups/grp-9999')).status, 404);

    const done = await api('/groups', {method: 'POST', body: {unit: 'V'}});
    assert.equal(done.status, 200);
    assert.equal(done.value.group.measurement, 'completed');
    const result = await api(`/groups/${done.value.group.id}/result`);
    assert.equal(result.status, 200);
    assertReadings(result.value.result.readings);
    assert.equal(result.value.result.unit, 'V');

    await control(source, {truncate: true});
    const stuck = await api('/groups', {method: 'POST', body: {unit: 'V'}});
    assert.equal(stuck.value.group.measurement, 'pending');
    const refused = await api(`/groups/${stuck.value.group.id}/result`);
    assert.equal(refused.status, 409);
    assert.match(refused.value.error, /待确认/);
    await control(source, {truncate: false});
    const recovered = await api(`/groups/${stuck.value.group.id}/recover`, {method: 'POST'});
    assert.equal(recovered.value.group.measurement, 'completed');
    assert.equal((await api(`/groups/${stuck.value.group.id}/result`)).status, 200);

    const list = await api('/groups');
    assert.equal(list.value.groups.length, 2);
    const stop = await api('/stop', {method: 'POST'});
    assert.equal(stop.value.lock.held, false);
  } finally {
    await server.close();
  }
}));
