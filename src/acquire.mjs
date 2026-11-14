import net from 'node:net';
import {createHash} from 'node:crypto';
import {mkdir, appendFile, readFile} from 'node:fs/promises';
import {dirname} from 'node:path';

// ---- 错误类型: 采集失败是数据, 值班人员据此区分"没有测量/结果待确认/已完成" ----
export class ProtocolError extends Error { constructor(message, progress) { super(message); this.name = 'ProtocolError'; if (progress) this.progress = progress; } }
export class DisconnectError extends Error { constructor(message, progress) { super(message); this.name = 'DisconnectError'; if (progress) this.progress = progress; } }
export class InstrumentError extends Error { constructor(message) { super(message); this.name = 'InstrumentError'; } }
export class BusyError extends Error { constructor(message) { super(message); this.name = 'BusyError'; } }
export class NotFoundError extends Error { constructor(message) { super(message); this.name = 'NotFoundError'; } }
export class StateConflictError extends Error { constructor(message) { super(message); this.name = 'StateConflictError'; } }
export class GroupValidationError extends Error { constructor(message) { super(message); this.name = 'GroupValidationError'; } }
export class ClientInputError extends Error { constructor(message) { super(message); this.name = 'ClientInputError'; } }

// 值班视图的三态测量分类
export const MEASUREMENT_LABELS = {none: '没有测量', pending: '结果待确认', completed: '已完成'};

const LF = 0x0a;
const HASH_MARK = 0x23;
const MAX_BLOCK_BYTES = 16 * 1024 * 1024;

// 网关帧解析。普通回答是以 LF 结束的文本行, 可能分任意多片到达;
// 定长二进制块为 #<位数><长度><负载>, 边界只由块头声明的长度决定,
// 不用读超时或换行猜边界(负载里可能含 0x0A); 以 ! 开头的行是异步通知。
export function createGatewayParser({onLine, onBlock, onNotify}) {
  let buffer = Buffer.alloc(0);
  let afterBlock = false; // 块负载收齐后允许跟一个 LF 结束符
  function drain() {
    for (;;) {
      if (buffer.length === 0) return;
      if (afterBlock) {
        if (buffer[0] === LF) buffer = buffer.subarray(1);
        afterBlock = false;
        if (buffer.length === 0) return;
      }
      if (buffer[0] === HASH_MARK) {
        if (buffer.length < 2) return;
        const digits = buffer[1] - 0x30;
        if (digits < 1 || digits > 9) throw new ProtocolError(`定长块头位数无效: 0x${buffer[1].toString(16)}`, progress());
        if (buffer.length < 2 + digits) return;
        const lengthText = buffer.subarray(2, 2 + digits).toString('latin1');
        if (!/^\d+$/.test(lengthText)) throw new ProtocolError(`定长块长度字段无效: ${lengthText}`, progress());
        const length = Number(lengthText);
        if (length > MAX_BLOCK_BYTES) throw new ProtocolError(`定长块长度超限: ${length}`, progress());
        const total = 2 + digits + length;
        if (buffer.length < total) return; // 负载未收齐, 继续等字节; 边界来自块头
        const payload = Buffer.from(buffer.subarray(2 + digits, total));
        buffer = buffer.subarray(total);
        afterBlock = true;
        onBlock(payload, {header: `#${digits}${lengthText}`, length});
        continue;
      }
      const lf = buffer.indexOf(LF);
      if (lf < 0) return;
      const rawLine = buffer.subarray(0, lf);
      buffer = buffer.subarray(lf + 1);
      const text = rawLine.toString('utf8').replace(/\r$/, '');
      if (text.length === 0) continue;
      if (text.startsWith('!')) onNotify(text);
      else onLine(text);
    }
  }
  function progress() {
    if (buffer.length === 0) return afterBlock ? {kind: 'block-terminator', receivedBytes: 0} : {kind: 'idle', receivedBytes: 0};
    if (buffer[0] === HASH_MARK) {
      if (buffer.length < 2) return {kind: 'block-header', receivedBytes: buffer.length};
      const digits = buffer[1] - 0x30;
      if (digits < 1 || digits > 9 || buffer.length < 2 + digits) return {kind: 'block-header', receivedBytes: buffer.length};
      const lengthText = buffer.subarray(2, 2 + digits).toString('latin1');
      if (!/^\d+$/.test(lengthText)) return {kind: 'block-header', receivedBytes: buffer.length};
      return {kind: 'block', expectedBytes: 2 + digits + Number(lengthText), receivedBytes: buffer.length, payloadBytes: Number(lengthText)};
    }
    return {kind: 'line', receivedBytes: buffer.length};
  }
  return {
    feed(chunk) { buffer = buffer.length === 0 ? Buffer.from(chunk) : Buffer.concat([buffer, chunk]); drain(); },
    progress
  };
}

// 一条网关连接上的命令/回答对应: 命令串行发出, 每条命令恰好消费一个回答;
// 异步通知与未请求的回答单独上报, 不占用下一条命令的回答位置。
// 每条命令的超时只是活性保护, 不参与帧边界判定。
export class Session {
  #socket; #parser; #pending = null; #queue = Promise.resolve(); #abortReason = null;
  static async connect(endpoint, {onNotify, onUnsolicited, connectTimeoutMs = 3000} = {}) {
    const socket = await new Promise((resolve, reject) => {
      const sock = net.connect(endpoint.port, endpoint.host);
      const timer = setTimeout(() => { sock.destroy(); reject(new DisconnectError('连接仪器网关超时', {kind: 'connect', receivedBytes: 0})); }, connectTimeoutMs);
      const onError = error => { clearTimeout(timer); reject(error); };
      sock.once('error', onError);
      sock.once('connect', () => { clearTimeout(timer); sock.removeListener('error', onError); resolve(sock); });
    });
    return new Session(socket, {onNotify, onUnsolicited});
  }
  constructor(socket, {onNotify, onUnsolicited} = {}) {
    this.#socket = socket;
    this.#parser = createGatewayParser({
      onLine: text => { if (this.#pending) this.#pending.settle(null, {type: 'line', text}); else onUnsolicited?.({type: 'line', text}); },
      onBlock: (payload, meta) => { if (this.#pending) this.#pending.settle(null, {type: 'block', payload, ...meta}); else onUnsolicited?.({type: 'block', ...meta}); },
      onNotify: text => onNotify?.(text)
    });
    socket.on('data', chunk => {
      try { this.#parser.feed(chunk); } catch (error) { this.#fail(error); }
    });
    socket.on('error', () => {});
    socket.on('close', () => this.#fail(new DisconnectError(this.#abortReason ?? '连接在命令完成前关闭', this.#parser.progress())));
  }
  #fail(error) {
    if (this.#pending) this.#pending.settle(error);
    if (!this.#socket.destroyed) this.#socket.destroy();
  }
  command(line, {timeoutMs = 3000} = {}) {
    const result = this.#queue.then(() => this.#roundTrip(line, timeoutMs));
    this.#queue = result.catch(() => {});
    return result;
  }
  #roundTrip(line, timeoutMs) {
    return new Promise((resolve, reject) => {
      if (this.#socket.destroyed) return reject(new DisconnectError(this.#abortReason ?? '连接已关闭', this.#parser.progress()));
      const timer = setTimeout(() => {
        const error = new ProtocolError(`命令回答超时: ${line}`, this.#parser.progress());
        this.#pending = null;
        this.#socket.destroy(); // 超时后帧边界不再可信, 放弃连接而不是猜边界
        reject(error);
      }, timeoutMs);
      this.#pending = {settle: (error, value) => { clearTimeout(timer); this.#pending = null; error ? reject(error) : resolve(value); }};
      try { this.#socket.write(line + '\n'); } catch (error) { this.#pending?.settle(error); }
    });
  }
  progress() { return this.#parser.progress(); }
  get abortReason() { return this.#abortReason; }
  abort(reason) { this.#abortReason = reason; this.#socket.destroy(); }
  async close() {
    if (this.#socket.destroyed) return;
    this.#socket.end();
    await new Promise(resolve => { this.#socket.once('close', resolve); setTimeout(resolve, 500).unref(); });
  }
}

// 设备锁: 同一时刻只允许一个作业做"配置+触发+读取"序列。
// 租约到期自动释放, 停止采集也会释放, 设备不会被永远锁住。
export class DeviceLock {
  #holder = null;
  acquire(owner, ttlMs = 30000) {
    const now = Date.now();
    if (this.#holder && this.#holder.expiresAt > now) {
      if (this.#holder.owner === owner) { this.#holder.expiresAt = now + ttlMs; return {owner, expiresAt: this.#holder.expiresAt}; }
      throw new BusyError(`设备正被作业 ${this.#holder.owner} 占用(租约至 ${new Date(this.#holder.expiresAt).toISOString()})`);
    }
    this.#holder = {owner, expiresAt: now + ttlMs};
    return {owner, expiresAt: this.#holder.expiresAt};
  }
  renew(owner, ttlMs = 30000) {
    const now = Date.now();
    if (!this.#holder || this.#holder.owner !== owner || this.#holder.expiresAt <= now) throw new BusyError(`作业 ${owner} 不持有设备锁`);
    this.#holder.expiresAt = now + ttlMs;
  }
  release(owner) { if (this.#holder?.owner === owner) this.#holder = null; }
  forceRelease() { this.#holder = null; }
  status() {
    const now = Date.now();
    if (!this.#holder || this.#holder.expiresAt <= now) return {held: false};
    return {held: true, owner: this.#holder.owner, expiresAt: new Date(this.#holder.expiresAt).toISOString()};
  }
}

// 审计日志: 每次状态迁移追加一份组快照, 重启后按 id 折叠恢复。
class Journal {
  constructor(path) { this.path = path; }
  async load() {
    const groups = new Map();
    if (!this.path) return groups;
    let text = '';
    try { text = await readFile(this.path, 'utf8'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      try {
        const event = JSON.parse(line);
        if (event.type === 'snapshot' && event.group?.id) groups.set(event.group.id, event.group);
      } catch { /* 跳过损坏行, 不让一条坏记录拖垮恢复 */ }
    }
    return groups;
  }
  async append(group) {
    if (!this.path) return;
    await mkdir(dirname(this.path), {recursive: true});
    await appendFile(this.path, JSON.stringify({type: 'snapshot', at: new Date().toISOString(), group}) + '\n');
  }
}

// 成组采集服务: 一组读数绑定本组确认过的通道配置与单位。
// 组状态机: created → configured → pending(已触发,结果待确认) → completed(校验通过,对下游开放)
//                                    ↘ failed(终态,异常已记录)
export class AcquisitionService {
  #endpoint; #journal; #groups = new Map(); #lock = new DeviceLock(); #active = null;
  #lockTtlMs; #commandTimeoutMs; #counter = 0;
  constructor({endpoint, journalPath = '.local/acquisition-journal.jsonl', lockTtlMs = 30000, commandTimeoutMs = 3000} = {}) {
    if (!endpoint?.port) throw new ClientInputError('缺少仪器网关地址');
    this.#endpoint = endpoint;
    this.#journal = new Journal(journalPath);
    this.#lockTtlMs = lockTtlMs;
    this.#commandTimeoutMs = commandTimeoutMs;
  }
  async open() {
    const loaded = await this.#journal.load();
    for (const [id, group] of loaded) this.#groups.set(id, group);
    this.#counter = [...this.#groups.keys()].reduce((max, id) => {
      const match = /^grp-(\d+)$/.exec(id);
      return match ? Math.max(max, Number(match[1])) : max;
    }, 0);
    return this;
  }
  #newGroup({requestedUnit, owner}) {
    this.#counter += 1;
    const now = new Date().toISOString();
    return {
      id: `grp-${String(this.#counter).padStart(4, '0')}`, owner, requestedUnit,
      state: 'created', measurement: 'none',
      basis: {identity: null, generation: null, confirmedUnit: null, triggerId: null},
      blocks: [], progress: [], notifications: [], exceptions: [],
      readings: null, released: false, createdAt: now, updatedAt: now
    };
  }
  async #commit(group) {
    group.measurement = group.state === 'completed' ? 'completed' : group.state === 'pending' ? 'pending' : 'none';
    group.updatedAt = new Date().toISOString();
    this.#groups.set(group.id, group);
    await this.#journal.append(group);
  }
  #mark(group, stage, detail) {
    group.progress.push({stage, detail, at: new Date().toISOString()});
  }
  async #connect(group) {
    return Session.connect(this.#endpoint, {
      connectTimeoutMs: this.#commandTimeoutMs,
      onNotify: text => { group.notifications.push(text); },
      onUnsolicited: item => { group.exceptions.push({stage: 'protocol', message: '收到未请求的回答', detail: item, at: new Date().toISOString()}); }
    });
  }
  async #queryLine(session, line) {
    const response = await session.command(line, {timeoutMs: this.#commandTimeoutMs});
    if (response.type !== 'line') throw new ProtocolError(`命令 ${line} 收到非文本回答`, session.progress());
    if (response.text.startsWith('ERR')) throw new InstrumentError(`仪器拒绝 ${line}: ${response.text}`);
    return response.text;
  }
  async #queryBlock(session, line) {
    const response = await session.command(line, {timeoutMs: this.#commandTimeoutMs});
    if (response.type === 'line' && response.text.startsWith('ERR')) throw new InstrumentError(`仪器拒绝 ${line}: ${response.text}`);
    if (response.type !== 'block') throw new ProtocolError(`命令 ${line} 未返回定长块`, session.progress());
    return response;
  }
  async #queryStat(session) {
    const text = await this.#queryLine(session, 'STAT?');
    let stat;
    try { stat = JSON.parse(text); } catch { throw new ProtocolError(`STAT? 回答不是 JSON: ${text}`, session.progress()); }
    return {generation: stat.generation ?? null, trigger: stat.trigger ?? null, unit: stat.unit ?? null};
  }
  // 校验仪器侧记录与本组确认依据一致: 世代未变(未复位)、触发号未被覆盖、触发时单位就是本组确认单位
  #validateStat(group, stat) {
    if (stat.generation !== group.basis.generation) throw new GroupValidationError(`设备世代由 ${group.basis.generation} 变为 ${stat.generation}(已复位), 本组测量结果已丢失`);
    if (stat.trigger !== group.basis.triggerId) throw new GroupValidationError(`触发序号不一致: 本组 ${group.basis.triggerId}, 仪器当前 ${stat.trigger}, 结果可能已被后续触发覆盖`);
    if (stat.unit !== group.basis.confirmedUnit) throw new GroupValidationError(`触发时单位 ${stat.unit ?? '未知'} 与本组确认单位 ${group.basis.confirmedUnit} 不一致, 拒绝标注读数`);
  }
  async #fetchAndValidate(group, session) {
    this.#mark(group, 'fetch', '请求读数块 FETCH?');
    const block = await this.#queryBlock(session, 'FETCH?');
    group.blocks.push({
      header: block.header, length: block.payload.length,
      sha256: createHash('sha256').update(block.payload).digest('hex'),
      base64: block.payload.toString('base64'), receivedAt: new Date().toISOString()
    });
    this.#mark(group, 'fetch', `收到定长块: 头 ${block.header}, 负载 ${block.payload.length} 字节`);
    await this.#commit(group); // 原始块先落账, 再做校验
    const stat = await this.#queryStat(session);
    this.#validateStat(group, stat);
    if (block.payload.length !== 16) throw new GroupValidationError(`读数块长度 ${block.payload.length} 不是 2 个 float64(16 字节)`);
    group.readings = {values: [block.payload.readDoubleBE(0), block.payload.readDoubleBE(8)], unit: group.basis.confirmedUnit, triggerId: group.basis.triggerId};
    this.#mark(group, 'validate', '块长度、世代、触发号、单位校验通过');
  }
  // 续租; 若停止采集已强制放锁, 以停止原因代替锁异常, 让值班看到真实起因
  #renew(group, session) {
    try { this.#lock.renew(group.id, this.#lockTtlMs); } catch (error) {
      if (session.abortReason) throw new DisconnectError(session.abortReason, session.progress());
      throw error;
    }
  }
  async #recordFailure(group, stage, error, session) {
    group.exceptions.push({stage, message: error.message, progress: error.progress ?? session?.progress() ?? null, at: new Date().toISOString()});
    const triggered = Number.isInteger(group.basis?.triggerId);
    // 校验失败是终态; 已触发但结果未读完保持"待确认", 可恢复; 未触发则没有测量
    group.state = (error instanceof GroupValidationError || !triggered) ? 'failed' : 'pending';
    await this.#commit(group);
  }
  // 完整执行一组采集。设备被占用时抛 BusyError, 不排队穿插。
  // 采集层面的失败不抛出, 而是记录在组里(状态/异常/进度), 返回组记录。
  async runGroup({unit, owner = 'acquisition', hooks = {}} = {}) {
    if (typeof unit !== 'string' || !/^[^\r\n]{1,32}$/.test(unit.trim())) throw new ClientInputError('单位参数无效');
    const group = this.#newGroup({requestedUnit: unit.trim(), owner});
    this.#lock.acquire(group.id, this.#lockTtlMs);
    await this.#commit(group);
    let session = null; let stage = 'connect';
    try {
      session = await this.#connect(group);
      this.#active = {groupId: group.id, session};
      this.#mark(group, 'connect', '已连接仪器网关');
      stage = 'identify';
      const identity = await this.#queryLine(session, '*IDN?');
      stage = 'basis';
      const basisStat = await this.#queryStat(session);
      group.basis.identity = identity;
      group.basis.generation = basisStat.generation;
      this.#mark(group, 'basis', `配置依据: 型号 ${identity}, 世代 ${basisStat.generation}`);
      await this.#commit(group);
      stage = 'configure';
      await this.#queryLine(session, `CONF:UNIT ${group.requestedUnit}`);
      group.basis.confirmedUnit = group.requestedUnit;
      group.state = 'configured';
      this.#mark(group, 'configure', `单位 ${group.requestedUnit} 已确认`);
      await this.#commit(group);
      if (hooks.afterConfirm) await hooks.afterConfirm(structuredClone(group), session);
      this.#renew(group, session);
      stage = 'trigger';
      const ok = await this.#queryLine(session, 'INIT');
      const triggerId = Number(/^OK (\d+)$/.exec(ok)?.[1]);
      if (!Number.isInteger(triggerId)) throw new ProtocolError(`INIT 回答无法解析: ${ok}`, session.progress());
      group.basis.triggerId = triggerId;
      group.state = 'pending'; // 触发已发生而结果未读完: 先落账再取数
      this.#mark(group, 'trigger', `触发已确认, 序号 ${triggerId}`);
      await this.#commit(group);
      if (hooks.afterTrigger) await hooks.afterTrigger(structuredClone(group), session);
      this.#renew(group, session);
      stage = 'fetch';
      await this.#fetchAndValidate(group, session);
      group.state = 'completed';
      group.released = true;
      await this.#commit(group);
    } catch (error) {
      await this.#recordFailure(group, stage, error, session);
    } finally {
      if (this.#active?.groupId === group.id) this.#active = null;
      this.#lock.release(group.id);
      if (session) await session.close().catch(() => {});
    }
    return group;
  }
  // 断线恢复: 只补读未读完的结果, 绝不重发触发命令, 不把两次测量合为一次。
  async recoverGroup(id) {
    const group = this.#requireGroup(id);
    if (group.state !== 'pending') throw new StateConflictError(`组 ${id} 状态为 ${group.state}, 没有待恢复的结果`);
    this.#lock.acquire(group.id, this.#lockTtlMs);
    let session = null; let stage = 'connect';
    try {
      session = await this.#connect(group);
      this.#active = {groupId: group.id, session};
      stage = 'stat';
      const stat = await this.#queryStat(session);
      this.#mark(group, 'recover', `恢复核对: 世代 ${stat.generation}, 触发号 ${stat.trigger}, 单位 ${stat.unit ?? '未知'}`);
      this.#validateStat(group, stat); // 不重发 INIT; 世代变/被覆盖/单位不符都判为不可恢复
      stage = 'fetch';
      await this.#fetchAndValidate(group, session);
      group.state = 'completed';
      group.released = true;
      await this.#commit(group);
    } catch (error) {
      await this.#recordFailure(group, stage, error, session);
    } finally {
      if (this.#active?.groupId === group.id) this.#active = null;
      this.#lock.release(group.id);
      if (session) await session.close().catch(() => {});
    }
    return group;
  }
  // 停止采集: 中止在途命令并放开设备锁, 设备不会被永远锁住。
  async stop() {
    const active = this.#active;
    if (active) active.session.abort('采集已停止');
    this.#lock.forceRelease();
  }
  async close() { await this.stop(); }
  #requireGroup(id) {
    const group = this.#groups.get(id);
    if (!group) throw new NotFoundError(`组 ${id} 不存在`);
    return group;
  }
  get(id) { return this.#requireGroup(id); }
  list() {
    return [...this.#groups.values()].map(group => ({
      id: group.id, owner: group.owner, state: group.state, measurement: group.measurement,
      requestedUnit: group.requestedUnit, confirmedUnit: group.basis?.confirmedUnit ?? null,
      triggerId: group.basis?.triggerId ?? null, released: group.released,
      exceptions: group.exceptions.length, updatedAt: group.updatedAt
    }));
  }
  // 只有读数完整并校验通过的组才对下游开放
  result(id) {
    const group = this.#requireGroup(id);
    if (group.state !== 'completed' || !group.released) {
      const last = group.exceptions.at(-1);
      throw new StateConflictError(`组 ${id} 的读数未开放: 测量状态=${MEASUREMENT_LABELS[group.measurement]}, 最近异常=${last ? `[${last.stage}] ${last.message}` : '无'}`);
    }
    return {
      id: group.id, readings: group.readings.values, unit: group.readings.unit, triggerId: group.readings.triggerId,
      basis: group.basis, blocks: group.blocks.map(({header, length, sha256}) => ({header, length, sha256}))
    };
  }
  lockStatus() { return this.#lock.status(); }
}
