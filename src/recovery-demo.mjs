import net from 'node:net';
import {mkdtemp} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createSource} from './source.mjs';
import {AcquisitionService, MEASUREMENT_LABELS} from './acquire.mjs';

// 恢复行为演示: 用网关的复位、截断回答与异步通知开关展示成组采集的恢复语义。
// 不接真实仪器, 不做波形分析; 仪器由本地来源模拟。
const source = await createSource();
const journalPath = join(await mkdtemp(join(tmpdir(), 'acq-demo-')), 'journal.jsonl');
const service = await new AcquisitionService({endpoint: source.state.endpoint, journalPath}).open();

const control = async body => (await fetch(source.url + '/control', {method: 'POST', body: JSON.stringify(body)})).json();
function show(group, note = '') {
  const last = group.exceptions.at(-1);
  console.log(`  ${group.id} 测量状态=${MEASUREMENT_LABELS[group.measurement]}(${group.state}) 单位=${group.basis?.confirmedUnit ?? group.requestedUnit} 触发号=${group.basis?.triggerId ?? '-'} 读数=${group.readings ? JSON.stringify(group.readings.values) : '-'}${last ? ` 最近异常=[${last.stage}] ${last.message}` : ''}${note}`);
}
// 模拟同事从另一条连接操作仪器: 逐行发命令, 等够回答行数再断开
async function colleague(lines, responses = lines.length) {
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

try {
  console.log('场景1: 正常成组采集 —— 读数绑定本组确认过的配置与单位');
  show(await service.runGroup({unit: 'V', owner: 'demo'}));

  console.log('场景2: 网关开启异步通知 —— 通知单独记录, 不占用命令的回答位置');
  await control({notify: true});
  const notified = await service.runGroup({unit: 'V', owner: 'demo'});
  show(notified, ` 异步通知 ${notified.notifications.length} 条: ${notified.notifications.slice(0, 2).join(' ')} …`);
  await control({notify: false});

  console.log('场景3: FETCH 途中被网关截断 —— 触发已发生而结果未读完');
  await control({truncate: true});
  const truncated = await service.runGroup({unit: 'V', owner: 'demo'});
  show(truncated, ` 接收进度=${JSON.stringify(truncated.exceptions.at(-1)?.progress)}`);
  console.log('  → 值班判断: 结果待确认。排除截断后恢复, 不重发触发命令');
  await control({truncate: false});
  const recovered = await service.recoverGroup(truncated.id);
  show(recovered, ` 恢复后触发号仍为 ${recovered.basis.triggerId}, 仪器总触发数=${(await control({})).trigger}`);

  console.log('场景4: 待确认期间设备被复位 —— 结果不可恢复, 按"没有测量"处理');
  await control({truncate: true});
  const lost = await service.runGroup({unit: 'V', owner: 'demo'});
  show(lost);
  const state = await (await fetch(source.url + '/state')).json();
  await control({generation: state.generation + 1, trigger: 0, truncate: false});
  show(await service.recoverGroup(lost.id), ' 恢复判定: 世代已变, 不自动补发触发');

  console.log('场景5: 同事在本组确认单位后、触发前改单位 —— 校验失败, 不把读数标错单位');
  show(await service.runGroup({unit: 'V', owner: 'demo', hooks: {afterConfirm: () => colleague(['CONF:UNIT MV'])}}));

  console.log('场景6: 同事在本组触发后只改单位(未再触发) —— 本组读数仍按确认单位');
  show(await service.runGroup({unit: 'V', owner: 'demo', hooks: {afterTrigger: () => colleague(['CONF:UNIT MV'])}}));

  console.log('场景7: 同事在本组触发后抢先触发 —— 结果归属不明, 不把两次测量合为一次');
  show(await service.runGroup({unit: 'V', owner: 'demo', hooks: {afterTrigger: () => colleague(['CONF:UNIT MV', 'INIT'], 2)}}));

  console.log('场景8: 停止采集 —— 放开设备锁, 不做永久占用');
  let hookEntered; const entered = new Promise(resolve => { hookEntered = resolve; });
  let releaseGate; const gate = new Promise(resolve => { releaseGate = resolve; });
  const stopped = service.runGroup({unit: 'V', owner: 'demo', hooks: {afterTrigger: () => { hookEntered(); return gate; }}});
  await entered;
  console.log(`  采集中锁状态: ${JSON.stringify(service.lockStatus())}`);
  await service.stop();
  console.log(`  停止后锁状态: ${JSON.stringify(service.lockStatus())}`);
  releaseGate();
  const stoppedGroup = await stopped;
  show(stoppedGroup, ' 被停止的组保持待确认, 可另行恢复');
  show(await service.runGroup({unit: 'V', owner: 'demo'}), ' 停止后新作业正常执行');
  show(await service.recoverGroup(stoppedGroup.id), ' 恢复时发现结果已被后续触发覆盖, 拒绝合并');

  console.log('汇总(值班视图):');
  for (const item of service.list()) {
    console.log(`  ${item.id} ${MEASUREMENT_LABELS[item.measurement]} state=${item.state} 单位=${item.confirmedUnit ?? item.requestedUnit} 触发号=${item.triggerId ?? '-'} 异常=${item.exceptions}`);
  }
  console.log(`审计日志(原始块/配置依据/进度/异常): ${journalPath}`);
} finally {
  await service.close();
  await source.close();
}
