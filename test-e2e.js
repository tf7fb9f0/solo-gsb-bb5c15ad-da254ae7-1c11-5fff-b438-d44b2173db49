'use strict';

const assert = require('assert');
const http = require('http');
const path = require('path');
const { spawn } = require('child_process');
const WebSocket = require('ws');

const TEST_PORT = process.env.TEST_PORT || '3099';
const BASE = `http://127.0.0.1:${TEST_PORT}`;
let failures = 0;
let serverProc = null;

function test(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => console.log(`  ✅ ${name}`))
    .catch((err) => {
      failures++;
      console.error(`  ❌ ${name}: ${err.message}`);
      console.error(err.stack);
    });
}

function postJSON(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      BASE + path,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
      },
      (res) => {
        let buf = '';
        res.on('data', (c) => (buf += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(buf) });
          } catch (e) {
            reject(e);
          }
        });
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

class Client {
  constructor() {
    this.ws = new WebSocket('ws://127.0.0.1:'+TEST_PORT+'/ws');
    this.queue = [];
    this.snapshot = null;
    this.lastError = null;
    this.joined = null;
    this.ws.on('message', (raw) => {
      const msg = JSON.parse(raw);
      if (msg.type === 'snapshot') this.snapshot = msg.snapshot;
      if (msg.type === 'joined') {
        this.joined = msg;
        this.snapshot = msg.snapshot;
      }
      if (msg.type === 'error') this.lastError = msg;
      this.queue.push(msg);
    });
  }
  open() {
    return new Promise((res, rej) => {
      this.ws.on('open', res);
      this.ws.on('error', rej);
    });
  }
  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }
  close() {
    return new Promise((res) => {
      this.ws.on('close', res);
      this.ws.close();
    });
  }
  waitFor(fn, timeout = 2000, name = new Error().stack.split('\n')[2].trim()) {
    return new Promise((resolve, reject) => {
      const t = setInterval(() => {
        try {
          if (fn(this)) {
            clearInterval(t);
            clearTimeout(to);
            resolve();
          }
        } catch { /* keep waiting */ }
      }, 20);
      const to = setTimeout(() => {
        clearInterval(t);
        reject(new Error(`timeout at: ${name}`));
      }, timeout);
    });
  }
  get v() {
    return this.snapshot ? this.snapshot.version : null;
  }
}

async function main() {
  await startServer();

  // 1. 创建房间
  const created = await postJSON('/api/rooms', { nick: '张主持' });
  assert.strictEqual(created.status, 200);
  assert.match(created.body.room, /^\d{6}$/);
  const code = created.body.room;
  console.log(`  (房间码 ${code})`);

  // 2. 主持人加入
  const host = new Client();
  await host.open();
  host.send({ type: 'join', room: code, nick: '张主持', participantId: created.body.participantId, hostToken: created.body.hostToken });
  await host.waitFor((c) => c.joined && c.joined.you.host === true, 2000, 'host join');
  assert.strictEqual(host.snapshot.hostId, created.body.participantId);

  // 3. 参与者加入
  const alice = new Client();
  await alice.open();
  alice.send({ type: 'join', room: code, nick: '小明' });
  await alice.waitFor((c) => c.joined && c.joined.you.nick === '小明');
  const aliceId = alice.joined.you.id;
  assert.ok(alice.joined.you.secret, '新参与者应收到 secret');

  const bob = new Client();
  await bob.open();
  bob.send({ type: 'join', room: code, nick: '小红' });
  await bob.waitFor((c) => c.snapshot && c.snapshot.participants.length === 3);
  const bobId = bob.joined.you.id;

  // 4. 两人申请发言；版本原子递增
  const v0 = alice.v;
  alice.send({ type: 'request_speak', version: alice.v });
  await alice.waitFor((c) => c.v === v0 + 1);
  bob.send({ type: 'request_speak', version: bob.v });
  await bob.waitFor((c) => c.v === v0 + 2);
  await host.waitFor((c) => c.v === v0 + 2);
  assert.deepStrictEqual(host.snapshot.order, [aliceId, bobId]);

  // 5. 过期命令：用旧版本申请 -> STALE_VERSION 且携带最新快照
  alice.lastError = null;
  alice.send({ type: 'request_speak', version: v0 });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
  assert.strictEqual(alice.lastError.snapshot.version, host.snapshot.version, '过期命令必须返回最新快照');
  assert.deepStrictEqual(alice.lastError.snapshot.order, [aliceId, bobId]);

  // 6. 取消发言
  const v1 = bob.v;
  bob.send({ type: 'cancel_speak', version: v1 });
  await bob.waitFor((c) => c.v === v1 + 1);
  assert.deepStrictEqual(bob.snapshot.order, [aliceId]);

  // 7. 非主持人不能开始发言
  alice.lastError = null;
  alice.send({ type: 'start_speaking', version: alice.v, participantId: aliceId });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');
  assert.strictEqual(alice.v, v1 + 1, '被拒绝的命令不得改变版本');

  // 8. 主持人开始发言 -> 从队列移除，startedAt 计时
  const beforeStart = Date.now();
  host.send({ type: 'start_speaking', version: host.v, participantId: aliceId });
  await host.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === aliceId);
  assert.deepStrictEqual(host.snapshot.order, []);
  assert.ok(host.snapshot.speaking.startedAt >= beforeStart);
  assert.ok(Math.abs(host.snapshot.serverTs - Date.now()) < 1000);
  const startVersion = host.v;

  // 9. 重新排队并拖动排序（alice 正在发言，由 bob 和 carol 排队）
  const carol = new Client();
  await carol.open();
  carol.send({ type: 'join', room: code, nick: '小刚' });
  await carol.waitFor((c) => c.joined && c.joined.you.nick === '小刚');
  const carolId = carol.joined.you.id;

  bob.send({ type: 'request_speak', version: bob.v });
  await host.waitFor((c) => c.snapshot.order.length === 1);
  carol.send({ type: 'request_speak', version: carol.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  assert.deepStrictEqual(host.snapshot.order, [bobId, carolId]);

  // 非法 reorder：成员集合不一致
  host.lastError = null;
  host.send({ type: 'reorder', version: host.v, order: [carolId] });
  await host.waitFor((c) => c.lastError && c.lastError.code === 'ORDER_MISMATCH');

  // 合法 reorder
  host.lastError = null;
  host.send({ type: 'reorder', version: host.v, order: [carolId, bobId] });
  await host.waitFor((c) => c.snapshot.order[0] === carolId && c.v > startVersion + 1);
  assert.deepStrictEqual(host.snapshot.order, [carolId, bobId]);

  // 10. 断线重连：bob 断线期间保留席位与队列位置，重连后恢复一致顺序
  const bobSecret = bob.joined.you.secret;
  const orderBefore = host.snapshot.order;
  await bob.close();
  await host.waitFor((c) => {
    const p = c.snapshot.participants.find((x) => x.id === bobId);
    return p && p.online === false;
  });
  assert.deepStrictEqual(host.snapshot.order, orderBefore, '离线后队列位置保留');

  const bob2 = new Client();
  await bob2.open();
  bob2.send({ type: 'join', room: code, nick: '小红', participantId: bobId, secret: bobSecret });
  await bob2.waitFor((c) => c.joined && c.joined.you.id === bobId);
  assert.deepStrictEqual(bob2.snapshot.order, host.snapshot.order, '重连后顺序一致');
  assert.ok(bob2.snapshot.speaking, '重连后当前发言状态恢复');
  assert.strictEqual(bob2.snapshot.speaking.id, aliceId);
  const gap = Date.now() + (bob2.snapshot.serverTs - Date.now()) - bob2.snapshot.speaking.startedAt;
  assert.ok(gap > 0 && gap < 10000, `重连后可继续计时（gap=${gap}ms）`);

  // 11. 主持人结束发言
  host.send({ type: 'end_speaking', version: host.v });
  await host.waitFor((c) => c.snapshot.speaking === null);

  // 12. 一键清空（队列 + 发言 + 记录）
  // 队列中还有 bob、carol；让 bob 开始发言，制造 队列+发言+记录 数据
  host.send({ type: 'start_speaking', version: host.v, participantId: bobId });
  await host.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === bobId);
  assert.strictEqual(host.snapshot.order.length, 1, 'carol 仍在队列');
  assert.ok(host.snapshot.logs.length > 0);

  host.send({ type: 'clear', version: host.v });
  await host.waitFor((c) => c.snapshot.logs.length === 1); // 清空本身留一条系统记录
  assert.deepStrictEqual(host.snapshot.order, []);
  assert.strictEqual(host.snapshot.speaking, null);
  assert.match(host.snapshot.logs[0].text, /一键清空/);

  // 13. 操作记录类型完整
  const kinds = new Set(host.snapshot.logs.map((l) => l.kind));
  assert.ok(kinds.has('system'));
  // 此前记录里应有 talk / queue / presence（清空后只剩 system，但整个过程中曾出现）

  // ===========================================================================
  // 协商换位
  // ===========================================================================

  // 14a. 三人排队：dave -> alice -> bob
  const dave = new Client();
  await dave.open();
  dave.send({ type: 'join', room: code, nick: '小兰' });
  await dave.waitFor((c) => c.joined && c.joined.you.nick === '小兰');
  const daveId = dave.joined.you.id;

  dave.send({ type: 'request_speak', version: dave.v });
  await host.waitFor((c) => c.snapshot.order.length === 1);
  alice.send({ type: 'request_speak', version: alice.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  bob2.send({ type: 'request_speak', version: bob2.v });
  await host.waitFor((c) => c.snapshot.order.length === 3);
  assert.deepStrictEqual(host.snapshot.order, [daveId, aliceId, bobId]);

  // 14b. dave 向 bob 发起换位邀请：记录发起时版本与位置，随快照广播
  const inviteVersion = dave.v;
  dave.send({ type: 'swap_invite', version: inviteVersion, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === daveId && s.to === bobId));
  let swap = host.snapshot.swaps.find((s) => s.from === daveId && s.to === bobId);
  assert.strictEqual(swap.version, inviteVersion, '邀请应记录发起时的队列版本');
  assert.strictEqual(swap.fromPos, 0);
  assert.strictEqual(swap.toPos, 2);
  const swapId = swap.id;
  await bob2.waitFor((c) => c.snapshot.swaps.some((s) => s.id === swapId && s.status === 'pending'));

  // 14c. 每人同一时刻最多参与一条待处理邀请
  dave.lastError = null;
  dave.send({ type: 'swap_invite', version: dave.v, participantId: aliceId });
  await dave.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_BUSY');
  alice.lastError = null;
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_BUSY');

  // 不能邀请自己 / 不在队列中的人
  dave.lastError = null;
  dave.send({ type: 'swap_invite', version: dave.v, participantId: daveId });
  await dave.waitFor((c) => c.lastError && c.lastError.code === 'BAD_TARGET');
  dave.lastError = null;
  dave.send({ type: 'swap_invite', version: dave.v, participantId: carolId });
  await dave.waitFor((c) => c.lastError && c.lastError.code === 'TARGET_NOT_QUEUED');

  // 14d. 非目标方不能接受；目标方接受 -> 原子交换
  alice.lastError = null;
  alice.send({ type: 'swap_accept', version: alice.v, swapId });
  await alice.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_NOT_YOURS');
  assert.deepStrictEqual(host.snapshot.order, [daveId, aliceId, bobId], '被拒绝的接受不得改动队列');

  bob2.send({ type: 'swap_accept', version: bob2.v, swapId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swapId && s.status === 'accepted'));
  assert.deepStrictEqual(host.snapshot.order, [bobId, aliceId, daveId], '接受后双方位置交换');

  // 14e. 已结束的邀请不能重复操作
  bob2.lastError = null;
  bob2.send({ type: 'swap_accept', version: bob2.v, swapId });
  await bob2.waitFor((c) => c.lastError && c.lastError.code === 'SWAP_RESOLVED');

  // 14f. 发起邀请后被主持人重排 -> 邀请失效且队列保持为新顺序
  bob2.send({ type: 'swap_invite', version: bob2.v, participantId: aliceId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === bobId && s.to === aliceId));
  const swap2 = host.snapshot.swaps.find((s) => s.from === bobId && s.to === aliceId && s.status === 'pending');

  host.send({ type: 'reorder', version: host.v, order: [daveId, bobId, aliceId] });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap2.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap2.id).reason, 'reordered');
  assert.deepStrictEqual(host.snapshot.order, [daveId, bobId, aliceId]);

  // 14g. 一方取消申请 -> 相关待处理邀请失效
  dave.send({ type: 'swap_invite', version: dave.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === daveId && s.to === bobId));
  const swap3 = host.snapshot.swaps.find((s) => s.from === daveId && s.to === bobId && s.status === 'pending');
  dave.send({ type: 'cancel_speak', version: dave.v });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap3.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap3.id).reason, 'canceled_speak');

  // 14h. 一方开始发言 -> 相关待处理邀请失效（bob 邀请 alice，主持人请 bob 发言）
  assert.deepStrictEqual(host.snapshot.order, [bobId, aliceId], 'dave 取消后队列剩 bob、alice');
  bob2.send({ type: 'swap_invite', version: bob2.v, participantId: aliceId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === bobId && s.to === aliceId));
  const swap4 = host.snapshot.swaps.find((s) => s.from === bobId && s.to === aliceId && s.status === 'pending');
  host.send({ type: 'start_speaking', version: host.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap4.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap4.id).reason, 'started_speaking');

  // 14i. 一方主动离开 -> 相关待处理邀请失效（alice 邀请已归队的 dave，随后 dave 离开）
  host.send({ type: 'end_speaking', version: host.v });
  await host.waitFor((c) => c.snapshot.speaking === null);
  dave.send({ type: 'request_speak', version: dave.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  alice.send({ type: 'swap_invite', version: alice.v, participantId: daveId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === daveId));
  const swap5 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === daveId && s.status === 'pending');
  dave.send({ type: 'leave' });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap5.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap5.id).reason, 'left');

  // 14j. 拒绝流程：alice 邀请 bob，bob 拒绝，队列不变
  assert.deepStrictEqual(host.snapshot.order, [aliceId]);
  bob2.send({ type: 'request_speak', version: bob2.v });
  await host.waitFor((c) => c.snapshot.order.length === 2);
  assert.deepStrictEqual(host.snapshot.order, [aliceId, bobId]);
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === bobId));
  const swap6 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === bobId && s.status === 'pending');
  bob2.send({ type: 'swap_reject', version: bob2.v, swapId: swap6.id });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap6.id && s.status === 'rejected'));
  assert.deepStrictEqual(host.snapshot.order, [aliceId, bobId], '拒绝后队列不变');

  // 14k. 撤回流程：alice 撤回邀请
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === bobId));
  const swap7 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === bobId && s.status === 'pending');
  alice.send({ type: 'swap_cancel', version: alice.v, swapId: swap7.id });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap7.id && s.status === 'cancelled'));

  // 14l. 被主持人移除 -> 相关待处理邀请失效，收尾队列
  alice.send({ type: 'swap_invite', version: alice.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.status === 'pending' && s.from === aliceId && s.to === bobId));
  const swap8 = host.snapshot.swaps.find((s) => s.from === aliceId && s.to === bobId && s.status === 'pending');
  host.send({ type: 'remove', version: host.v, participantId: bobId });
  await host.waitFor((c) =>
    c.snapshot.swaps.some((s) => s.id === swap8.id && s.status === 'invalid'));
  assert.strictEqual(
    host.snapshot.swaps.find((s) => s.id === swap8.id).reason, 'removed');

  // 14m. 接受不存在的邀请 -> NO_SWAP；防御性的位置重校验由各联动失效场景覆盖
  // （协议层面：任何能改变参与方位置的队列操作都会先联动失效邀请）
  bob2.lastError = null;
  bob2.send({ type: 'swap_accept', version: bob2.v, swapId: 'sw_not_exist' });
  await bob2.waitFor((c) => c.lastError && c.lastError.code === 'NO_SWAP');

  await dave.close();

  // 15. 房间不存在
  const ghost = new Client();
  await ghost.open();
  ghost.send({ type: 'join', room: '999999', nick: '幽灵' });
  await ghost.waitFor((c) => c.lastError && c.lastError.code === 'NO_ROOM');

  await host.close();
  await alice.close();
  await bob2.close();
  await carol.close();
  await ghost.close();
  console.log('\n全部测试通过 🎉');
}

function startServer() {
  return new Promise((resolve, reject) => {
    serverProc = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
      env: { ...process.env, PORT: TEST_PORT },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let settled = false;
    let output = '';
    const onData = (d) => {
      output += String(d);
      if (output.includes('已启动') && !settled) {
        settled = true;
        setTimeout(resolve, 100);
      }
    };
    serverProc.stdout.on('data', onData);
    serverProc.stderr.on('data', onData);
    serverProc.on('error', (e) => !settled && (settled = true, reject(e)));
    serverProc.on('exit', () => {
      if (!settled) {
        settled = true;
        reject(new Error(`server exited early:\n${output}`));
      }
    });
  });
}

main()
  .then(() => {
    if (serverProc) serverProc.kill();
    process.exit(0);
  })
  .catch((e) => {
    console.error(e);
    if (serverProc) serverProc.kill();
    process.exit(1);
  });
