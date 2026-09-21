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

// ---------------------------------------------------------------------------
// 安全撤销测试辅助：独立房间，避免干扰主流程
// ---------------------------------------------------------------------------

async function createRoom(nick) {
  const created = await postJSON('/api/rooms', { nick: nick || '主持' });
  assert.strictEqual(created.status, 200);
  return created.body;
}

async function hostJoin(created, nick) {
  const c = new Client();
  await c.open();
  c.send({
    type: 'join',
    room: created.room,
    nick: nick || '主持',
    participantId: created.participantId,
    hostToken: created.hostToken
  });
  await c.waitFor((x) => x.joined && x.joined.you.host === true);
  return c;
}

async function guestJoin(code, nick) {
  const c = new Client();
  await c.open();
  c.send({ type: 'join', room: code, nick });
  await c.waitFor((x) => x.joined && x.joined.you.nick === nick);
  return c;
}

/** 主持人执行一条版本化命令并等待版本推进 */
async function hostCmd(host, cmd) {
  const v = host.v;
  host.lastError = null;
  host.send(Object.assign({ version: host.v }, cmd));
  await host.waitFor((c) => c.v === v + 1 || (c.lastError && c.lastError.code === 'STALE_VERSION'));
}

/**
 * 等待房间版本稳定：host 的 join 提交会在 guest join 之后再广播一个快照，
 * guest 需等到该快照到达，否则其首条命令会被判为 STALE_VERSION。
 */
async function waitStable(host, clients, ms = 150) {
  const v = host.v;
  await new Promise((r) => setTimeout(r, ms));
  for (const c of clients) {
    await c.waitFor((x) => x.v === host.v && x.v >= v);
  }
}

/** 以 guest 视角发送版本化命令：先同步到主持人的最新版本，避免 STALE_VERSION 干扰断言 */
async function guestSend(host, guest, cmd) {
  await waitStable(host, [guest]);
  guest.lastError = null;
  guest.send(Object.assign({ version: guest.v }, cmd));
}

async function waitForUndo(host, predicate) {
  await host.waitFor((c) => c.snapshot.undo && predicate(c.snapshot.undo));
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

  // ===========================================================================
  // 安全撤销（四类逆操作 + 介入变更边界）
  // ===========================================================================

  // 16. reorder 逆操作：恢复拖动前队列
  {
    const created = await createRoom('撤销主持');
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿甲');
    const g2 = await guestJoin(created.room, '阿乙');
    const g3 = await guestJoin(created.room, '阿丙');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const id3 = g3.joined.you.id;

    await waitStable(h, [g1, g2, g3]);
    g1.send({ type: 'request_speak', version: g1.v });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    g2.send({ type: 'request_speak', version: g2.v });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    g3.send({ type: 'request_speak', version: g3.v });
    await h.waitFor((c) => c.snapshot.order.length === 3);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3]);

    const vBefore = h.v;
    await hostCmd(h, { type: 'reorder', order: [id3, id1, id2] });
    assert.deepStrictEqual(h.snapshot.order, [id3, id1, id2]);
    // 快照展示可撤销操作摘要
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'reorder');
    assert.strictEqual(h.snapshot.undo.version, vBefore + 1, '撤销锚点为操作提交后的版本');
    assert.match(h.snapshot.undo.summary, /顺序/);
    assert.strictEqual(h.snapshot.undo.reason, null);

    // 撤销 -> 队列原子恢复，undo 清空，追加 audit 记录
    const vUndo = h.v;
    h.send({ type: 'undo_host', version: vUndo });
    await h.waitFor((c) => c.v === vUndo + 1);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2, id3]);
    assert.strictEqual(h.snapshot.undo, null);
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'audit' && /恢复.*顺序/.test(l.text)),
      '撤销需追加可见审计记录');

    // 撤销后再次撤销 -> UNDO_NONE，版本不变
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_NONE');
    assert.ok(h.lastError.snapshot, '错误应附带最新快照');

    // 拖动排序后出现介入变更（新成员申请发言）-> 撤销立即失效
    await hostCmd(h, { type: 'reorder', order: [id2, id3, id1] });
    await waitForUndo(h, (u) => u.status === 'available');
    const staleV = h.v;
    await guestSend(h, g1, { type: 'cancel_speak' });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.op, 'reorder');
    assert.strictEqual(h.snapshot.undo.reason, 'queue_changed');
    assert.ok(h.snapshot.undo.reasonText, '失效需带可读原因');

    // 失效后撤销 -> UNDO_INVALIDATED，不产生新版本
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');
    assert.strictEqual(h.lastError.snapshot.version, staleV + 1);
    assert.strictEqual(h.v, staleV + 1, '失效撤销不得改变版本');

    // 用过期版本撤销 -> STALE_VERSION + 最新快照
    h.lastError = null;
    h.send({ type: 'undo_host', version: staleV - 1 });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'STALE_VERSION');
    assert.strictEqual(h.lastError.snapshot.version, h.snapshot.version);

    // 非主持人不能撤销
    await guestSend(h, g2, { type: 'undo_host' });
    await g2.waitFor((c) => c.lastError && c.lastError.code === 'FORBIDDEN');

    await h.close(); await g1.close(); await g2.close(); await g3.close();
  }

  // 17. remove 逆操作：恢复队列位置；正在发言的成员恢复发言且 startedAt 保留
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿猫');
    const g2 = await guestJoin(created.room, '阿狗');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;

    await waitStable(h, [g1, g2]);
    g1.send({ type: 'request_speak', version: g1.v });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    g2.send({ type: 'request_speak', version: g2.v });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 移出队首 -> 撤销恢复到原下标
    await hostCmd(h, { type: 'remove', participantId: id1 });
    assert.deepStrictEqual(h.snapshot.order, [id2]);
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'remove');
    assert.match(h.snapshot.undo.summary, /阿猫/);
    // 成员席位与在线状态不受影响
    const p1 = h.snapshot.participants.find((p) => p.id === id1);
    assert.ok(p1, '移出队列不得删除成员席位');

    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2], '应恢复到原队列位置');
    assert.strictEqual(h.snapshot.undo, null);

    // 移出正在发言者 -> 撤销恢复发言，startedAt 不变
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const startedAt = h.snapshot.speaking.startedAt;
    assert.ok(h.snapshot.speaking && h.snapshot.speaking.id === id1);
    await hostCmd(h, { type: 'remove', participantId: id1 });
    assert.strictEqual(h.snapshot.speaking, null);
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.startedAt, startedAt, '撤销恢复发言须保留原计时起点');

    // 移出后有新介入（他人申请）-> 撤销失效
    await hostCmd(h, { type: 'remove', participantId: id2 });
    assert.deepStrictEqual(h.snapshot.order, []);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'queue_changed');
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');
    assert.deepStrictEqual(h.snapshot.order, [id2], '失效撤销不得改动队列');

    await h.close(); await g1.close(); await g2.close();
  }

  // 18. start_speaking 逆操作：发言人回到原队列位置，并恢复之前的发言人
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿东');
    const g2 = await guestJoin(created.room, '阿西');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;

    await waitStable(h, [g1, g2]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 请队首发言：从队列移除
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    assert.deepStrictEqual(h.snapshot.order, [id2]);
    assert.strictEqual(h.snapshot.speaking.id, id1);
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'start_speaking');

    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking === null);
    assert.deepStrictEqual(h.snapshot.order, [id1, id2], '撤销后发言人回到原队列位置');
    assert.strictEqual(h.snapshot.undo, null);

    // 带「切换发言人」语义：id1 发言中，请 id2 发言（自动结束 id1），撤销应恢复 id1 与计时
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const firstStartedAt = h.snapshot.speaking.startedAt;
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id2 });
    assert.strictEqual(h.snapshot.speaking.id, id2);
    assert.deepStrictEqual(h.snapshot.order, []);
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.startedAt, firstStartedAt, '原发言人计时连续');
    assert.deepStrictEqual(h.snapshot.order, [id2], '新发言人回到原队列位置');

    await h.close(); await g1.close(); await g2.close();
  }

  // 19. end_speaking 逆操作：恢复当前发言，startedAt 保持连续
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿南');
    const id1 = g1.joined.you.id;

    await waitStable(h, [g1]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'start_speaking', participantId: id1 });
    const startedAt = h.snapshot.speaking.startedAt;

    await hostCmd(h, { type: 'end_speaking' });
    assert.strictEqual(h.snapshot.speaking, null);
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'end_speaking');

    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.speaking && c.snapshot.speaking.id === id1);
    assert.strictEqual(h.snapshot.speaking.startedAt, startedAt, '恢复发言须沿用原 startedAt');
    assert.ok(h.snapshot.logs.some((l) => l.kind === 'audit' && /计时连续/.test(l.text)));

    await h.close(); await g1.close();
  }

  // 20. 介入变更边界：仅保留最近一次主持人操作；成员/换位/清空/一键清空均使撤销失效
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿春');
    const g2 = await guestJoin(created.room, '阿夏');
    const g3 = await guestJoin(created.room, '阿秋');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const id3 = g3.joined.you.id;

    await waitStable(h, [g1, g2, g3]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await guestSend(h, g3, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 3);

    // 连续两次主持人操作：只保留最近一次（旧的不可再撤销）
    await hostCmd(h, { type: 'reorder', order: [id2, id1, id3] });
    await waitForUndo(h, (u) => u.status === 'available' && u.op === 'reorder');
    await hostCmd(h, { type: 'reorder', order: [id2, id3, id1] });
    assert.strictEqual(h.snapshot.undo.op, 'reorder');
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.undo === null);
    assert.deepStrictEqual(h.snapshot.order, [id2, id1, id3], '撤销只能回退到最近一次操作之前');
    assert.notDeepStrictEqual(h.snapshot.order, [id1, id2, id3], '不允许连续撤销多步');

    // 成员加入/断线/离开均属于版本化状态变更，撤销立即失效
    await hostCmd(h, { type: 'reorder', order: [id1, id3, id2] });
    await waitForUndo(h, (u) => u.status === 'available');
    const g4 = await guestJoin(created.room, '阿冬');
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'presence_changed');
    await g4.close();
    // 等待服务端处理完断线提交后再继续，避免主持人命令踩中过期版本
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.nick === '阿冬');
      return p && p.online === false;
    });

    // 换位状态变更同样使撤销失效
    await hostCmd(h, { type: 'reorder', order: [id3, id1, id2] });
    await waitForUndo(h, (u) => u.status === 'available');
    await guestSend(h, g1, { type: 'swap_invite', participantId: id2 });
    await h.waitFor((c) => c.snapshot.undo && c.snapshot.undo.status === 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'swap_changed');

    // 一键清空使撤销立即失效并注明原因（clear 本身不可撤销）
    await hostCmd(h, { type: 'reorder', order: [id3, id2, id1] });
    await waitForUndo(h, (u) => u.status === 'available');
    await hostCmd(h, { type: 'clear' });
    assert.strictEqual(h.snapshot.undo.status, 'invalid', '一键清空使撤销失效');
    assert.strictEqual(h.snapshot.undo.reason, 'cleared');
    h.lastError = null;
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.lastError && c.lastError.code === 'UNDO_INVALIDATED');

    // 清空记录（日志变更）也使挂起撤销失效
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await hostCmd(h, { type: 'remove', participantId: id1 });
    await waitForUndo(h, (u) => u.status === 'available');
    await hostCmd(h, { type: 'clear_logs' });
    assert.strictEqual(h.snapshot.undo.status, 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'logs_cleared');

    await h.close(); await g1.close(); await g2.close(); await g3.close();
  }

  // 21. 撤销边界：不回滚在线状态与身份凭证；审计记录独立可见
  {
    const created = await createRoom();
    const h = await hostJoin(created);
    const g1 = await guestJoin(created.room, '阿梅');
    const g2 = await guestJoin(created.room, '阿兰');
    const id1 = g1.joined.you.id;
    const id2 = g2.joined.you.id;
    const secret2 = g2.joined.you.secret;

    await waitStable(h, [g1, g2]);
    await guestSend(h, g1, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    await guestSend(h, g2, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);

    // 主持人移出 g2 后，g2 断线；撤销恢复队列位置但保持离线
    await hostCmd(h, { type: 'remove', participantId: id2 });
    assert.deepStrictEqual(h.snapshot.order, [id1]);
    await g2.close();
    await h.waitFor((c) => {
      const p = c.snapshot.participants.find((x) => x.id === id2);
      return p && p.online === false;
    });
    // 注意：断线本身是版本化变更，已使撤销失效，主持人无法借撤销回滚在线状态
    assert.strictEqual(h.snapshot.undo.status, 'invalid');
    assert.strictEqual(h.snapshot.undo.reason, 'presence_changed');
    // 身份凭证仍然有效：g2 可凭原 id+secret 重连，席位仍在
    const g2b = new Client();
    await g2b.open();
    g2b.send({ type: 'join', room: created.room, nick: '阿兰', participantId: id2, secret: secret2 });
    await g2b.waitFor((c) => c.joined && c.joined.you.id === id2);
    const still = g2b.snapshot.participants.find((p) => p.id === id2);
    assert.ok(still && still.online === true);
    assert.ok(!g2b.snapshot.order.includes(id2), '失效撤销没有恢复队列位置');

    // 审计记录不参与撤销：成功撤销产生的 audit 日志在后续操作后依然保留
    // 构造一次成功撤销并验证审计独立
    await guestSend(h, g2b, { type: 'request_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 2);
    await hostCmd(h, { type: 'reorder', order: [id2, id1] });
    h.send({ type: 'undo_host', version: h.v });
    await h.waitFor((c) => c.snapshot.undo === null);
    const auditCount = h.snapshot.logs.filter((l) => l.kind === 'audit').length;
    await guestSend(h, g1, { type: 'cancel_speak' });
    await h.waitFor((c) => c.snapshot.order.length === 1);
    assert.strictEqual(
      h.snapshot.logs.filter((l) => l.kind === 'audit').length,
      auditCount,
      '后续变更不得删除既有审计记录'
    );

    await h.close(); await g1.close(); await g2b.close();
  }

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
