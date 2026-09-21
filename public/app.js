/* global */
'use strict';

// ---------------------------------------------------------------------------
// 全局状态
// ---------------------------------------------------------------------------

const state = {
  snapshot: null, // 最近一次服务端快照
  version: 0,
  me: null,       // { id, nick, host, secret? }
  creds: null,    // localStorage 中持久化的身份
  ws: null,
  connected: false,
  intentionalClose: false,
  reconnectDelay: 500,
  reconnectTimer: null,
  clockOffset: 0, // 服务端时钟 - 本地时钟
  timerHandle: null,
  // 换位邀请：swapId -> 最近已知状态（用于检测状态变化并弹出反馈）
  swapSeeded: false,
  swapStatus: new Map(),
  // 安全撤销：最近一次已展示的撤销状态（用于检测可用/失效/已撤销的跳变）
  undoSeen: null
};

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => [...document.querySelectorAll(sel)];

// ---------------------------------------------------------------------------
// DOM
// ---------------------------------------------------------------------------

const els = {
  home: $('#view-home'),
  room: $('#view-room'),
  formCreate: $('#form-create'),
  createNick: $('#create-nick'),
  formJoin: $('#form-join'),
  joinRoom: $('#join-room'),
  joinNick: $('#join-nick'),
  roomCode: $('#room-code'),
  hostBadge: $('#host-badge'),
  versionBadge: $('#version-badge'),
  connDot: $('#conn-dot'),
  connText: $('#conn-text'),
  btnCopyLink: $('#btn-copy-link'),
  btnExit: $('#btn-exit'),
  stageIdle: $('#stage-idle'),
  stageActive: $('#stage-active'),
  stageNick: $('#stage-nick'),
  stageTimer: $('#stage-timer'),
  btnEnd: $('#btn-end'),
  queueList: $('#queue-list'),
  queueEmpty: $('#queue-empty'),
  queueCount: $('#queue-count'),
  undoBar: $('#undo-bar'),
  swapBar: $('#swap-bar'),
  btnRequest: $('#btn-request'),
  btnCancel: $('#btn-cancel'),
  logList: $('#log-list'),
  peopleList: $('#people-list'),
  peopleCount: $('#people-count'),
  btnClearAll: $('#btn-clear-all'),
  btnClearLogs: $('#btn-clear-logs'),
  toast: $('#toast-container')
};

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

function toast(message, kind = '') {
  const div = document.createElement('div');
  div.className = `toast ${kind}`;
  div.textContent = message;
  els.toast.appendChild(div);
  setTimeout(() => {
    div.style.opacity = '0';
    div.style.transition = 'opacity 0.3s';
    setTimeout(() => div.remove(), 300);
  }, 3200);
}

function storageKey(code) {
  return `speakqueue_room_${code}`;
}

function saveCreds() {
  if (!state.creds) return;
  localStorage.setItem(storageKey(state.creds.room), JSON.stringify(state.creds));
}

function clearCreds() {
  if (state.creds) localStorage.removeItem(storageKey(state.creds.room));
  state.creds = null;
}

function wsUrl() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${proto}://${location.host}/ws`;
}

function pad2(n) {
  return String(n).padStart(2, '0');
}

function formatDuration(ms) {
  if (ms < 0) ms = 0;
  const total = Math.floor(ms / 1000);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return h > 0 ? `${h}:${pad2(m)}:${pad2(s)}` : `${pad2(m)}:${pad2(s)}`;
}

function formatTime(ts) {
  const d = new Date(ts);
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
}

function serverNow() {
  return Date.now() + state.clockOffset;
}

// ---------------------------------------------------------------------------
// 视图切换
// ---------------------------------------------------------------------------

function showHome() {
  els.room.classList.add('hidden');
  els.home.classList.remove('hidden');
  document.body.classList.remove('role-host', 'role-participant');
}

function showRoom() {
  els.home.classList.add('hidden');
  els.room.classList.remove('hidden');
}

// ---------------------------------------------------------------------------
// 创建 / 加入（首页）
// ---------------------------------------------------------------------------

function prefillFromUrl() {
  const params = new URLSearchParams(location.search);
  const code = params.get('room');
  if (code) els.joinRoom.value = code.replace(/\D/g, '').slice(0, 6);
}
prefillFromUrl();

els.formCreate.addEventListener('submit', async (e) => {
  e.preventDefault();
  const nick = els.createNick.value.trim();
  try {
    const res = await fetch('/api/rooms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nick })
    });
    if (!res.ok) throw new Error(`创建失败（${res.status}）`);
    const data = await res.json();
    const creds = {
      room: data.room,
      id: data.participantId,
      nick: nick || '主持人',
      host: true,
      hostToken: data.hostToken
    };
    localStorage.setItem(storageKey(data.room), JSON.stringify(creds));
    enterRoom(creds);
  } catch (err) {
    toast(err.message || '创建房间失败', 'error');
  }
});

els.formJoin.addEventListener('submit', (e) => {
  e.preventDefault();
  const code = els.joinRoom.value.trim().replace(/\D/g, '').padStart(6, '0');
  const nick = els.joinNick.value.trim();
  if (code.length !== 6) return toast('请输入 6 位房间码', 'error');
  if (!nick) return toast('请输入昵称', 'error');

  // 若此浏览器存有该房间身份（含创建者 hostToken），直接恢复
  const stored = JSON.parse(localStorage.getItem(storageKey(code)) || 'null');
  if (stored) {
    stored.nick = nick;
    enterRoom(stored);
  } else {
    enterRoom({ room: code, nick, host: false });
  }
});

function enterRoom(creds) {
  state.creds = creds;
  state.me = null;
  state.snapshot = null;
  state.version = 0;
  state.swapSeeded = false;
  state.swapStatus = new Map();
  state.undoSeen = null;
  state.intentionalClose = false;
  state.reconnectDelay = 500;

  els.roomCode.textContent = creds.room;
  showRoom();
  connect();
}

// ---------------------------------------------------------------------------
// WebSocket 连接与自动重连
// ---------------------------------------------------------------------------

function connect() {
  setConn('connecting');
  const ws = new WebSocket(wsUrl());
  state.ws = ws;

  ws.addEventListener('open', () => {
    state.connected = true;
    state.reconnectDelay = 500;
    setConn('online');
    sendJoin();
  });

  ws.addEventListener('message', (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    handleMessage(msg);
  });

  ws.addEventListener('close', () => {
    state.connected = false;
    if (state.intentionalClose) return;
    setConn('offline');
    scheduleReconnect();
  });

  ws.addEventListener('error', () => {
    try { ws.close(); } catch { /* ignore */ }
  });
}

function scheduleReconnect() {
  clearTimeout(state.reconnectTimer);
  els.connText.textContent = `连接断开，${Math.round(state.reconnectDelay / 100) / 10} 秒后重连…`;
  state.reconnectTimer = setTimeout(() => {
    connect();
    state.reconnectDelay = Math.min(state.reconnectDelay * 1.5, 10000);
  }, state.reconnectDelay);
}

function sendJoin() {
  const c = state.creds;
  const msg = { type: 'join', room: c.room, nick: c.nick };
  if (c.id) msg.participantId = c.id;
  if (c.host && c.hostToken) msg.hostToken = c.hostToken;
  if (!c.host && c.secret) msg.secret = c.secret;
  wsSend(msg);
}

function wsSend(obj) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(obj));
    return true;
  }
  toast('当前未连接服务器', 'error');
  return false;
}

function sendCmd(obj) {
  obj.version = state.version;
  wsSend(obj);
}

function setConn(status) {
  els.connDot.classList.remove('online', 'offline');
  if (status === 'online') {
    els.connDot.classList.add('online');
    els.connText.textContent = '已连接';
  } else if (status === 'offline') {
    els.connDot.classList.add('offline');
    els.connText.textContent = '已断开';
  } else {
    els.connText.textContent = '连接中…';
  }
}

// ---------------------------------------------------------------------------
// 服务端消息
// ---------------------------------------------------------------------------

function handleMessage(msg) {
  switch (msg.type) {
    case 'joined': {
      state.me = msg.you;
      // 首次快照只播种状态，不把重连前已存在的邀请/撤销状态当成新变化弹提示
      state.swapSeeded = false;
      state.swapStatus = new Map();
      state.undoSeen = null;
      // 持久化身份，供断线/刷新后恢复
      const creds = state.creds;
      creds.id = msg.you.id;
      creds.nick = msg.you.nick;
      creds.host = !!msg.you.host;
      if (msg.you.secret) creds.secret = msg.you.secret;
      saveCreds();

      document.body.classList.toggle('role-host', creds.host);
      document.body.classList.toggle('role-participant', !creds.host);
      els.hostBadge.classList.toggle('hidden', !creds.host);
      applySnapshot(msg.snapshot);
      break;
    }
    case 'snapshot':
      applySnapshot(msg.snapshot);
      break;
    case 'error':
      if (msg.code === 'STALE_VERSION') {
        toast(`状态已过期，已同步到最新版本`, 'error');
      } else if (msg.code === 'NO_ROOM') {
        toast('房间不存在或已被回收', 'error');
        state.intentionalClose = true;
        try { state.ws.close(); } catch { /* ignore */ }
        clearCreds();
        setTimeout(showHome, 800);
      } else {
        toast(msg.error || '操作失败', 'error');
      }
      if (msg.snapshot) applySnapshot(msg.snapshot);
      break;
    case 'kicked':
      toast(msg.reason || '你已在其他页面打开此房间', 'error');
      state.intentionalClose = true;
      try { state.ws.close(); } catch { /* ignore */ }
      clearCreds();
      setTimeout(showHome, 1000);
      break;
  }
}

function applySnapshot(snap) {
  state.snapshot = snap;
  state.version = snap.version;
  state.clockOffset = snap.serverTs - Date.now();
  els.versionBadge.textContent = `v${snap.version}`;
  processSwaps(snap);
  processUndo(snap);
  render();
}

// ---------------------------------------------------------------------------
// 安全撤销：按快照展示可撤销操作摘要或失效原因
// ---------------------------------------------------------------------------

const UNDO_OP_TEXT = {
  reorder: '调整发言顺序',
  remove: '移出队列',
  start_speaking: '请其开始发言',
  end_speaking: '结束发言'
};

/** 生成撤销状态的本地签名，用于检测跳变，避免重复弹提示 */
function undoSignature(u) {
  if (!u) return 'none';
  return `${u.op}:${u.status}:${u.version}:${u.reason || ''}`;
}

function processUndo(snap) {
  const u = snap.undo || null;
  const sig = undoSignature(u);
  if (state.undoSeen === null) {
    // 首次快照（含重连）只播种，不提示
    state.undoSeen = sig;
    return;
  }
  if (sig === state.undoSeen) return;
  const prev = state.undoSeen;
  state.undoSeen = sig;

  if (u && u.status === 'invalid' && prev !== 'none' && !prev.endsWith(':invalid:' + (u.reason || ''))) {
    // 可撤销操作因介入变更而失效
    toast(`撤销已失效：${u.reasonText || '房间状态出现新的变更'}`, 'error');
  } else if (!u && prev !== 'none') {
    // 撤销被成功消费（或被一键清空等整体重置）
    toast('已撤销最近一次主持人操作', 'success');
  } else if (u && u.status === 'available' && prev === 'none') {
    toast(`可撤销：${u.summary || UNDO_OP_TEXT[u.op] || '主持人操作'}`, 'info');
  }
}

// ---------------------------------------------------------------------------
// 协商换位：状态变化反馈
// ---------------------------------------------------------------------------

function nickOf(snap, id) {
  const p = snap.participants.find((x) => x.id === id);
  return p ? p.nick : '已离开';
}

function swapOutcomeToast(snap, swap, prevStatus) {
  if (swap.status === 'pending') {
    if (state.me && swap.to === state.me.id && !prevStatus) {
      return [`「${nickOf(snap, swap.from)}」邀请你交换发言位置（第 ${swap.fromPos + 1} ↔ 第 ${swap.toPos + 1} 位）`, 'info'];
    }
    return null;
  }
  if (swap.status === 'accepted') {
    return [`换位成功：「${nickOf(snap, swap.from)}」与「${nickOf(snap, swap.to)}」已交换位置`, 'success'];
  }
  if (swap.status === 'rejected') {
    if (state.me && swap.from === state.me.id) {
      return [`「${nickOf(snap, swap.to)}」拒绝了你的换位邀请`, 'error'];
    }
    return ['换位邀请已拒绝', 'info'];
  }
  if (swap.status === 'cancelled') {
    if (state.me && swap.to === state.me.id) {
      return [`「${nickOf(snap, swap.from)}」撤回了换位邀请`, 'info'];
    }
    return ['换位邀请已撤回', 'info'];
  }
  if (swap.status === 'invalid') {
    const reasonText = {
      canceled_speak: '一方取消了发言申请',
      started_speaking: '一方开始发言',
      removed: '一方被移出队列',
      left: '一方离开了房间',
      reordered: '主持人调整了发言顺序',
      cleared: '主持人清空了队列',
      position_changed: '双方位置已变化'
    }[swap.reason] || '邀请已失效';
    return [`换位邀请已失效：${reasonText}`, 'error'];
  }
  return null;
}

function processSwaps(snap) {
  const seen = new Set();
  for (const swap of snap.swaps || []) {
    seen.add(swap.id);
    const prev = state.swapStatus.get(swap.id);
    if (state.swapSeeded && (!prev || prev.status !== swap.status)) {
      const outcome = swapOutcomeToast(snap, swap, prev ? prev.status : null);
      if (outcome) toast(outcome[0], outcome[1]);
    }
    state.swapStatus.set(swap.id, { status: swap.status, reason: swap.reason });
  }
  // 清理已从快照消失（过期回收）的本地记录
  for (const id of [...state.swapStatus.keys()]) {
    if (!seen.has(id)) state.swapStatus.delete(id);
  }
  state.swapSeeded = true;
}

// ---------------------------------------------------------------------------
// 渲染
// ---------------------------------------------------------------------------

function render() {
  const snap = state.snapshot;
  if (!snap) return;
  renderStage(snap);
  renderQueue(snap);
  renderUndoBar(snap);
  renderSwapBar(snap);
  renderLogs(snap);
  renderPeople(snap);
}

function participantMap(snap) {
  const m = new Map();
  for (const p of snap.participants) m.set(p.id, p);
  return m;
}

function renderStage(snap) {
  if (snap.speaking) {
    els.stageIdle.classList.add('hidden');
    els.stageActive.classList.remove('hidden');
    els.stageNick.innerHTML =
      `<span class="pulse"></span> ${escapeHtml(snap.speaking.nick)}`;
    updateTimer();
  } else {
    els.stageIdle.classList.remove('hidden');
    els.stageActive.classList.add('hidden');
  }
}

function updateTimer() {
  const snap = state.snapshot;
  if (snap && snap.speaking) {
    const elapsed = serverNow() - snap.speaking.startedAt;
    els.stageTimer.textContent = formatDuration(elapsed);
  }
}

if (!state.timerHandle) {
  state.timerHandle = setInterval(updateTimer, 250);
}

function renderQueue(snap) {
  const pmap = participantMap(snap);
  els.queueList.textContent = '';
  els.queueCount.textContent = snap.order.length;
  els.queueEmpty.classList.toggle('hidden', snap.order.length > 0);
  els.queueList.classList.toggle('hidden', snap.order.length === 0);

  const myId = state.me ? state.me.id : null;
  const isHost = !!(state.me && state.me.host);
  const myPending = myId ? (snap.swaps || []).find((s) => s.status === 'pending' && (s.from === myId || s.to === myId)) : null;

  snap.order.forEach((pid, i) => {
    const p = pmap.get(pid);
    const nick = p ? p.nick : '已离开';
    const li = document.createElement('li');
    li.className = 'queue-item';
    li.dataset.id = pid;
    if (state.me && pid === state.me.id) li.classList.add('is-me');
    if (drag && drag.item.dataset.id === pid) li.classList.add('dragging');

    const pendingHere = (snap.swaps || []).find(
      (s) => s.status === 'pending' && (s.from === pid || s.to === pid)
    );
    let swapTag = '';
    if (pendingHere) {
      const otherId = pendingHere.from === pid ? pendingHere.to : pendingHere.from;
      const role = pendingHere.from === pid ? '邀请' : '被邀';
      swapTag = `<span class="swap-tag" title="${role}换位：与「${escapeHtml(nickOf(snap, otherId))}」">⇄ 换位${role}中</span>`;
    }

    // 参与者：对队列中其他人可发起换位邀请；自己有待处理邀请或对方已有邀请时禁用
    let swapInviteBtn = '';
    if (!isHost && pid !== myId) {
      const busy = !!myPending || !!pendingHere;
      swapInviteBtn = `
        <button class="btn btn-ghost btn-mini swap-invite-btn ${busy ? 'is-disabled' : ''}"
                data-act="swap-invite" ${busy ? 'disabled' : ''}
                title="${busy ? '存在待处理的换位邀请' : `邀请「${escapeHtml(nick)}」交换位置`}">⇄ 换位</button>`;
    }

    li.draggable = false; // 使用自定义 pointer 拖拽，同时兼容触屏
    li.innerHTML = `
      <span class="drag-handle ${isHost ? '' : 'hidden'}" title="拖动排序">⠿</span>
      <span class="queue-index">${i + 1}</span>
      <span class="queue-nick">${escapeHtml(nick)}</span>
      ${state.me && pid === state.me.id ? '<span class="me-tag">我</span>' : ''}
      ${swapTag}
      ${p && !p.online ? '<span class="offline-tag">离线</span>' : ''}
      ${swapInviteBtn}
      ${isHost ? `
        <button class="btn btn-primary btn-mini" data-act="start" title="请其发言">▶ 开始</button>
        <button class="btn btn-ghost btn-mini" data-act="remove" title="移出队列">✕</button>
      ` : ''}
    `;
    els.queueList.appendChild(li);
  });

  // 拖拽过程中若收到快照重建了 DOM，需要把拖拽引用绑到新元素，
  // 并保持用户已拖出的视觉位置
  if (drag) {
    const fresh = els.queueList.querySelector(`.queue-item[data-id="${CSS.escape(drag.item.dataset.id)}"]`);
    if (fresh && fresh !== drag.item) {
      drag.item = fresh;
    }
  }

  // 参与者的申请/取消按钮状态
  if (state.me && !state.me.host) {
    const myId = state.me.id;
    const inQueue = snap.order.includes(myId);
    const speaking = snap.speaking && snap.speaking.id === myId;
    els.btnRequest.classList.toggle('hidden', inQueue || speaking);
    els.btnCancel.classList.toggle('hidden', !inQueue);
    els.btnRequest.disabled = speaking;
    els.btnRequest.textContent = speaking ? '你正在发言中' : '🙋 申请发言';
  }
}

/**
 * 安全撤销条：展示最近一次主持人操作的结构化摘要，
 * 可一键撤销；若已被介入变更作废则展示失效原因（按钮禁用）。
 */
function renderUndoBar(snap) {
  const bar = els.undoBar;
  const u = snap.undo;
  bar.textContent = '';
  if (!state.me || !state.me.host || !u) {
    bar.hidden = true;
    return;
  }
  bar.hidden = false;
  bar.classList.toggle('undo-invalid', u.status === 'invalid');

  const main = document.createElement('span');
  main.className = 'undo-main';
  const opText = UNDO_OP_TEXT[u.op] || '主持人操作';
  if (u.status === 'available') {
    const time = formatTime(u.ts);
    main.innerHTML =
      `<span class="undo-tag">可撤销</span>` +
      `<span class="undo-summary">${escapeHtml(u.summary || opText)}</span>` +
      `<span class="undo-meta">${time} · v${u.version} 之后的任何变更都会使其失效</span>`;
    const btn = document.createElement('button');
    btn.className = 'btn btn-mini';
    btn.type = 'button';
    btn.dataset.act = 'undo-host';
    btn.textContent = '↩ 撤销';
    bar.appendChild(main);
    bar.appendChild(btn);
  } else {
    main.innerHTML =
      `<span class="undo-tag undo-tag-dead">已失效</span>` +
      `<span class="undo-summary">${escapeHtml(opText)}：${escapeHtml(u.reasonText || '房间状态出现新的变更')}</span>`;
    bar.appendChild(main);
  }
}

/**
 * 协商换位状态卡：分别展示「我发起的」与「我收到的」待处理邀请，
 * 提供接受 / 拒绝 / 撤回操作。普通参与者视图可见。
 */
function renderSwapBar(snap) {
  els.swapBar.textContent = '';
  if (!state.me || state.me.host) return;

  const myId = state.me.id;
  const pending = (snap.swaps || []).filter((s) => s.status === 'pending');
  const outgoing = pending.filter((s) => s.from === myId);
  const incoming = pending.filter((s) => s.to === myId);

  for (const swap of outgoing) {
    const card = document.createElement('div');
    card.className = 'swap-card swap-out';
    card.innerHTML = `
      <div class="swap-card-main">
        <span class="swap-card-title">⇄ 换位邀请已发出</span>
        <span class="swap-card-desc">等待「${escapeHtml(nickOf(snap, swap.to))}」回应
          （第 ${swap.fromPos + 1} 位 ↔ 第 ${swap.toPos + 1} 位）</span>
      </div>
      <button class="btn btn-ghost btn-mini" data-swap-act="cancel" data-swap-id="${swap.id}">撤回邀请</button>
    `;
    els.swapBar.appendChild(card);
  }

  for (const swap of incoming) {
    const card = document.createElement('div');
    card.className = 'swap-card swap-in';
    card.innerHTML = `
      <div class="swap-card-main">
        <span class="swap-card-title">⇄ 收到换位邀请</span>
        <span class="swap-card-desc">「${escapeHtml(nickOf(snap, swap.from))}」想与你交换位置
          （第 ${swap.fromPos + 1} 位 ↔ 第 ${swap.toPos + 1} 位）</span>
      </div>
      <div class="swap-card-actions">
        <button class="btn btn-primary btn-mini" data-swap-act="accept" data-swap-id="${swap.id}">接受</button>
        <button class="btn btn-ghost btn-mini" data-swap-act="reject" data-swap-id="${swap.id}">拒绝</button>
      </div>
    `;
    els.swapBar.appendChild(card);
  }
}

function renderLogs(snap) {
  els.logList.textContent = '';
  // 最新的在最上面
  for (let i = snap.logs.length - 1; i >= 0; i--) {
    const log = snap.logs[i];
    const li = document.createElement('li');
    li.className = `log-item kind-${log.kind}`;
    li.innerHTML = `<span class="log-time">${formatTime(log.ts)}</span><span>${escapeHtml(log.text)}</span>`;
    els.logList.appendChild(li);
  }
}

function renderPeople(snap) {
  els.peopleCount.textContent = snap.participants.length;
  els.peopleList.textContent = '';
  const sorted = snap.participants.slice().sort((a, b) => {
    if (a.id === snap.hostId) return -1;
    if (b.id === snap.hostId) return 1;
    return a.joinedAt - b.joinedAt;
  });
  for (const p of sorted) {
    const li = document.createElement('li');
    li.className = 'people-item';
    li.innerHTML = `
      <span class="people-status ${p.online ? 'online' : 'offline'}"></span>
      <span class="people-nick ${p.online ? '' : 'offline-name'}">${escapeHtml(p.nick)}</span>
      ${p.id === snap.hostId ? '<span class="crown" title="主持人">👑</span>' : ''}
      ${state.me && p.id === state.me.id ? '<span class="me-tag">我</span>' : ''}
    `;
    els.peopleList.appendChild(li);
  }
}

// ---------------------------------------------------------------------------
// 房间内操作
// ---------------------------------------------------------------------------

els.btnRequest.addEventListener('click', () => {
  sendCmd({ type: 'request_speak' });
});

els.btnCancel.addEventListener('click', () => {
  sendCmd({ type: 'cancel_speak' });
});

els.btnEnd.addEventListener('click', () => {
  sendCmd({ type: 'end_speaking' });
});

els.btnClearAll.addEventListener('click', () => {
  if (!confirm('确定一键清空？\n将清空发言队列、结束当前发言并清空操作记录。')) return;
  sendCmd({ type: 'clear' });
});

els.btnClearLogs.addEventListener('click', () => {
  if (!confirm('确定清空操作记录？')) return;
  sendCmd({ type: 'clear_logs' });
});

// 安全撤销条：撤销最近一次主持人操作（版本冲突仍走 STALE_VERSION 流程）
els.undoBar.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-act="undo-host"]');
  if (!btn || btn.disabled) return;
  sendCmd({ type: 'undo_host' });
});

// 队列条目：开始发言 / 移出 / 发起换位邀请
els.queueList.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-act]');
  if (!btn) return;
  const li = e.target.closest('.queue-item');
  const pid = li.dataset.id;
  if (btn.dataset.act === 'start') {
    sendCmd({ type: 'start_speaking', participantId: pid });
  } else if (btn.dataset.act === 'remove') {
    sendCmd({ type: 'remove', participantId: pid });
  } else if (btn.dataset.act === 'swap-invite') {
    if (btn.disabled) return;
    sendCmd({ type: 'swap_invite', participantId: pid });
  }
});

// 换位状态卡：接受 / 拒绝 / 撤回
els.swapBar.addEventListener('click', (e) => {
  const btn = e.target.closest('button[data-swap-act]');
  if (!btn) return;
  const swapId = btn.dataset.swapId;
  const act = btn.dataset.swapAct;
  if (act === 'accept') sendCmd({ type: 'swap_accept', swapId });
  else if (act === 'reject') sendCmd({ type: 'swap_reject', swapId });
  else if (act === 'cancel') sendCmd({ type: 'swap_cancel', swapId });
});

// ---------------------------------------------------------------------------
// 主持人拖动排序（pointer 事件，兼容鼠标与触屏）
// ---------------------------------------------------------------------------

let drag = null;

els.queueList.addEventListener('pointerdown', (e) => {
  if (!state.me || !state.me.host) return;
  const handle = e.target.closest('.drag-handle');
  if (!handle) return;
  const item = handle.closest('.queue-item');
  if (!item) return;
  e.preventDefault();

  drag = {
    item,
    handle,
    startY: e.clientY,
    moved: false
  };
  handle.setPointerCapture(e.pointerId);
});

els.queueList.addEventListener('pointermove', (e) => {
  if (!drag) return;
  if (!drag.moved && Math.abs(e.clientY - drag.startY) < 6) return;
  drag.moved = true;
  drag.item.classList.add('dragging');

  const items = $$('#queue-list .queue-item');
  const dragged = drag.item;
  let target = null;
  for (const it of items) {
    if (it === dragged) continue;
    const box = it.getBoundingClientRect();
    if (e.clientY < box.top + box.height / 2) { target = it; break; }
  }
  $$('#queue-list .queue-item').forEach((it) => it.classList.remove('drag-over'));
  if (target) {
    target.classList.add('drag-over');
    els.queueList.insertBefore(dragged, target);
  } else {
    els.queueList.appendChild(dragged);
  }
});

function endDrag() {
  if (!drag) return;
  const { item, moved, handle } = drag;
  item.classList.remove('dragging');
  $$('#queue-list .queue-item').forEach((it) => it.classList.remove('drag-over'));

  if (moved) {
    const newOrder = $$('#queue-list .queue-item').map((it) => it.dataset.id);
    const oldOrder = state.snapshot.order;
    const changed = newOrder.some((id, i) => id !== oldOrder[i]);
    if (changed) {
      sendCmd({ type: 'reorder', order: newOrder });
    }
  }
  try { handle.releasePointerCapture(); } catch { /* ignore */ }
  drag = null;
}

els.queueList.addEventListener('pointerup', endDrag);
els.queueList.addEventListener('pointercancel', endDrag);

// ---------------------------------------------------------------------------
// 顶栏：复制 / 退出
// ---------------------------------------------------------------------------

els.roomCode.addEventListener('click', async () => {
  await copyText(els.roomCode.textContent);
  toast(`房间码 ${els.roomCode.textContent} 已复制`, 'success');
});

els.btnCopyLink.addEventListener('click', async () => {
  const url = `${location.origin}/?room=${state.creds.room}`;
  await copyText(url);
  toast('邀请链接已复制', 'success');
});

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); } catch { /* ignore */ }
    ta.remove();
  }
}

els.btnExit.addEventListener('click', () => {
  if (!confirm('确定退出房间？你的席位将被移除。')) return;
  state.intentionalClose = true;
  wsSend({ type: 'leave' });
  try { state.ws.close(); } catch { /* ignore */ }
  clearCreds();
  clearTimeout(state.reconnectTimer);
  showHome();
  toast('已退出房间');
});

// 页面卸载：主动断开（服务端会保留离线席位等待重连，不发送 leave）
window.addEventListener('beforeunload', () => {
  state.intentionalClose = true;
  try { state.ws.close(); } catch { /* ignore */ }
});

// 从后台/休眠恢复时，WebSocket 可能已被系统静默断开：立即探测并重连
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState !== 'visible' || !state.creds) return;
  if (!state.ws || state.ws.readyState === WebSocket.CLOSED) {
    clearTimeout(state.reconnectTimer);
    state.intentionalClose = false;
    state.reconnectDelay = 500;
    connect();
  }
});

showHome();
