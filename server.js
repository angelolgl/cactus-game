const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
// Durable storage dir. Mount a Railway Volume at /data (or set DATA_DIR) so accounts
// + games survive redeploys. Falls back to a temp dir (ephemeral) if not writable.
const DATA_DIR = (() => {
  const dir = process.env.DATA_DIR || '/data';
  try { fs.mkdirSync(dir, { recursive: true }); fs.accessSync(dir, fs.constants.W_OK); return dir; }
  catch (e) {
    const t = require('os').tmpdir();
    console.warn('[storage] ' + dir + ' non inscriptible → repli sur ' + t + ' (données NON persistantes ; ajoute un Volume Railway monté sur /data).');
    return t;
  }
})();

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    // Don't cache JS image files aggressively so corrections propagate
    if (filePath.endsWith('.js')) {
      res.setHeader('Cache-Control', 'no-cache, must-revalidate');
    }
  }
}));

// ── Rooms storage ──
const rooms = {}; // roomCode → gameState
const TURN_MS = 25000; // per-turn reflection time before auto pioche/défausse

// Resolve player index, refreshing socketId on reconnect
function resolvePlayer(room, socket, playerIndex) {
  let pi = room.players.findIndex(p => p.socketId === socket.id);
  if (pi < 0 && typeof playerIndex === 'number' && room.players[playerIndex]) {
    pi = playerIndex;
    room.players[pi].socketId = socket.id;
    socket.join(room.code);
  }
  return pi;
}

// ── Host helpers (host is tracked by SEAT index, so it survives reconnection) ──
function seatOf(room, socket) { return room.players.findIndex(p => p.socketId === socket.id); }
function isHostSocket(room, socket) { return seatOf(room, socket) === (room.hostIndex || 0); }
// Give the host role to the first connected player (used when the host leaves).
function reassignHost(room) {
  let idx = room.players.findIndex(p => p.connected !== false);
  if (idx < 0) idx = 0;
  room.hostIndex = idx;
  return idx;
}


function makeCode() {
  return String(Math.floor(1000 + Math.random() * 9000));
}

// ── Card utils ──
const SUITS = ['♠','♥','♦','♣'];
const VALS  = ['A','2','3','4','5','6','7','8','9','10','J','Q','K'];

function pts(card) {
  if (card.value === '9') return (card.suit === '♥' || card.suit === '♦') ? -2 : 14;
  return {A:1,'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'10':10,J:11,Q:12,K:0}[card.value];
}

function makeDeck() {
  const d = [];
  let id = 0;
  for (const s of SUITS) for (const v of VALS) d.push({ suit: s, value: v, uid: id++ });
  return d;
}

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function firstNonSpecial(deck) {
  const SPECIALS = ['7','9','J','K'];
  for (let i = 0; i < deck.length; i++) {
    if (!SPECIALS.includes(deck[i].value)) {
      return deck.splice(i, 1)[0];
    }
  }
  return deck.shift();
}

// ── Game init ──
function initGame(room) {
  const deck = shuffle(makeDeck());
  const cc = room.cardCount;
  room.players.forEach(p => { p.hand = deck.splice(0, cc); p.peeked = false; });
  room.deck    = deck;
  room.discard = [firstNonSpecial(deck)];
  room.cur     = Math.floor(Math.random() * room.players.length);
  room.phase   = 'peek';
  room.peekDone = 0;
  room.peekedPlayers = new Set();
  room.drawn   = null;
  room.sevenP  = false;
  room.jackP   = false;
  room.jackQueue = [];
  room.sevenQueue = [];
  room.sevenOwner = null;
  room.pendingAutoCactus = {};
  room._ended = false;
  room.jackSel = [];
  room.jackTimerStep = null;
  room.jackActivated = false;
  room.cactusRound = false;
  room.cactusPl    = null;
  room.round       = (room.round || 0) + 1;
  room.log         = [];
  if (!room.totals) room.totals = room.players.map(() => 0);
}

function addLog(room, msg) {
  if (!room) return;
  if (!room.log) room.log = [];
  room.log.unshift(msg);
  if (room.log.length > 30) room.log.pop();
}

// ── Broadcast helpers ──
function broadcastRoom(code) {
  const room = rooms[code];
  if (!room) return;
  room._lastActivity = Date.now();
  room._dirty = true;   // mark for persistence snapshot
  // Send each player their private view
  room.players.forEach((p, pi) => {
    const socket = io.sockets.sockets.get(p.socketId);
    if (!socket) return;
    socket.emit('gameState', buildState(room, pi));
  });
  syncTurnTimer(room);
}

// ── Per-turn reflection timer (server-authoritative). Fills a ring on the client;
// when it elapses, the current player's turn is auto-played (pioche + défausse). ──
function syncTurnTimer(room){
  const active = (room.phase === 'draw' || room.phase === 'drawn' || room.phase === 'exchange');
  if (!active){
    if (room._turnTimer){ clearTimeout(room._turnTimer); room._turnTimer = null; }
    room._turnTimerFor = null; room.turnDeadline = null;
    return;
  }
  if (room._turnTimerFor == null){   // a fresh turn just began
    room._turnTimerFor = room.cur;
    room.turnDeadline = Date.now() + TURN_MS;
    if (room._turnTimer) clearTimeout(room._turnTimer);
    room._turnTimer = setTimeout(() => onTurnTimeout(room.code), TURN_MS);
  }
}
function onTurnTimeout(code){
  const room = rooms[code];
  if (!room) return;
  room._turnTimer = null;
  const pi = room.cur;
  if (room.phase === 'draw'){
    reshuffleIfNeeded(room);
    if (room.deck.length === 0) return;      // nothing left to draw (rare end-game)
    room.drawn = room.deck.shift();
    room.phase = 'drawn';
    io.to(code).emit('animDraw', { pi });
    addLog(room, `⏱️ Temps écoulé — pioche automatique pour ${room.players[pi].name}.`);
    broadcastRoom(code);
    setTimeout(() => {
      const r = rooms[code];
      if (!r || r.cur !== pi || r.phase !== 'drawn') return;  // player/turn changed meanwhile
      autoDiscardDrawn(code, pi);
    }, 650);
  } else if (room.phase === 'drawn' || room.phase === 'exchange'){
    autoDiscardDrawn(code, pi);
  }
}
function autoDiscardDrawn(code, pi){
  const room = rooms[code];
  if (!room || !room.drawn) return;
  const c = room.drawn; room.drawn = null;
  room.discard.push(c);
  addLog(room, `⏱️ ${room.players[pi].name} n'a pas joué à temps — défausse automatique (${c.value}${c.suit}).`);
  io.to(code).emit('animDiscard', { pi, card: c });
  // Inactivity skip: any power on the auto-discarded card (7 or Valet) is LOST.
  room.phase = 'draw';
  endTurnIfNeeded(room);
  broadcastRoom(code);
}

function buildState(room, pi) {
  // Build state for player pi - hand cards are hidden for others
  const revealAll = room.phase === 'score';
  const players = room.players.map((p, i) => {
    const changed = p.changed || {};
    const hand = (i === pi || revealAll)
      ? p.hand.map(c => ({ ...c, changed: changed[c.uid] || 0 }))
      : p.hand.map(c => ({ hidden: true, changed: changed[c.uid] || 0 }));
    return {
      name: p.name,
      cardCount: p.hand.length,
      hand,
      isActive: i === room.cur,
      connected: p.connected !== false,
      avatar: (typeof p.avatar === 'number') ? p.avatar : null,
    };
  });

  return {
    players,
    myIndex: pi,
    phase: room.phase,
    discard: room.discard.length > 0 ? room.discard[room.discard.length - 1] : null,
    discardEmpty: room.discard.length === 0,
    deckCount: room.deck.length,
    drawn: room.phase === 'drawn' && pi === room.cur ? room.drawn : null,
    sevenP: room.sevenP,
    sevenOwner: room.sevenOwner != null ? room.sevenOwner : null,
    sevenActivated: room.sevenActivated || false,
    jackP: room.jackP,
    jackTimerStep: room.jackTimerStep,
    jackActivated: room.jackActivated,
    jackSel: room.jackSel,
    jackOwner: room.jackQueue.length > 0 ? room.jackQueue[0].player : null,
    cur: room.cur,
    round: room.round,
    totals: room.totals,
    log: room.log,
    cactusRound: room.cactusRound,
    cactusPl: room.cactusPl,
    turnActive: (room.phase === 'draw' || room.phase === 'drawn' || room.phase === 'exchange'),
    turnRemaining: room.turnDeadline ? Math.max(0, room.turnDeadline - Date.now()) : null,
    turnMs: TURN_MS,
    rawScores: room._rawScores || null,
    finalScores: room._finalScores || null,
  };
}

// ── Light anti-cheat / anti-flood (the server is the source of truth; these are guard-rails) ──
const RL_WINDOW = 1000;        // sliding window (ms) for the global per-connection limiter
const RL_MAX    = 20;          // max events per window per connection
const WRONG_SNAP_LOCK = 500;   // lockout (ms) on the snap action after a wrong snap
const ACTION_COOLDOWN = {      // min ms between two of the SAME action (per connection)
  draw:250, takeDiscard:250, discardDrawn:200, exchange:250, cancelExchange:150,
  snap:225, sevenActivate:150, sevenLook:150, sevenSkip:150,
  jackActivate:150, jackIgnore:150, jackPick:120, jackConfirm:200,
  cactus:400, peekDone:300, setAvatar:150, startGame:600, nextRound:600,
  createRoom:500, joinRoom:400, chatMessage:700, reaction:300, rejoin:0,
  register:1500, login:800, authToken:400, logout:400,
};
const _rl = new Map(); // socket.id -> { times:[], last:{}, rejected:0, snapLock:0 }
function _rlState(socket){
  let st = _rl.get(socket.id);
  if(!st){ st = { times:[], last:{}, rejected:0, snapLock:0 }; _rl.set(socket.id, st); }
  return st;
}
function _rlReject(st, action){
  st.rejected++;
  if(st.rejected % 25 === 0) console.warn(`[anti-cheat] ${st.rejected} actions rejetees (derniere: ${action})`);
}
// Returns true if allowed; false (silently dropped) if flooding / too fast / locked.
function allow(socket, action){
  const st = _rlState(socket);
  const now = Date.now();
  st.times = st.times.filter(t => now - t < RL_WINDOW);
  if(st.times.length >= RL_MAX){ _rlReject(st, action); return false; }
  const cd = ACTION_COOLDOWN[action] || 0;
  if(cd && st.last[action] && now - st.last[action] < cd){ _rlReject(st, action); return false; }
  if(action === 'snap' && st.snapLock && now < st.snapLock){ _rlReject(st, action); return false; }
  st.times.push(now);
  st.last[action] = now;
  return true;
}
// Bounds check for client-supplied indices
function inRange(n, len){ return Number.isInteger(n) && n >= 0 && n < len; }

// ── Socket events ──
// ══════════════ ACCOUNTS (register / login / stats / history) ══════════════
const ACCOUNTS_FILE = process.env.ACCOUNTS_FILE || path.join(DATA_DIR, 'cactus-accounts.json');
let accounts = {};     // emailLower -> { email, username, salt, hash, friendCode, createdAt, stats, history }
let authTokens = {};   // token -> usernameLower
let _accDirty = false;
function loadAccounts(){
  try { const j = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8')); accounts = j.users || {}; authTokens = j.tokens || {}; }
  catch(e){ accounts = {}; authTokens = {}; }
}
function saveAccounts(){
  try { fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify({ users: accounts, tokens: authTokens })); _accDirty = false; }
  catch(e){ console.error('accounts save failed:', e.message); }
}
loadAccounts();
setInterval(() => { if (_accDirty) saveAccounts(); }, 5000);
function _hashPw(pw, salt){ return crypto.scryptSync(String(pw), salt, 32).toString('hex'); }
function _genFriendCode(){
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c; do { c = 'CACT-' + Array.from({length:5}, () => A[Math.floor(Math.random()*A.length)]).join(''); }
  while (Object.values(accounts).some(a => a.friendCode === c));
  return c;
}
function _genToken(){ return crypto.randomBytes(24).toString('hex'); }
function _newToken(key){ const t = _genToken(); authTokens[t] = key; _accDirty = true; return t; }
function _pubUser(a){ return { email: a.email, username: a.username, friendCode: a.friendCode, stats: a.stats || {games:0,wins:0,cactus:0}, history: (a.history||[]).slice(0,20) }; }
function _validEmail(e){ return typeof e === 'string' && /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(e.trim()); }
function _validPw(p){ return typeof p === 'string' && p.length >= 6 && p.length <= 100; }

// ── Socket events ──
io.on('connection', socket => {
  socket.username = null;

  socket.on('register', ({ email, username, password } = {}) => {
    if (!allow(socket, 'register')) return;
    email = (email||'').trim().toLowerCase();
    username = (username||'').trim();
    if (!_validEmail(email)) return socket.emit('authResult', { ok:false, mode:'register', error:'Adresse email invalide.' });
    if (!username || username.length < 2 || username.length > 20) return socket.emit('authResult', { ok:false, mode:'register', error:'Pseudo : 2 à 20 caractères.' });
    if (!_validPw(password)) return socket.emit('authResult', { ok:false, mode:'register', error:'Mot de passe : 6 caractères minimum.' });
    if (accounts[email]) return socket.emit('authResult', { ok:false, mode:'register', error:'Cette adresse email est déjà utilisée.' });
    const salt = crypto.randomBytes(16).toString('hex');
    accounts[email] = { email, username, salt, hash: _hashPw(password, salt), friendCode: _genFriendCode(), createdAt: Date.now(), stats:{games:0,wins:0,cactus:0}, history:[] };
    _accDirty = true; saveAccounts();
    socket.username = email;
    socket.emit('authResult', { ok:true, user:_pubUser(accounts[email]), token:_newToken(email) });
  });

  socket.on('login', ({ email, password } = {}) => {
    if (!allow(socket, 'login')) return;
    const key = (email || '').trim().toLowerCase();
    const a = accounts[key];
    const fail = () => socket.emit('authResult', { ok:false, mode:'login', error:'Email ou mot de passe incorrect.' });
    if (!a || typeof password !== 'string') return fail();
    const h = _hashPw(password, a.salt);
    let same = false;
    try { same = h.length === a.hash.length && crypto.timingSafeEqual(Buffer.from(h), Buffer.from(a.hash)); } catch(e){}
    if (!same) return fail();
    socket.username = key;
    socket.emit('authResult', { ok:true, user:_pubUser(a), token:_newToken(key) });
  });

  socket.on('authToken', ({ token } = {}) => {
    if (!allow(socket, 'authToken')) return;
    const key = token && authTokens[token];
    const a = key && accounts[key];
    if (!a) return socket.emit('authResult', { ok:false, expired:true });
    socket.username = key;
    socket.emit('authResult', { ok:true, user:_pubUser(a), token });
  });

  socket.on('logout', ({ token } = {}) => {
    if (token && authTokens[token]) { delete authTokens[token]; _accDirty = true; }
    socket.username = null;
  });

  console.log('connect', socket.id);

  // ── Lobby ──
  socket.on('createRoom', ({ name }) => {
    if (!allow(socket, 'createRoom')) return;
    const code = makeCode();
    rooms[code] = {
      code, cardCount: 4,
      players: [{ socketId: socket.id, name, hand: [], ready: false, connected: true, avatar: null, username: socket.username || null }],
      hostIndex: 0,
      started: false,
      totals: null,
      round: 0,
      chat: [],
    };
    socket.join(code);
    socket.emit('roomCreated', { code, playerIndex: 0 });
    io.to(code).emit('lobbyUpdate', { players: rooms[code].players.map(p => ({ name: p.name, avatar: p.avatar })), host: rooms[code].hostIndex });
    console.log(`Room ${code} created by ${name}`);
  });

  socket.on('joinRoom', ({ code, name }) => {
    if (!allow(socket, 'joinRoom')) return;
    code = String(code).trim();
    const room = rooms[code];
    if (!room) { socket.emit('error', 'Partie introuvable'); return; }
    if (room.started) { socket.emit('error', 'Partie déjà commencée'); return; }
    if (room.players.length >= 5) { socket.emit('error', 'Partie pleine (5 max)'); return; }
    const pi = room.players.length;
    room.players.push({ socketId: socket.id, name, hand: [], ready: false, connected: true, avatar: null, username: socket.username || null });
    socket.join(code);
    socket.emit('roomJoined', { code, playerIndex: pi });
    io.to(code).emit('lobbyUpdate', { players: room.players.map(p => ({ name: p.name, avatar: p.avatar })), host: room.hostIndex });
    console.log(`${name} joined ${code}`);
  });

  // ── Choose / change avatar (lobby only, before game start) ──
  socket.on('setAvatar', ({ code, playerIndex, avatar }) => {
    if (!allow(socket, 'setAvatar')) return;
    const room = rooms[code];
    if (!room || room.started) return;              // only before launch
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi < 0) return;
    if (avatar !== null && (typeof avatar !== 'number' || avatar < 0 || avatar > 9)) return;
    room.players[pi].avatar = avatar;   // null = default cactus
    io.to(code).emit('lobbyUpdate', { players: room.players.map(p => ({ name: p.name, avatar: p.avatar })), host: room.hostIndex });
  });

  socket.on('startGame', ({ code, cardCount }) => {
    if (!allow(socket, 'startGame')) return;
    const room = rooms[code];
    if (!room || !isHostSocket(room, socket)) return;
    if (room.players.length < 2) { socket.emit('error', 'Minimum 2 joueurs'); return; }
    // Validate card count based on player count
    const n = room.players.length;
    let cc = cardCount || 4;
    if (n === 2) cc = Math.max(4, Math.min(8, cc));
    else if (n === 3) cc = Math.max(4, Math.min(5, cc));
    else cc = 4; // 4-5 players
    room.cardCount = cc;
    room.started = true;
    // Players without a chosen avatar keep the default cactus (rendered client-side).
    initGame(room);
    io.to(code).emit('gameStarted');
    broadcastRoom(code);
    console.log(`Game ${code} started: ${n} players, ${cc} cards`);
  });

  // ── Peek ──
  socket.on('peekDone', ({ code, playerIndex }) => {
    if (!allow(socket, 'peekDone')) return;
    const room = rooms[code];
    if (!room) return;
    let pi = room.players.findIndex(p => p.socketId === socket.id);
    // Fallback: use playerIndex sent by client (handles reconnections)
    if (pi < 0 && typeof playerIndex === 'number') {
      pi = playerIndex;
      // Update the stale socketId
      if (room.players[pi]) room.players[pi].socketId = socket.id;
    }
    if (pi < 0) return;
    if (!room.peekedPlayers) room.peekedPlayers = new Set();
    room.peekedPlayers.add(pi);
    console.log(`Peek done: player ${pi}, total ${room.peekedPlayers.size}/${room.players.length}`);
    if (room.peekedPlayers.size >= room.players.length && room.phase === 'peek') {
      // All players done - show countdown then start
      room.phase = 'countdown';
      addLog(room, 'Tous les joueurs sont prêts !');
      broadcastRoom(code);
      let cd = 3;
      const timer = setInterval(() => {
        room.countdownValue = cd;
        io.to(code).emit('startCountdown', { value: cd });
        cd--;
        if (cd < 0) {
          clearInterval(timer);
          room.phase = 'draw';
          addLog(room, `Partie lancée ! ${room.players[room.cur].name} commence.`);
          broadcastRoom(code);
        }
      }, 1000);
    } else {
      broadcastRoom(code);
    }
  });

  // ── Draw ──
  socket.on('draw', ({ code, playerIndex }) => {
    if (!allow(socket, 'draw')) return;
    const room = rooms[code];
    if (!room || room.phase !== 'draw') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    reshuffleIfNeeded(room);
    if (room.deck.length === 0) return;
    room.drawn = room.deck.shift();
    room.phase = 'drawn';
    // Player has drawn → they have seen any swapped cards, clear their green LED markers
    if (room.players[pi]) room.players[pi].changed = {};
    addLog(room, `${room.players[pi].name} pioche.`);
    broadcastRoom(code);
    io.to(code).emit('animDraw', { pi });
  });

  // ── Take discard ──
  socket.on('takeDiscard', ({ code, playerIndex }) => {
    if (!allow(socket, 'takeDiscard')) return;
    const room = rooms[code];
    if (!room || room.phase !== 'draw') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    const top = room.discard[room.discard.length - 1];
    if (!top || ['7','J'].includes(top.value)) return;
    room.drawn = room.discard.pop();
    room.phase = 'drawn';
    // Player has taken a card → they have seen any swapped cards, clear their LED markers
    if (room.players[pi]) room.players[pi].changed = {};
    addLog(room, `${room.players[pi].name} prend la défausse : ${room.drawn.value}${room.drawn.suit}.`);
    broadcastRoom(code);
    io.to(code).emit('animTake', { pi, card: room.drawn });
  });

  // ── Discard drawn ──
  socket.on('discardDrawn', ({ code, playerIndex }) => {
    if (!allow(socket, 'discardDrawn')) return;
    const room = rooms[code];
    if (!room || room.phase !== 'drawn') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    const c = room.drawn;
    room.drawn = null;
    room.discard.push(c);
    addLog(room, `${room.players[pi].name} défausse ${c.value}${c.suit}.`);
    io.to(code).emit('animDiscard', { pi, card: c });
    if (!checkSpecial(room, c)) {
      room.phase = 'draw';
      endTurnIfNeeded(room);
    }
    broadcastRoom(code);
  });

  // ── Exchange drawn with hand card ──
  socket.on('exchange', ({ code, cardIndex, playerIndex }) => {
    if (!allow(socket, 'exchange')) return;
    const room = rooms[code];
    if (!room || !['drawn','exchange'].includes(room.phase)) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    if (!room.drawn) return;
    if (!inRange(cardIndex, room.players[pi].hand.length)) return;
    const old = room.players[pi].hand[cardIndex];
    if (!old) return;
    room.players[pi].hand[cardIndex] = room.drawn;
    room.drawn = null;
    room.discard.push(old);
    addLog(room, `${room.players[pi].name} échange → défausse ${old.value}${old.suit}.`);
    io.to(code).emit('animExchange', { pi, cardIndex, oldCard: old });
    if (!checkSpecial(room, old)) {
      room.phase = 'draw';
      endTurnIfNeeded(room);
    }
    broadcastRoom(code);
  });

  // ── Discard drawn via clicking discard (cancel exchange) ──
  socket.on('cancelExchange', ({ code, playerIndex }) => {
    if (!allow(socket, 'cancelExchange')) return;
    const room = rooms[code];
    if (!room || room.phase !== 'exchange') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur || !room.drawn) return;
    const c = room.drawn;
    room.drawn = null;
    room.discard.push(c);
    addLog(room, `${room.players[pi].name} défausse ${c.value}${c.suit}.`);
    if (!checkSpecial(room, c)) {
      room.phase = 'draw';
      endTurnIfNeeded(room);
    }
    broadcastRoom(code);
  });

  // ── Snap ──
  socket.on('snap', ({ code, cardIndex, playerIndex }) => {
    if (!allow(socket, 'snap')) return;
    const room = rooms[code];
    if (!room) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi < 0) return;
    // Snap is allowed during active play phases — by ANY player on their OWN cards
    const snapPhases = ['draw','drawn','seven','jack','cactusWindow'];
    if (!snapPhases.includes(room.phase)) return;
    if (!inRange(cardIndex, room.players[pi].hand.length)) return;

    const card = room.players[pi].hand[cardIndex];
    const top = room.discard[room.discard.length - 1];
    if (!card || !top) return;

    // Snap rule: same VALUE only (color does not matter)
    if (card.value === top.value) {
      // Good snap
      room.players[pi].hand.splice(cardIndex, 1);
      room.discard.push(card);
      addLog(room, `⚡ ${room.players[pi].name} snap ${card.value}${card.suit} !`);
      // Animate the snapped card flying to the discard (revealed for all)
      io.to(code).emit('animSnap', { pi, cardIndex, card });

      // CUMULATE POWERS: a snapped Jack or 7 also grants its power to the snapper
      if (card.value === 'J') {
        if (!room.jackQueue) room.jackQueue = [];
        room.jackQueue.push({ card, player: pi });
        addLog(room, `🃏 Valet snappé par ${room.players[pi].name} — pouvoir en file.`);
        if (!room.jackP) processNextJack(room);
      } else if (card.value === '7') {
        if (!room.sevenQueue) room.sevenQueue = [];
        room.sevenQueue.push({ player: pi });
        if (!room.sevenP) processNextSeven(room);
      }

      // AUTO-CACTUS when the snap empties this player's hand
      if (room.players[pi].hand.length === 0) {
        room.pendingAutoCactus = room.pendingAutoCactus || {};
        if (pi === room.cur) {
          // Snapped my LAST card on my OWN turn → immediate Cactus,
          // the last turn starts right away with the next player.
          if (!room.cactusRound) {
            room.cactusRound = true;
            room.cactusPl = pi;
            addLog(room, `🌵 CACTUS automatique ! ${room.players[pi].name} n'a plus de cartes.`);
            io.to(code).emit('cactusAnnounce', { playerName: room.players[pi].name });
          }
          // No powers pending → end my turn now and move on
          if (!room.sevenP && !room.jackP) {
            if (room._cactusTimer) clearTimeout(room._cactusTimer);
            room.drawn = null;
            passTurn(room);
          }
        } else {
          // Snapped my last card when it is NOT my turn → Cactus triggers
          // when the turn comes back to me (I then get skipped).
          room.pendingAutoCactus[pi] = true;
          addLog(room, `${room.players[pi].name} n'a plus de cartes (snap).`);
        }
      }
    } else {
      // Bad snap → penalty
      reshuffleIfNeeded(room);
      const penalty = room.deck.length > 0 ? room.deck.shift() : null;
      if (penalty) room.players[pi].hand.push(penalty);
      addLog(room, `❌ ${room.players[pi].name} rate son snap (${card.value}${card.suit}) — pénalité !`);
      io.to(code).emit('wrongSnap', { playerIndex: pi, card });
      _rlState(socket).snapLock = Date.now() + WRONG_SNAP_LOCK;
    }
    broadcastRoom(code);
  });

  // ── Seven power: look at own card ──
  socket.on('sevenActivate', ({ code, playerIndex }) => {
    if (!allow(socket, 'sevenActivate')) return;
    const room = rooms[code];
    if (!room || !room.sevenP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.sevenOwner) return;
    room.sevenActivated = true;
    addLog(room, '🔮 Cliquez sur une de vos cartes à regarder.');
    broadcastRoom(code);
  });

  socket.on('sevenLook', ({ code, cardIndex, playerIndex }) => {
    if (!allow(socket, 'sevenLook')) return;
    const room = rooms[code];
    if (!room || !room.sevenP || !room.sevenActivated) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.sevenOwner) return;
    if (!inRange(cardIndex, room.players[pi].hand.length)) return;
    const card = room.players[pi].hand[cardIndex];
    if (!card) return;
    // Send the card only to this player
    socket.emit('revealCard', { card, reason: 'seven' });
    room.sevenQueue.shift();
    processNextSeven(room);
    broadcastRoom(code);
  });

  socket.on('sevenSkip', ({ code, playerIndex }) => {
    if (!allow(socket, 'sevenSkip')) return;
    const room = rooms[code];
    if (!room || !room.sevenP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.sevenOwner) return;
    room.sevenQueue.shift();
    processNextSeven(room);
    broadcastRoom(code);
    return;
  });

  socket.on('_sevenSkipOld', ({ code, playerIndex }) => {
    const room = rooms[code];
    if (!room) return;
    room.sevenP = false;
    room.sevenActivated = false;
    room.sevenActivated = false;
    room.phase = 'draw';
    endTurnIfNeeded(room);
    broadcastRoom(code);
  });

  // ── Jack power ──
  socket.on('jackActivate', ({ code, playerIndex }) => {
    if (!allow(socket, 'jackActivate')) return;
    const room = rooms[code];
    if (!room || !room.jackP || room.jackTimerStep !== 'decide') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    // Only the jack owner can activate (first in queue)
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;
    room.jackActivated = true;
    room.jackTimerStep = 'pick1';
    room.jackSel = [];
    addLog(room, '🃏 Choisissez la 1ère carte à échanger.');
    broadcastRoom(code);
  });

  socket.on('jackIgnore', ({ code, playerIndex }) => {
    if (!allow(socket, 'jackIgnore')) return;
    const room = rooms[code];
    if (!room || !room.jackP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;
    room.jackQueue.shift();
    processNextJack(room);
    broadcastRoom(code);
  });

  socket.on('jackPick', ({ code, playerIndex, cardIndex, actorIndex }) => {
    if (!allow(socket, 'jackPick')) return;
    const room = rooms[code];
    if (!room || !room.jackP || !room.jackActivated) return;
    const pi = resolvePlayer(room, socket, actorIndex);
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;
    if (!inRange(playerIndex, room.players.length)) return;
    if (!inRange(cardIndex, room.players[playerIndex].hand.length)) return;

    if (room.jackTimerStep === 'pick1') {
      room.jackSel = [{ p: playerIndex, i: cardIndex }];
      room.jackTimerStep = 'pick2';
      addLog(room, `🃏 ${room.players[playerIndex].name} carte ${cardIndex+1} sélectionnée.`);
      broadcastRoom(code);
    } else if (room.jackTimerStep === 'pick2') {
      const sel = room.jackSel[0];
      if (playerIndex === sel.p && cardIndex === sel.i) {
        // Deselect
        room.jackSel = [];
        room.jackTimerStep = 'pick1';
        broadcastRoom(code);
        return;
      }
      if (playerIndex === sel.p) { socket.emit('error', '2 joueurs différents requis'); return; }
      room.jackSel = [sel, { p: playerIndex, i: cardIndex }];
      broadcastRoom(code);
    }
  });

  socket.on('jackConfirm', ({ code, playerIndex }) => {
    if (!allow(socket, 'jackConfirm')) return;
    const room = rooms[code];
    if (!room || !room.jackP || room.jackSel.length < 2) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;
    doJackSwap(room);
    broadcastRoom(code);
  });

  // ── Cactus ──
  socket.on('cactus', ({ code, playerIndex }) => {
    if (!allow(socket, 'cactus')) return;
    const room = rooms[code];
    if (!room || room.cactusRound) return;
    // Cactus can be called during your draw phase OR during the 3s cactusWindow after your turn
    if (room.phase !== 'draw' && room.phase !== 'cactusWindow') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    if (room._cactusTimer) clearTimeout(room._cactusTimer);
    room.cactusRound = true;
    room.cactusPl = pi;
    addLog(room, `🌵 CACTUS ! ${room.players[pi].name}`);
    // Broadcast a big "CACTUS" announcement to all players
    io.to(code).emit('cactusAnnounce', { playerName: room.players[pi].name });
    passTurn(room);
    broadcastRoom(code);
  });

  // ── Next round ──
  socket.on('nextRound', ({ code }) => {
    if (!allow(socket, 'nextRound')) return;
    const room = rooms[code];
    if (!room || !isHostSocket(room, socket)) return;
    if (room.totals && room.totals.some(t => t >= 100)) return;
    initGame(room);
    io.to(code).emit('gameStarted');
    broadcastRoom(code);
  });

  socket.on('chatMessage', ({ code, playerIndex, text }) => {
    if (!allow(socket, 'chatMessage')) return;
    const room = rooms[code];
    if (!room) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi < 0) return;
    const name = room.players[pi]?.name || 'Joueur';
    const msg = String(text || '').slice(0, 200).trim();
    if (!msg) return;
    if (!room.chat) room.chat = [];
    room.chat.push({ name, text: msg, playerIndex: pi, ts: Date.now() });
    if (room.chat.length > 100) room.chat.shift();
    io.to(code).emit('chatUpdate', { chat: room.chat });
  });

  // ── Quick reactions (ephemeral emoji over a player's avatar) ──
  const ALLOWED_REACTIONS = ['👍','❤️','👏','😂','😮','🌵'];
  socket.on('reaction', ({ code, playerIndex, emoji }) => {
    if (!allow(socket, 'reaction')) return;
    const room = rooms[code];
    if (!room) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi < 0) return;
    if (!ALLOWED_REACTIONS.includes(emoji)) return;
    // Light anti-spam: ignore if this player reacted < 350ms ago
    const now = Date.now();
    const p = room.players[pi];
    if (p._lastReact && now - p._lastReact < 350) return;
    p._lastReact = now;
    io.to(code).emit('reaction', { pi, emoji });
  });

  socket.on('rejoin', ({ code, name, playerIndex }) => {
    if (!allow(socket, 'rejoin')) return;
    const room = rooms[code];
    if (!room) { socket.emit('rejoinFailed'); return; }
    let pi = (typeof playerIndex === 'number' && room.players[playerIndex]) ? playerIndex : -1;
    if (pi < 0 && name) pi = room.players.findIndex(p => p.name === name);
    if (pi < 0) { socket.emit('rejoinFailed'); return; }
    room.players[pi].socketId = socket.id;
    room.players[pi].connected = true;
    socket.join(code);
    addLog(room, `${room.players[pi].name} est de retour.`);
    io.to(code).emit('playerConn', { pi, connected: true, name: room.players[pi].name });
    socket.emit('rejoined', { playerIndex: pi, code });
    broadcastRoom(code);
  });

  socket.on('disconnect', () => {
    _rl.delete(socket.id);
    // Keep the player's game state so they can reconnect; just mark them offline.
    for (const code in rooms) {
      const room = rooms[code];
      const pi = room.players.findIndex(p => p.socketId === socket.id);
      if (pi >= 0) {
        if (room.hostIndex == null) room.hostIndex = 0;
        if (room.started) {
          // Game in progress: keep the seat, mark offline so they can reconnect
          room.players[pi].connected = false;
          addLog(room, `${room.players[pi].name} s'est déconnecté...`);
          io.to(code).emit('playerConn', { pi, connected: false, name: room.players[pi].name });
          // If the host dropped, hand the host role to a connected player so the
          // game can still advance (start next round, rematch, etc.).
          if (pi === room.hostIndex) {
            const nh = reassignHost(room);
            io.to(code).emit('hostChanged', { host: nh });
            addLog(room, `${room.players[nh] ? room.players[nh].name : 'Un joueur'} est maintenant l'hôte.`);
          }
          broadcastRoom(code);
        } else {
          // Still in the lobby: remove them from the list
          const wasHost = (pi === room.hostIndex);
          room.players.splice(pi, 1);
          if (room.players.length === 0) { delete rooms[code]; break; }
          // Keep hostIndex pointing at the right seat after the splice
          if (wasHost) room.hostIndex = 0;
          else if (pi < room.hostIndex) room.hostIndex -= 1;
          if (room.hostIndex < 0 || room.hostIndex >= room.players.length) room.hostIndex = 0;
          io.to(code).emit('lobbyUpdate', { players: room.players.map(p => ({ name: p.name, avatar: p.avatar })), host: room.hostIndex });
        }
        break;
      }
    }
  });
});

// ── Game logic helpers ──
function checkSpecial(room, card) {
  if (!card) return false;
  if (card.value === '7') {
    if (!room.sevenQueue) room.sevenQueue = [];
    room.sevenQueue.push({ player: room.cur });
    addLog(room, `🔮 7 posé par ${room.players[room.cur].name}.`);
    if (!room.sevenP) processNextSeven(room);
    return true;
  }
  if (card.value === 'J') {
    if (!room.jackQueue) room.jackQueue = [];
    room.jackQueue.push({ card, player: room.cur });
    addLog(room, `🃏 Valet posé par ${room.players[room.cur].name}.`);
    if (!room.jackP) processNextJack(room);
    return true;
  }
  return false;
}

function processNextSeven(room) {
  if (!room.sevenQueue || room.sevenQueue.length === 0) {
    room.sevenP = false;
    room.sevenActivated = false;
    room.sevenOwner = null;
    // After all sevens, if jacks are pending, process them; else end turn
    if (room.jackQueue && room.jackQueue.length > 0) {
      processNextJack(room);
    } else {
      room.phase = 'draw';
      endTurnIfNeeded(room);
    }
    return;
  }
  const next = room.sevenQueue[0];
  room.sevenP = true;
  room.sevenActivated = false;
  room.sevenOwner = next.player;
  room.phase = 'seven';
  addLog(room, `🔮 Pouvoir du 7 de ${room.players[next.player].name}.`);
}

function processNextJack(room) {
  if (!room.jackQueue || room.jackQueue.length === 0) {
    room.jackP = false;
    room.jackActivated = false;
    room.jackSel = [];
    room.jackTimerStep = null;
    // After all jacks, process any pending sevens, else end turn
    if (room.sevenQueue && room.sevenQueue.length > 0) {
      processNextSeven(room);
    } else {
      room.phase = 'draw';
      endTurnIfNeeded(room);
    }
    return;
  }
  const next = room.jackQueue[0];
  room.jackP = true;
  room.jackActivated = false;
  room.jackSel = [];
  room.jackTimerStep = 'decide';
  room.phase = 'jack';
  addLog(room, `🃏 Valet de ${room.players[next.player].name} — utiliser le pouvoir ?`);
}

function doJackSwap(room) {
  const [a, b] = room.jackSel;
  if (!a || !b || a.p === b.p) { room.jackQueue.shift(); processNextJack(room); return; }
  if (!room.players[a.p].changed) room.players[a.p].changed = {};
  if (!room.players[b.p].changed) room.players[b.p].changed = {};
  const chA = room.players[a.p].changed;
  const chB = room.players[b.p].changed;
  // Cards currently at each slot (before swap) and the swap-count that slot carries
  const cardA = room.players[a.p].hand[a.i];
  const cardB = room.players[b.p].hand[b.i];
  const countA = chA[cardA.uid] || 0; // how many times slot A was already swapped
  const countB = chB[cardB.uid] || 0;
  // Perform the swap
  room.players[a.p].hand[a.i] = cardB;
  room.players[b.p].hand[b.i] = cardA;
  addLog(room, `🔄 ${room.players[a.p].name} #${a.i+1} ↔ ${room.players[b.p].name} #${b.i+1}`);
  // Each swapped slot's count increments (capped at 4 = the max number of Jacks).
  // The count follows the SLOT; the card now sitting there displays it.
  delete chA[cardA.uid];
  delete chB[cardB.uid];
  chA[cardB.uid] = Math.min(4, countA + 1);
  chB[cardA.uid] = Math.min(4, countB + 1);
  room.jackSel = [];
  room.jackQueue.shift();
  processNextJack(room);
}

function reshuffleIfNeeded(room) {
  if (room.deck.length === 0) {
    if (room.discard.length <= 1) return;
    const top = room.discard.pop();
    room.deck = shuffle([...room.discard]);
    room.discard = [top];
    addLog(room, '🔄 Pioche reconstituée.');
  }
}

function endTurnIfNeeded(room) {
  // AUTO-CACTUS: if the player who just acted has no cards left, Cactus is automatic.
  // No 3-second window — jump straight to ending the round flow.
  if (room.players[room.cur] && room.players[room.cur].hand.length === 0) {
    if (!room.cactusRound) {
      room.cactusRound = true;
      room.cactusPl = room.cur;
      addLog(room, `🌵 CACTUS automatique ! ${room.players[room.cur].name} n'a plus de cartes.`);
      io.to(room.code).emit('cactusAnnounce', { playerName: room.players[room.cur].name });
    } else {
      addLog(room, `${room.players[room.cur].name} n'a plus de cartes.`);
    }
    room.drawn = null;
    passTurn(room);
    broadcastRoom(room.code);
    return;
  }

  // Otherwise: give a 3-second window to call Cactus and/or snap.
  room.phase = 'cactusWindow';
  room.drawn = null;
  broadcastRoom(room.code);
  if (room._cactusTimer) clearTimeout(room._cactusTimer);
  room._cactusTimer = setTimeout(() => {
    if (room.phase === 'cactusWindow') {
      passTurn(room);
      broadcastRoom(room.code);
    }
  }, 3000);
}

function passTurn(room) {
  room._ended = false;

  // Move to the next player, skipping any who snapped out their hand (pending auto-cactus)
  let guard = 0;
  while (guard <= room.players.length) {
    guard++;
    // Advance one seat
    const next = (room.cur + 1) % room.players.length;

    // In a cactus round, ending happens when we complete the loop back to the caller
    if (room.cactusRound && next === room.cactusPl) {
      endRound(room);
      return;
    }
    room.cur = next;

    // If this player snapped their last card → auto-cactus (if none) and skip them
    if (room.pendingAutoCactus && room.pendingAutoCactus[room.cur]) {
      if (!room.cactusRound) {
        room.cactusRound = true;
        room.cactusPl = room.cur;
        addLog(room, `🌵 CACTUS automatique ! ${room.players[room.cur].name} n'a plus de cartes.`);
        io.to(room.code).emit('cactusAnnounce', { playerName: room.players[room.cur].name });
      } else {
        addLog(room, `${room.players[room.cur].name} est passé (plus de cartes).`);
      }
      delete room.pendingAutoCactus[room.cur];
      continue; // skip this player's turn
    }

    // Normal stop: this player plays
    room.phase = 'draw';
    room.drawn = null;
    return;
  }
  // Safety: if everyone was skipped, end the round
  endRound(room);
}

function endRound(room) {
  room.players.forEach(p => { p.hand = p.hand.filter(Boolean); });
  const rawScores = room.players.map(p => p.hand.reduce((a, c) => a + pts(c), 0));
  const finalScores = [...rawScores];
  if (room.cactusPl !== null) {
    const cpScore = rawScores[room.cactusPl];
    const othersMin = Math.min(...rawScores.filter((_, i) => i !== room.cactusPl));
    if (cpScore > 5 || othersMin < cpScore) {
      finalScores[room.cactusPl] = cpScore * 2;
    }
  }
  room.players.forEach((_, i) => { room.totals[i] += finalScores[i]; });

  // Game over? record stats + history for logged-in players (once).
  if (room.totals.some(t => t >= 100) && !room._statsSaved) {
    room._statsSaved = true;
    let winner = 0;
    for (let i = 1; i < room.totals.length; i++) if (room.totals[i] < room.totals[winner]) winner = i;
    room.players.forEach((p, i) => {
      if (!p.username || !accounts[p.username]) return;
      const a = accounts[p.username];
      a.stats = a.stats || { games:0, wins:0, cactus:0 };
      a.stats.games++;
      if (i === winner) a.stats.wins++;
      a.history = a.history || [];
      a.history.unshift({ date: Date.now(), result: (i === winner ? 'win' : 'loss'), total: room.totals[i], winner: room.players[winner].name, players: room.players.map(x => x.name) });
      if (a.history.length > 20) a.history.length = 20;
      _accDirty = true;
      if (p.socketId) io.to(p.socketId).emit('accountUpdate', { user: _pubUser(a) });
    });
    saveAccounts();
  }

  room.phase = 'score';
  room._rawScores = rawScores;
  room._finalScores = finalScores;
  addLog(room, `Fin de la manche ${room.round} !`);
}

// ── Sleep prevention & health check ──
app.get('/ping', (req, res) => res.json({ ok: true, rooms: Object.keys(rooms).length }));

// Auto-clean inactive / abandoned rooms (frees memory)
setInterval(() => {
  const now = Date.now();
  for (const code of Object.keys(rooms)) {
    const room = rooms[code];
    if (!room._lastActivity) room._lastActivity = now;
    const idle = now - room._lastActivity;
    const anyConnected = room.players.some(p => p.connected !== false);
    // Delete if: nobody connected for >10 min, OR no activity at all for >1h
    if ((!anyConnected && idle > 10 * 60 * 1000) || idle > 60 * 60 * 1000) {
      delete rooms[code];
      _persistDirty = true;
    }
  }
}, 60000);

// ── Best-effort persistence (survives a server restart) ──
// Snapshot the in-memory rooms to disk and restore them on boot. On Railway the
// filesystem is ephemeral: this survives in-container restarts/crashes, and also
// redeploys IF a persistent Volume is mounted and STATE_FILE points to it.
const STATE_FILE = process.env.STATE_FILE || path.join(DATA_DIR, 'cactus-state.json');
let _persistDirty = false;

function _saveState() {
  try {
    const data = JSON.stringify(rooms, (key, value) => {
      if (key === '_cactusTimer' || key === '_turnTimer') return undefined; // Timeouts: not serializable
      if (value instanceof Set) return { __set: true, v: [...value] };
      return value;
    });
    fs.writeFileSync(STATE_FILE, data);
  } catch (e) { console.warn('[persist] save failed:', e.message); }
}

function _loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const parsed = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'), (key, value) => {
      if (value && value.__set) return new Set(value.v);
      return value;
    });
    let n = 0;
    for (const code in parsed) {
      const r = parsed[code];
      r._cactusTimer = null;
      r._turnTimer = null; r._turnTimerFor = null; r.turnDeadline = null;
      r._dirty = false;
      if (!(r.peekedPlayers instanceof Set)) r.peekedPlayers = new Set(r.peekedPlayers || []);
      if (typeof r.hostIndex !== 'number') r.hostIndex = 0;
      (r.players || []).forEach(p => { p.connected = false; });  // sockets are gone; players will rejoin
      if (r.phase === 'countdown') r.phase = 'peek';             // interrupted countdown → restart cleanly
      rooms[code] = r;
      n++;
    }
    if (n) console.log(`[persist] restored ${n} room(s) from ${STATE_FILE}`);
  } catch (e) { console.warn('[persist] load failed:', e.message); }
}

// Periodic snapshot when something changed
setInterval(() => {
  const dirty = _persistDirty || Object.values(rooms).some(r => r._dirty);
  if (dirty) { _saveState(); _persistDirty = false; Object.values(rooms).forEach(r => { r._dirty = false; }); }
}, 5000);

// Save on graceful shutdown (Railway sends SIGTERM on redeploy/restart)
['SIGTERM', 'SIGINT'].forEach(sig => process.on(sig, () => { _saveState(); process.exit(0); }));

_loadState();  // restore before accepting connections

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log(`Cactus server running on port ${PORT}`));
