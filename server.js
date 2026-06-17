const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

// ── Rooms storage ──
const rooms = {}; // roomCode → gameState

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
  room.log.unshift(msg);
  if (room.log.length > 30) room.log.pop();
}

// ── Broadcast helpers ──
function broadcastRoom(code) {
  const room = rooms[code];
  if (!room) return;
  // Send each player their private view
  room.players.forEach((p, pi) => {
    const socket = io.sockets.sockets.get(p.socketId);
    if (!socket) return;
    socket.emit('gameState', buildState(room, pi));
  });
}

function buildState(room, pi) {
  // Build state for player pi - hand cards are hidden for others
  const players = room.players.map((p, i) => ({
    name: p.name,
    cardCount: p.hand.length,
    // Only send own hand
    hand: i === pi ? p.hand : p.hand.map(() => ({ hidden: true })),
    isActive: i === room.cur,
  }));

  return {
    players,
    myIndex: pi,
    phase: room.phase,
    discard: room.discard.length > 0 ? room.discard[room.discard.length - 1] : null,
    discardEmpty: room.discard.length === 0,
    deckCount: room.deck.length,
    drawn: room.phase === 'drawn' && pi === room.cur ? room.drawn : null,
    sevenP: room.sevenP && pi === room.cur,
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
  };
}

// ── Socket events ──
io.on('connection', socket => {
  console.log('connect', socket.id);

  // ── Lobby ──
  socket.on('createRoom', ({ name }) => {
    const code = makeCode();
    rooms[code] = {
      code, cardCount: 4,
      players: [{ socketId: socket.id, name, hand: [], ready: false }],
      host: socket.id,
      started: false,
      totals: null,
      round: 0,
    };
    socket.join(code);
    socket.emit('roomCreated', { code, playerIndex: 0 });
    io.to(code).emit('lobbyUpdate', { players: rooms[code].players.map(p => p.name), host: 0 });
    console.log(`Room ${code} created by ${name}`);
  });

  socket.on('joinRoom', ({ code, name }) => {
    code = String(code).trim();
    const room = rooms[code];
    if (!room) { socket.emit('error', 'Partie introuvable'); return; }
    if (room.started) { socket.emit('error', 'Partie déjà commencée'); return; }
    if (room.players.length >= 5) { socket.emit('error', 'Partie pleine (5 max)'); return; }
    const pi = room.players.length;
    room.players.push({ socketId: socket.id, name, hand: [], ready: false });
    socket.join(code);
    socket.emit('roomJoined', { code, playerIndex: pi });
    io.to(code).emit('lobbyUpdate', { players: room.players.map(p => p.name), host: 0 });
    console.log(`${name} joined ${code}`);
  });

  socket.on('startGame', ({ code, cardCount }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    if (room.players.length < 2) { socket.emit('error', 'Minimum 2 joueurs'); return; }
    // Validate card count based on player count
    const n = room.players.length;
    let cc = cardCount || 4;
    if (n === 2) cc = Math.max(4, Math.min(8, cc));
    else if (n === 3) cc = Math.max(4, Math.min(5, cc));
    else cc = 4; // 4-5 players
    room.cardCount = cc;
    room.started = true;
    initGame(room);
    io.to(code).emit('gameStarted');
    broadcastRoom(code);
    console.log(`Game ${code} started: ${n} players, ${cc} cards`);
  });

  // ── Peek ──
  socket.on('peekDone', ({ code, playerIndex }) => {
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
    const room = rooms[code];
    if (!room || room.phase !== 'draw') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    reshuffleIfNeeded(room);
    if (room.deck.length === 0) return;
    room.drawn = room.deck.shift();
    room.phase = 'drawn';
    addLog(room, `${room.players[pi].name} pioche.`);
    broadcastRoom(code);
  });

  // ── Take discard ──
  socket.on('takeDiscard', ({ code, playerIndex }) => {
    const room = rooms[code];
    if (!room || room.phase !== 'draw') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    const top = room.discard[room.discard.length - 1];
    if (!top || ['7','J'].includes(top.value)) return;
    room.drawn = room.discard.pop();
    room.phase = 'drawn';
    addLog(room, `${room.players[pi].name} prend la défausse : ${room.drawn.value}${room.drawn.suit}.`);
    broadcastRoom(code);
  });

  // ── Discard drawn ──
  socket.on('discardDrawn', ({ code, playerIndex }) => {
    const room = rooms[code];
    if (!room || room.phase !== 'drawn') return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
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

  // ── Exchange drawn with hand card ──
  socket.on('exchange', ({ code, cardIndex, playerIndex }) => {
    const room = rooms[code];
    if (!room || !['drawn','exchange'].includes(room.phase)) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    if (!room.drawn) return;
    const old = room.players[pi].hand[cardIndex];
    if (!old) return;
    room.players[pi].hand[cardIndex] = room.drawn;
    room.drawn = null;
    room.discard.push(old);
    addLog(room, `${room.players[pi].name} échange → défausse ${old.value}${old.suit}.`);
    if (!checkSpecial(room, old)) {
      room.phase = 'draw';
      endTurnIfNeeded(room);
    }
    broadcastRoom(code);
  });

  // ── Discard drawn via clicking discard (cancel exchange) ──
  socket.on('cancelExchange', ({ code, playerIndex }) => {
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
    const room = rooms[code];
    if (!room) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi < 0) return;
    // Snap is allowed during active play phases — by ANY player on their OWN cards
    const snapPhases = ['draw','drawn','seven','jack'];
    if (!snapPhases.includes(room.phase)) return;

    const card = room.players[pi].hand[cardIndex];
    const top = room.discard[room.discard.length - 1];
    if (!card || !top) return;

    const sameColor = (c1, c2) => (c1.suit==='♥'||c1.suit==='♦') === (c2.suit==='♥'||c2.suit==='♦');

    if (card.value === top.value && sameColor(card, top)) {
      // Good snap
      room.players[pi].hand.splice(cardIndex, 1);
      room.discard.push(card);
      addLog(room, `⚡ ${room.players[pi].name} snap ${card.value}${card.suit} !`);
      // Jack snapped during decide → stays in decide phase
      if (card.value === 'J' && room.jackP && room.jackTimerStep === 'decide') {
        addLog(room, '🃏 Valet snappé — toujours en attente de décision.');
      }
    } else {
      // Bad snap → penalty
      reshuffleIfNeeded(room);
      const penalty = room.deck.length > 0 ? room.deck.shift() : null;
      if (penalty) room.players[pi].hand.push(penalty);
      addLog(room, `❌ ${room.players[pi].name} rate son snap (${card.value}${card.suit}) — pénalité !`);
      io.to(code).emit('wrongSnap', { playerIndex: pi, card });
    }
    broadcastRoom(code);
  });

  // ── Seven power: look at own card ──
  socket.on('sevenLook', ({ code, cardIndex, playerIndex }) => {
    const room = rooms[code];
    if (!room || !room.sevenP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    const card = room.players[pi].hand[cardIndex];
    if (!card) return;
    // Send the card only to this player
    socket.emit('revealCard', { card, reason: 'seven' });
    room.sevenP = false;
    room.phase = 'draw';
    endTurnIfNeeded(room);
    broadcastRoom(code);
  });

  socket.on('sevenSkip', ({ code, playerIndex }) => {
    const room = rooms[code];
    if (!room || !room.sevenP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.cur) return;
    room.sevenP = false;
    room.phase = 'draw';
    endTurnIfNeeded(room);
    broadcastRoom(code);
  });

  // ── Jack power ──
  socket.on('jackActivate', ({ code, playerIndex }) => {
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
    const room = rooms[code];
    if (!room || !room.jackP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;
    room.jackQueue.shift();
    processNextJack(room);
    broadcastRoom(code);
  });

  socket.on('jackPick', ({ code, playerIndex, cardIndex, actorIndex }) => {
    const room = rooms[code];
    if (!room || !room.jackP || !room.jackActivated) return;
    const pi = resolvePlayer(room, socket, actorIndex);
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;

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
    const room = rooms[code];
    if (!room || !room.jackP || room.jackSel.length < 2) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (room.jackQueue.length === 0 || room.jackQueue[0].player !== pi) return;
    doJackSwap(room);
    broadcastRoom(code);
  });

  // ── Cactus ──
  socket.on('cactus', ({ code, playerIndex }) => {
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
    passTurn(room);
    broadcastRoom(code);
  });

  // ── Next round ──
  socket.on('nextRound', ({ code }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    if (room.totals && room.totals.some(t => t >= 100)) return;
    initGame(room);
    io.to(code).emit('gameStarted');
    broadcastRoom(code);
  });

  socket.on('disconnect', () => {
    console.log('disconnect', socket.id);
  });
});

// ── Game logic helpers ──
function checkSpecial(room, card) {
  if (!card) return false;
  if (card.value === '7') {
    room.sevenP = true;
    room.phase = 'seven';
    addLog(room, '🔮 Pouvoir du 7 — regardez une de vos cartes.');
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

function processNextJack(room) {
  if (!room.jackQueue || room.jackQueue.length === 0) {
    room.jackP = false;
    room.jackActivated = false;
    room.jackSel = [];
    room.jackTimerStep = null;
    room.phase = 'draw';
    endTurnIfNeeded(room);
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
  const tmp = room.players[a.p].hand[a.i];
  room.players[a.p].hand[a.i] = room.players[b.p].hand[b.i];
  room.players[b.p].hand[b.i] = tmp;
  addLog(room, `🔄 ${room.players[a.p].name} #${a.i+1} ↔ ${room.players[b.p].name} #${b.i+1}`);
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
  // Give the current player a 3-second window to call Cactus before the turn passes
  // (only if cactus hasn't already been called)
  if (!room.cactusRound) {
    room.phase = 'cactusWindow';
    room.drawn = null;
    broadcastRoom(room.code);
    if (room._cactusTimer) clearTimeout(room._cactusTimer);
    room._cactusTimer = setTimeout(() => {
      // If still in cactusWindow (no cactus called), pass the turn
      if (room.phase === 'cactusWindow') {
        passTurn(room);
        broadcastRoom(room.code);
      }
    }, 3000);
    return;
  }
  passTurn(room);
}

function passTurn(room) {
  if (room.cactusRound) {
    const next = (room.cur + 1) % room.players.length;
    if (next === room.cactusPl) { endRound(room); return; }
    room.cur = next;
  } else {
    room.cur = (room.cur + 1) % room.players.length;
    if (room.players.some(p => p.hand.length === 0)) { endRound(room); return; }
  }
  room.phase = 'draw';
  room.drawn = null;
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
  room.phase = 'score';
  room._rawScores = rawScores;
  room._finalScores = finalScores;
  addLog(room, `Fin de la manche ${room.round} !`);
}

// ── Sleep prevention & health check ──
app.get('/ping', (req, res) => res.json({ ok: true, rooms: Object.keys(rooms).length }));

// Auto-clean empty rooms after 2h
setInterval(() => {
  const now = Date.now();
  for (const code of Object.keys(rooms)) {
    const room = rooms[code];
    if (!room._lastActivity) room._lastActivity = now;
    if (now - room._lastActivity > 2 * 3600 * 1000) delete rooms[code];
  }
}, 60000);

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log(`Cactus server running on port ${PORT}`));
