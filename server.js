const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

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
  // Send each player their private view
  room.players.forEach((p, pi) => {
    const socket = io.sockets.sockets.get(p.socketId);
    if (!socket) return;
    socket.emit('gameState', buildState(room, pi));
  });
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
    rawScores: room._rawScores || null,
    finalScores: room._finalScores || null,
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
      players: [{ socketId: socket.id, name, hand: [], ready: false, connected: true, avatar: null }],
      host: socket.id,
      started: false,
      totals: null,
      round: 0,
      chat: [],
    };
    socket.join(code);
    socket.emit('roomCreated', { code, playerIndex: 0 });
    io.to(code).emit('lobbyUpdate', { players: rooms[code].players.map(p => ({ name: p.name, avatar: p.avatar })), host: 0 });
    console.log(`Room ${code} created by ${name}`);
  });

  socket.on('joinRoom', ({ code, name }) => {
    code = String(code).trim();
    const room = rooms[code];
    if (!room) { socket.emit('error', 'Partie introuvable'); return; }
    if (room.started) { socket.emit('error', 'Partie déjà commencée'); return; }
    if (room.players.length >= 5) { socket.emit('error', 'Partie pleine (5 max)'); return; }
    const pi = room.players.length;
    room.players.push({ socketId: socket.id, name, hand: [], ready: false, connected: true, avatar: null });
    socket.join(code);
    socket.emit('roomJoined', { code, playerIndex: pi });
    io.to(code).emit('lobbyUpdate', { players: room.players.map(p => ({ name: p.name, avatar: p.avatar })), host: 0 });
    console.log(`${name} joined ${code}`);
  });

  // ── Choose / change avatar (lobby only, before game start) ──
  socket.on('setAvatar', ({ code, playerIndex, avatar }) => {
    const room = rooms[code];
    if (!room || room.started) return;              // only before launch
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi < 0) return;
    if (typeof avatar !== 'number' || avatar < 0 || avatar > 9) return;
    room.players[pi].avatar = avatar;
    io.to(code).emit('lobbyUpdate', { players: room.players.map(p => ({ name: p.name, avatar: p.avatar })), host: 0 });
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
    // Anyone who never picked an avatar gets a default based on their seat.
    room.players.forEach((p, i) => { if (typeof p.avatar !== 'number') p.avatar = i % 10; });
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
    // Player has drawn → they have seen any swapped cards, clear their green LED markers
    if (room.players[pi]) room.players[pi].changed = {};
    addLog(room, `${room.players[pi].name} pioche.`);
    broadcastRoom(code);
    io.to(code).emit('animDraw', { pi });
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
    // Player has taken a card → they have seen any swapped cards, clear their LED markers
    if (room.players[pi]) room.players[pi].changed = {};
    addLog(room, `${room.players[pi].name} prend la défausse : ${room.drawn.value}${room.drawn.suit}.`);
    broadcastRoom(code);
    io.to(code).emit('animTake', { pi, card: room.drawn });
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
    io.to(code).emit('animDiscard', { pi, card: c });
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
    io.to(code).emit('animExchange', { pi, cardIndex, oldCard: old });
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
    const snapPhases = ['draw','drawn','seven','jack','cactusWindow'];
    if (!snapPhases.includes(room.phase)) return;

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
    }
    broadcastRoom(code);
  });

  // ── Seven power: look at own card ──
  socket.on('sevenActivate', ({ code, playerIndex }) => {
    const room = rooms[code];
    if (!room || !room.sevenP) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.sevenOwner) return;
    room.sevenActivated = true;
    addLog(room, '🔮 Cliquez sur une de vos cartes à regarder.');
    broadcastRoom(code);
  });

  socket.on('sevenLook', ({ code, cardIndex, playerIndex }) => {
    const room = rooms[code];
    if (!room || !room.sevenP || !room.sevenActivated) return;
    const pi = resolvePlayer(room, socket, playerIndex);
    if (pi !== room.sevenOwner) return;
    const card = room.players[pi].hand[cardIndex];
    if (!card) return;
    // Send the card only to this player
    socket.emit('revealCard', { card, reason: 'seven' });
    room.sevenQueue.shift();
    processNextSeven(room);
    broadcastRoom(code);
  });

  socket.on('sevenSkip', ({ code, playerIndex }) => {
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
    // Broadcast a big "CACTUS" announcement to all players
    io.to(code).emit('cactusAnnounce', { playerName: room.players[pi].name });
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

  socket.on('chatMessage', ({ code, playerIndex, text }) => {
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
    // Keep the player's game state so they can reconnect; just mark them offline.
    for (const code in rooms) {
      const room = rooms[code];
      const pi = room.players.findIndex(p => p.socketId === socket.id);
      if (pi >= 0) {
        if (room.started) {
          // Game in progress: keep the seat, mark offline so they can reconnect
          room.players[pi].connected = false;
          addLog(room, `${room.players[pi].name} s'est déconnecté...`);
          io.to(code).emit('playerConn', { pi, connected: false, name: room.players[pi].name });
          broadcastRoom(code);
        } else {
          // Still in the lobby: remove them from the list
          room.players.splice(pi, 1);
          if (room.players.length === 0) { delete rooms[code]; }
          else io.to(code).emit('lobbyUpdate', { players: room.players.map(p => ({ name: p.name, avatar: p.avatar })), host: 0 });
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
