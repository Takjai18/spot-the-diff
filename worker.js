/**
 * 眼明手快找不同 — Cloudflare Workers + Durable Objects Backend
 * Supports:
 *   - Mode 1: shared_race (全場即時搶點 — atomic first-come-first-served per diff)
 *   - Mode 2: solo_sprint (各自鬥快搵齊 — independent completion race + speed bonus)
 *   - Team Mode (2-4 Teams) & Solo Mode
 *   - Host God-Mode controls, wrong-tap cooldown, ping/pong keepalive, reconnect sync
 */

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const VALID_TEAMS = ["red", "blue", "yellow", "green"];
const MAX_PLAYERS = 40;
const HIT_TOLERANCE_MIN_RADIUS = 24;

function sanitizeText(str, maxLen = 18) {
  if (typeof str !== "string") return "";
  return str.replace(/[\u0000-\u001F\u007F<>]/g, "").trim().slice(0, maxLen);
}

function clampNum(val, min, max, fallback) {
  const n = Number(val);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.round(n)));
}

export function computeRankedLeaderboard(entries) {
  const sorted = [...entries].sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const aFound = a.foundCount || 0;
    const bFound = b.foundCount || 0;
    if (bFound !== aFound) return bFound - aFound;
    const aFin = a.finishedAt || Number.MAX_SAFE_INTEGER;
    const bFin = b.finishedAt || Number.MAX_SAFE_INTEGER;
    if (aFin !== bFin) return aFin - bFin;
    return String(a.name || "").localeCompare(String(b.name || ""));
  });
  let prevScore = null;
  let prevRank = 0;
  return sorted.map((item, idx) => {
    const rank = prevScore !== null && item.score === prevScore ? prevRank : idx + 1;
    prevScore = item.score;
    prevRank = rank;
    return { ...item, rank };
  });
}

export class SpotDiffRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sockets = new Map();
    this.state = this._createInitialState();
  }

  _createInitialState() {
    return {
      roomCode: "",
      hostId: null,
      phase: "lobby",
      config: {
        gameMode: "shared_race",
        teamMode: false,
        roundCount: 6,
        roundDurationSec: 75,
        wrongTapCooldownMs: 2000,
        selectedSceneIds: [],
      },
      players: new Map(),
      rounds: [],
      currentRoundIndex: 0,
      roundStartTime: 0,
      roundEndTime: 0,
      paused: false,
      pausedRemainingMs: 0,
      claimedDiffs: {},
      revealedHints: [],
      finishOrder: [],
      roundHistory: [],
    };
  }

  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    const url = new URL(request.url);
    const roomCode = sanitizeText(url.searchParams.get("room") || "", 8).toUpperCase();
    if (roomCode && !this.state.roomCode) {
      this.state.roomCode = roomCode;
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response(
        JSON.stringify({
          ok: true,
          roomCode: this.state.roomCode,
          phase: this.state.phase,
          playerCount: this.state.players.size,
        }),
        { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
      );
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    const meta = { role: null, clientId: null };
    this.sockets.set(server, meta);

    server.addEventListener("message", (event) => {
      try {
        const msg = JSON.parse(event.data);
        this.handleMessage(server, msg);
      } catch (_err) {
        this._send(server, { type: "error", code: "BAD_JSON", message: "無效訊息格式" });
      }
    });

    const onDisconnect = () => this.handleDisconnect(server);
    server.addEventListener("close", onDisconnect);
    server.addEventListener("error", onDisconnect);

    return new Response(null, { status: 101, webSocket: client });
  }

  async alarm() {
    if (this.state.phase === "playing" && !this.state.paused) {
      const now = Date.now();
      if (now >= this.state.roundEndTime - 250) {
        this._endCurrentRound("timeout");
      } else if (this.ctx?.storage?.setAlarm) {
        await this.ctx.storage.setAlarm(this.state.roundEndTime);
      }
    }
  }

  _scheduleAlarm(timestampMs) {
    if (this.ctx?.storage?.setAlarm && Number.isFinite(timestampMs) && timestampMs > Date.now()) {
      try { this.ctx.storage.setAlarm(timestampMs); } catch (_e) {}
    }
  }

  _clearAlarm() {
    if (this.ctx?.storage?.deleteAlarm) {
      try { this.ctx.storage.deleteAlarm(); } catch (_e) {}
    }
  }

  handleDisconnect(ws) {
    const meta = this.sockets.get(ws);
    if (!meta) return;
    this.sockets.delete(ws);

    if (meta.role === "player" && meta.clientId) {
      const player = this.state.players.get(meta.clientId);
      if (player) {
        let stillOpen = false;
        for (const m of this.sockets.values()) {
          if (m.role === "player" && m.clientId === meta.clientId) { stillOpen = true; break; }
        }
        if (!stillOpen) {
          player.connected = false;
          this._broadcastState();
        }
      }
    } else if (meta.role === "host" && meta.clientId === this.state.hostId) {
      let hostStillOpen = false;
      for (const m of this.sockets.values()) {
        if (m.role === "host" && m.clientId === this.state.hostId) { hostStillOpen = true; break; }
      }
      if (!hostStillOpen) this._broadcast({ type: "host_status", connected: false });
    }
  }

  handleMessage(ws, msg) {
    if (!msg || typeof msg !== "object" || typeof msg.type !== "string") return;
    const now = Date.now();

    if (msg.type === "ping") {
      this._send(ws, { type: "pong", serverTime: now });
      return;
    }

    const meta = this.sockets.get(ws);
    if (!meta) return;

    switch (msg.type) {
      case "join_host": {
        const hostId = sanitizeText(msg.hostId || `host_${now}`, 36) || `host_${now}`;
        const roomCode = sanitizeText(msg.roomCode || this.state.roomCode, 8).toUpperCase();
        if (roomCode) this.state.roomCode = roomCode;
        this.state.hostId = hostId;
        meta.role = "host";
        meta.clientId = hostId;
        if (msg.config && typeof msg.config === "object" && this.state.phase === "lobby") {
          this._applyConfig(msg.config);
        }
        this._broadcast({ type: "host_status", connected: true });
        this._send(ws, { type: "state_sync", state: this.getPublicStateFor(meta), serverTime: now });
        return;
      }

      case "join_player": {
        const playerId = sanitizeText(msg.playerId || "", 40);
        const name = sanitizeText(msg.name || "", 18);
        if (!playerId || !name) {
          this._send(ws, { type: "error", code: "INVALID_JOIN", message: "請輸入有效暱稱" });
          return;
        }
        const team = VALID_TEAMS.includes(msg.team) ? msg.team : "red";
        let player = this.state.players.get(playerId);
        if (!player) {
          if (this.state.players.size >= MAX_PLAYERS) {
            this._send(ws, { type: "error", code: "ROOM_FULL", message: "房間人數已滿（上限 40 人）" });
            return;
          }
          player = {
            id: playerId, name, team, score: 0, roundScore: 0,
            foundDiffIds: [], finishedAt: null, combo: 0, lastWrongAt: 0, connected: true,
          };
          this.state.players.set(playerId, player);
        } else {
          player.name = name;
          if (this.state.phase === "lobby") player.team = team;
          player.connected = true;
        }
        meta.role = "player";
        meta.clientId = playerId;
        this._broadcastState();
        return;
      }

      case "update_config": {
        if (!this._isHost(meta) || this.state.phase !== "lobby") return;
        this._applyConfig(msg.config);
        this._broadcastState();
        return;
      }

      case "start_game": {
        if (!this._isHost(meta)) return;
        if (msg.config && typeof msg.config === "object") this._applyConfig(msg.config);
        const incomingRounds = Array.isArray(msg.rounds) ? msg.rounds : [];
        const validRounds = incomingRounds
          .filter((r) => r && typeof r.sceneId === "string" && Array.isArray(r.diffs) && r.diffs.length > 0)
          .slice(0, 10)
          .map((r) => ({
            sceneId: sanitizeText(r.sceneId, 40),
            title: sanitizeText(r.title || r.sceneId, 30),
            difficulty: sanitizeText(r.difficulty || "normal", 16),
            diffs: r.diffs.slice(0, 12).map((d, idx) => ({
              id: sanitizeText(d.id || `d${idx + 1}`, 24),
              x: Number(d.x) || 0,
              y: Number(d.y) || 0,
              r: clampNum(d.r, 12, 60, 24),
              label: sanitizeText(d.label || `不同處 ${idx + 1}`, 30),
            })),
          }));

        if (validRounds.length === 0) {
          this._send(ws, { type: "error", code: "NO_ROUNDS", message: "請至少選擇 1 個關卡" });
          return;
        }

        this.state.rounds = validRounds;
        this.state.currentRoundIndex = 0;
        this.state.roundHistory = [];
        for (const p of this.state.players.values()) {
          p.score = 0; p.roundScore = 0; p.foundDiffIds = []; p.finishedAt = null; p.combo = 0; p.lastWrongAt = 0;
        }
        this._beginRound(0, now);
        return;
      }

      case "spot_tap": {
        if (meta.role !== "player" && !this._isHost(meta)) return;
        if (this.state.phase !== "playing" || this.state.paused) return;
        const playerId = meta.role === "player" ? meta.clientId : sanitizeText(msg.playerId || "", 40);
        const player = this.state.players.get(playerId);
        if (!player) return;
        if (now >= this.state.roundEndTime) {
          this._endCurrentRound("timeout");
          return;
        }
        this._handleSpotTap(ws, player, Number(msg.x), Number(msg.y), now);
        return;
      }

      case "host_pause_toggle": {
        if (!this._isHost(meta) || this.state.phase !== "playing") return;
        if (!this.state.paused) {
          this.state.paused = true;
          this.state.pausedRemainingMs = Math.max(0, this.state.roundEndTime - now);
          this._clearAlarm();
        } else {
          this.state.paused = false;
          this.state.roundEndTime = now + this.state.pausedRemainingMs;
          this.state.pausedRemainingMs = 0;
          this._scheduleAlarm(this.state.roundEndTime);
        }
        this._broadcastState();
        return;
      }

      case "host_reveal_hint": {
        if (!this._isHost(meta) || this.state.phase !== "playing") return;
        const round = this.state.rounds[this.state.currentRoundIndex];
        if (!round) return;
        const activePlayers = [...this.state.players.values()].filter((p) => p.connected);
        const unfound = round.diffs.filter((d) => {
          if (this.state.revealedHints.includes(d.id)) return false;
          if (this.state.config.gameMode === "shared_race") {
            return !this.state.claimedDiffs[d.id];
          }
          if (activePlayers.length > 0 && activePlayers.every((p) => p.foundDiffIds.includes(d.id))) {
            return false;
          }
          return true;
        });
        if (unfound.length > 0) {
          const picked = unfound[0];
          this.state.revealedHints.push(picked.id);
          this._broadcast({ type: "hint_revealed", diffId: picked.id, x: picked.x, y: picked.y, r: picked.r, label: picked.label });
          this._broadcastState();
        }
        return;
      }

      case "host_end_round": {
        if (!this._isHost(meta) || this.state.phase !== "playing") return;
        this._endCurrentRound("host_skip");
        return;
      }

      case "host_next_round": {
        if (!this._isHost(meta) || this.state.phase !== "round_summary") return;
        const nextIdx = this.state.currentRoundIndex + 1;
        if (nextIdx >= this.state.rounds.length) {
          this.state.phase = "finished";
          this._clearAlarm();
          this._broadcastState();
        } else {
          this._beginRound(nextIdx, now);
        }
        return;
      }

      case "host_finish_game": {
        if (!this._isHost(meta)) return;
        this._clearAlarm();
        this.state.phase = "finished";
        this.state.paused = false;
        this._broadcastState();
        return;
      }

      case "host_adjust_score": {
        if (!this._isHost(meta)) return;
        const targetId = sanitizeText(msg.playerId || "", 40);
        const delta = clampNum(msg.delta, -5000, 5000, 0);
        const player = this.state.players.get(targetId);
        if (!player || delta === 0) return;
        player.score = Math.max(0, player.score + delta);
        this._broadcastState();
        return;
      }

      case "host_kick_player": {
        if (!this._isHost(meta)) return;
        const targetId = sanitizeText(msg.playerId || "", 40);
        if (!this.state.players.has(targetId)) return;
        this.state.players.delete(targetId);
        for (const [sock, m] of this.sockets.entries()) {
          if (m.role === "player" && m.clientId === targetId) {
            this._send(sock, { type: "kicked", message: "主持已將你移出房間" });
            try { sock.close(1000, "kicked"); } catch (_e) {}
            this.sockets.delete(sock);
          }
        }
        this._broadcastState();
        return;
      }

      case "host_reset_game": {
        if (!this._isHost(meta)) return;
        this._clearAlarm();
        this.state.phase = "lobby";
        this.state.rounds = [];
        this.state.currentRoundIndex = 0;
        this.state.claimedDiffs = {};
        this.state.revealedHints = [];
        this.state.finishOrder = [];
        this.state.roundHistory = [];
        this.state.paused = false;
        this.state.pausedRemainingMs = 0;
        for (const p of this.state.players.values()) {
          p.score = 0; p.roundScore = 0; p.foundDiffIds = []; p.finishedAt = null; p.combo = 0; p.lastWrongAt = 0;
        }
        this._broadcastState();
        return;
      }
      default: return;
    }
  }

  _isHost(meta) { return meta && meta.role === "host" && meta.clientId === this.state.hostId; }

  _applyConfig(cfg) {
    if (!cfg || typeof cfg !== "object") return;
    if (cfg.gameMode === "shared_race" || cfg.gameMode === "solo_sprint") this.state.config.gameMode = cfg.gameMode;
    if (typeof cfg.teamMode === "boolean") this.state.config.teamMode = cfg.teamMode;
    if (cfg.roundCount !== undefined) this.state.config.roundCount = clampNum(cfg.roundCount, 1, 10, this.state.config.roundCount);
    if (cfg.roundDurationSec !== undefined) this.state.config.roundDurationSec = clampNum(cfg.roundDurationSec, 30, 180, this.state.config.roundDurationSec);
    if (cfg.wrongTapCooldownMs !== undefined) this.state.config.wrongTapCooldownMs = clampNum(cfg.wrongTapCooldownMs, 0, 5000, this.state.config.wrongTapCooldownMs);
    if (Array.isArray(cfg.selectedSceneIds)) {
      this.state.config.selectedSceneIds = cfg.selectedSceneIds.filter((s) => typeof s === "string").map((s) => sanitizeText(s, 40)).slice(0, 10);
    }
  }

  _beginRound(index, now) {
    this.state.currentRoundIndex = index;
    this.state.phase = "playing";
    this.state.paused = false;
    this.state.pausedRemainingMs = 0;
    this.state.roundStartTime = now;
    this.state.roundEndTime = now + this.state.config.roundDurationSec * 1000;
    this.state.claimedDiffs = {};
    this.state.revealedHints = [];
    this.state.finishOrder = [];
    for (const p of this.state.players.values()) {
      p.roundScore = 0; p.foundDiffIds = []; p.finishedAt = null; p.combo = 0; p.lastWrongAt = 0;
    }
    this._scheduleAlarm(this.state.roundEndTime);
    this._broadcastState();
  }

  _handleSpotTap(ws, player, x, y, now) {
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    const round = this.state.rounds[this.state.currentRoundIndex];
    if (!round) return;

    const cooldownMs = this.state.config.wrongTapCooldownMs;
    if (cooldownMs > 0 && now - player.lastWrongAt < cooldownMs) {
      this._send(ws, { type: "tap_cooldown", remainingMs: cooldownMs - (now - player.lastWrongAt) });
      return;
    }

    let hitDiff = null;
    let bestDistSq = Infinity;
    for (const d of round.diffs) {
      const hitR = Math.max(d.r, HIT_TOLERANCE_MIN_RADIUS);
      const distSq = (x - d.x) ** 2 + (y - d.y) ** 2;
      if (distSq <= hitR * hitR && distSq < bestDistSq) { bestDistSq = distSq; hitDiff = d; }
    }

    if (this.state.config.gameMode === "shared_race") {
      if (!hitDiff) {
        player.lastWrongAt = now;
        player.combo = 0;
        this._send(ws, { type: "tap_miss", x, y, cooldownMs });
        return;
      }
      if (this.state.claimedDiffs[hitDiff.id]) {
        this._send(ws, { type: "already_claimed", diffId: hitDiff.id, by: this.state.claimedDiffs[hitDiff.id].playerName });
        return;
      }
      player.combo += 1;
      const elapsedRatio = Math.min(1, Math.max(0, (now - this.state.roundStartTime) / (this.state.config.roundDurationSec * 1000)));
      const points = 100 + Math.round((1 - elapsedRatio) * 50) + Math.min(player.combo - 1, 4) * 15;

      this.state.claimedDiffs[hitDiff.id] = {
        playerId: player.id, playerName: player.name, team: player.team,
        claimedAt: now, points, x: hitDiff.x, y: hitDiff.y, r: hitDiff.r, label: hitDiff.label,
      };
      player.foundDiffIds.push(hitDiff.id);
      player.roundScore += points;
      player.score += points;

      this._broadcast({
        type: "diff_claimed", diffId: hitDiff.id, x: hitDiff.x, y: hitDiff.y, r: hitDiff.r,
        label: hitDiff.label, playerId: player.id, playerName: player.name, team: player.team,
        points, combo: player.combo,
      });

      if (Object.keys(this.state.claimedDiffs).length >= round.diffs.length) {
        this._endCurrentRound("all_found");
      } else {
        this._broadcastState();
      }
      return;
    }

    if (player.finishedAt !== null) return;
    if (!hitDiff) {
      player.lastWrongAt = now;
      player.combo = 0;
      this._send(ws, { type: "tap_miss", x, y, cooldownMs });
      return;
    }
    if (player.foundDiffIds.includes(hitDiff.id)) {
      this._send(ws, { type: "already_found", diffId: hitDiff.id });
      return;
    }

    player.foundDiffIds.push(hitDiff.id);
    player.combo += 1;
    let points = 100;
    let finishBonus = 0;
    let finishedNow = false;

    if (player.foundDiffIds.length >= round.diffs.length) {
      finishedNow = true;
      player.finishedAt = now;
      this.state.finishOrder.push(player.id);
      const rankBonus = [150, 100, 60][this.state.finishOrder.length - 1] ?? 30;
      const timeBonus = Math.round(Math.max(0, (this.state.roundEndTime - now) / 1000) * 2);
      finishBonus = rankBonus + timeBonus;
      points += finishBonus;
    }

    player.roundScore += points;
    player.score += points;

    this._send(ws, {
      type: "solo_diff_found", diffId: hitDiff.id, x: hitDiff.x, y: hitDiff.y, r: hitDiff.r,
      label: hitDiff.label, points, finishBonus, finished: finishedNow,
      foundCount: player.foundDiffIds.length, totalCount: round.diffs.length,
    });

    if (finishedNow) {
      this._broadcast({
        type: "player_finished", playerId: player.id, playerName: player.name,
        team: player.team, finishRank: this.state.finishOrder.length, finishBonus,
      });
    }

    const connectedPlayers = [...this.state.players.values()].filter((p) => p.connected);
    if (connectedPlayers.length > 0 && connectedPlayers.every((p) => p.finishedAt !== null)) {
      this._endCurrentRound("all_finished");
    } else {
      this._broadcastState();
    }
  }

  _endCurrentRound(reason) {
    this._clearAlarm();
    this.state.phase = "round_summary";
    this.state.paused = false;
    this.state.pausedRemainingMs = 0;
    const round = this.state.rounds[this.state.currentRoundIndex];
    this.state.roundHistory.push({
      roundIndex: this.state.currentRoundIndex,
      sceneId: round ? round.sceneId : "",
      title: round ? round.title : "",
      reason,
      claimedCount: this.state.config.gameMode === "shared_race"
        ? Object.keys(this.state.claimedDiffs).length
        : this.state.finishOrder.length,
    });
    this._broadcastState();
  }

  _buildTeamLeaderboard() {
    const teamMeta = {
      red: { id: "red", name: "紅隊", color: "#FF4D4D", score: 0, members: 0, foundCount: 0 },
      blue: { id: "blue", name: "藍隊", color: "#2563EB", score: 0, members: 0, foundCount: 0 },
      yellow: { id: "yellow", name: "黃隊", color: "#EAB308", score: 0, members: 0, foundCount: 0 },
      green: { id: "green", name: "綠隊", color: "#22C55E", score: 0, members: 0, foundCount: 0 },
    };
    for (const p of this.state.players.values()) {
      const t = teamMeta[p.team] || teamMeta.red;
      t.score += p.score; t.members += 1; t.foundCount += p.foundDiffIds.length;
    }
    return computeRankedLeaderboard(Object.values(teamMeta).filter((t) => t.members > 0));
  }

  getPublicStateFor(meta) {
    const round = this.state.rounds[this.state.currentRoundIndex] || null;
    const isRevealPhase = this.state.phase === "round_summary" || this.state.phase === "finished";
    const isHost = this._isHost(meta);

    const playersArr = [...this.state.players.values()].map((p) => ({
      id: p.id, name: p.name, team: p.team, score: p.score, roundScore: p.roundScore,
      foundCount: p.foundDiffIds.length, finishedAt: p.finishedAt, combo: p.combo, connected: p.connected,
    }));

    const myPlayer = meta?.role === "player" ? this.state.players.get(meta.clientId) : null;
    return {
      roomCode: this.state.roomCode,
      phase: this.state.phase,
      config: { ...this.state.config },
      currentRound: round ? {
        index: this.state.currentRoundIndex, totalRounds: this.state.rounds.length,
        sceneId: round.sceneId, title: round.title, difficulty: round.difficulty,
        totalDiffs: round.diffs.length, diffs: isRevealPhase || isHost ? round.diffs : [],
      } : null,
      roundStartTime: this.state.roundStartTime,
      roundEndTime: this.state.roundEndTime,
      paused: this.state.paused,
      pausedRemainingMs: this.state.pausedRemainingMs,
      claimedDiffs: this.state.claimedDiffs,
      revealedHints: this.state.revealedHints,
      finishOrder: this.state.finishOrder,
      players: computeRankedLeaderboard(playersArr),
      teamLeaderboard: this._buildTeamLeaderboard(),
      myFoundDiffs: myPlayer && round ? round.diffs.filter((d) => myPlayer.foundDiffIds.includes(d.id)) : [],
    };
  }

  _send(ws, payload) { try { ws.send(JSON.stringify(payload)); } catch (_e) {} }
  _broadcast(payload) {
    const raw = JSON.stringify(payload);
    for (const ws of this.sockets.keys()) { try { ws.send(raw); } catch (_e) {} }
  }
  _broadcastState() {
    const now = Date.now();
    for (const [ws, meta] of this.sockets.entries()) {
      this._send(ws, { type: "state_sync", state: this.getPublicStateFor(meta), serverTime: now });
    }
  }
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS_HEADERS });
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ ok: true, service: "spot-the-diff" }), {
        status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
      });
    }
    if (url.pathname === "/ws" || url.pathname.startsWith("/room/")) {
      const fromQuery = sanitizeText(url.searchParams.get("room") || "", 8).toUpperCase();
      const fromPath = url.pathname.startsWith("/room/")
        ? sanitizeText(url.pathname.slice("/room/".length).split("/")[0], 8).toUpperCase()
        : "";
      const roomCode = fromQuery || fromPath;
      if (!roomCode) {
        return new Response(JSON.stringify({ error: "Missing room code" }), {
          status: 400, headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
        });
      }
      const id = env.ROOMS.idFromName(roomCode);
      return env.ROOMS.get(id).fetch(request);
    }
    if (env.ASSETS) return env.ASSETS.fetch(request);
    return new Response("Spot the Difference Realtime Worker is running.", { status: 200, headers: CORS_HEADERS });
  },
};
