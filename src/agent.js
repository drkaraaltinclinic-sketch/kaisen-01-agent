/**
 * KAISEN-01 — Ichimoku Kinko Hyo Screener Agent
 * Data: Hyperliquid Info API (native candles from the execution venue)
 * Optional: TradingView webhook receiver (POST /webhook)
 * Screens multi-timeframe (1H momentum + 4H structure) for:
 *   TK Cross · Kumo Breakout · Edge-to-Edge · The Trinity
 * Emits standardized payloads for SUPREME LEADER and reports to GECKO-01.
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const fetch = require('node-fetch');
const http = require('http');
const cors = require('cors');
const path = require('path');

// ─── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3005;
const GECKO_URL = process.env.GECKO_URL || 'wss://gecko-01-agent-production.up.railway.app/?agent=KAISEN-01';
const HL_API = process.env.HL_API || 'https://api.hyperliquid.xyz/info';
const ASSETS_RAW = (process.env.ASSETS || 'BTC,ETH,SOL,HYPE,XRP,ADA,LINK,AVAX,POPCAT').trim();
const ALL_MODE = ASSETS_RAW.toUpperCase() === 'ALL';
let ASSETS = ALL_MODE ? [] : ASSETS_RAW.split(',').map(s => s.trim());
const MAX_ASSETS = parseInt(process.env.MAX_ASSETS || '50');            // cap when ASSETS=ALL
const SCAN_PACING_MS = parseInt(process.env.SCAN_PACING_MS || (ALL_MODE ? '1100' : '250'));
const TIMEFRAMES = (process.env.TIMEFRAMES || '1h,4h').split(',').map(s => s.trim());
const SCAN_INTERVAL_MS = parseInt(process.env.SCAN_INTERVAL_MS || '300000'); // 5 min
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';                     // optional shared secret

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  startTime: Date.now(),
  geckoConnected: false,
  scanCount: 0,
  signalCount: 0,
  webhookCount: 0,
  candles: {},
  analysis: {},
  signals: [],
  errors: [],
};

// ─── Ichimoku Math ───────────────────────────────────────────────────────────
const hh = (c, n, end) => Math.max(...c.slice(end - n + 1, end + 1).map(x => x.h));
const ll = (c, n, end) => Math.min(...c.slice(end - n + 1, end + 1).map(x => x.l));

function ichimokuAt(candles, i) {
  if (i < 52 + 26) return null;
  const tenkan = (hh(candles, 9, i) + ll(candles, 9, i)) / 2;
  const kijun  = (hh(candles, 26, i) + ll(candles, 26, i)) / 2;
  const j = i - 26; // cloud at price index i was projected from i-26
  const spanA = ((hh(candles, 9, j) + ll(candles, 9, j)) / 2 + (hh(candles, 26, j) + ll(candles, 26, j)) / 2) / 2;
  const spanB = (hh(candles, 52, j) + ll(candles, 52, j)) / 2;
  const fSpanA = (tenkan + kijun) / 2;                       // future cloud
  const fSpanB = (hh(candles, 52, i) + ll(candles, 52, i)) / 2;
  const close = candles[i].c;
  const cloudTop = Math.max(spanA, spanB), cloudBot = Math.min(spanA, spanB);
  return {
    close, tenkan, kijun, spanA, spanB, fSpanA, fSpanB, cloudTop, cloudBot,
    pricePos: close > cloudTop ? 'ABOVE' : close < cloudBot ? 'BELOW' : 'INSIDE',
    cloudBullish: spanA > spanB,
    futureBullish: fSpanA > fSpanB,
    chikouClear: candles[i - 26] ? (close > candles[i - 26].h ? 'BULL' : close < candles[i - 26].l ? 'BEAR' : 'BLOCKED') : 'UNKNOWN',
  };
}

function screen(coin, tf, candles) {
  const i = candles.length - 1;
  const now = ichimokuAt(candles, i);
  const prev = ichimokuAt(candles, i - 1);
  if (!now || !prev) return null;

  const setups = [];

  // 1. TK Cross
  const tkBull = now.tenkan > now.kijun && prev.tenkan <= prev.kijun;
  const tkBear = now.tenkan < now.kijun && prev.tenkan >= prev.kijun;
  if (tkBull) setups.push({ type: 'TK_CROSS_BULL', dir: 'LONG', weight: now.pricePos === 'ABOVE' ? 30 : now.pricePos === 'INSIDE' ? 20 : 10 });
  if (tkBear) setups.push({ type: 'TK_CROSS_BEAR', dir: 'SHORT', weight: now.pricePos === 'BELOW' ? 30 : now.pricePos === 'INSIDE' ? 20 : 10 });

  // 2. Kumo Breakout
  if (now.pricePos === 'ABOVE' && prev.pricePos !== 'ABOVE') setups.push({ type: 'KUMO_BREAKOUT_BULL', dir: 'LONG', weight: 35 });
  if (now.pricePos === 'BELOW' && prev.pricePos !== 'BELOW') setups.push({ type: 'KUMO_BREAKOUT_BEAR', dir: 'SHORT', weight: 35 });

  // 3. Edge-to-Edge
  if (now.pricePos === 'INSIDE' && prev.pricePos !== 'INSIDE') {
    const dir = prev.pricePos === 'BELOW' ? 'LONG' : 'SHORT';
    setups.push({ type: 'EDGE_TO_EDGE', dir, weight: 15, target: dir === 'LONG' ? now.cloudTop : now.cloudBot });
  }

  // 4. The Trinity
  if (now.pricePos === 'ABOVE' && tkBull && now.tenkan > now.cloudTop && now.chikouClear === 'BULL') {
    setups.push({ type: 'TRINITY_BULL', dir: 'LONG', weight: 50 });
  }
  if (now.pricePos === 'BELOW' && tkBear && now.tenkan < now.cloudBot && now.chikouClear === 'BEAR') {
    setups.push({ type: 'TRINITY_BEAR', dir: 'SHORT', weight: 50 });
  }

  // Continuous trend context
  let trendScore = 0;
  trendScore += now.pricePos === 'ABOVE' ? 20 : now.pricePos === 'BELOW' ? -20 : 0;
  trendScore += now.tenkan > now.kijun ? 10 : -10;
  trendScore += now.cloudBullish ? 8 : -8;
  trendScore += now.futureBullish ? 7 : -7;
  trendScore += now.chikouClear === 'BULL' ? 10 : now.chikouClear === 'BEAR' ? -10 : 0;

  const analysis = {
    coin, tf,
    price: now.close,
    tenkan: +now.tenkan.toFixed(6), kijun: +now.kijun.toFixed(6),
    spanA: +now.spanA.toFixed(6), spanB: +now.spanB.toFixed(6),
    cloudTop: +now.cloudTop.toFixed(6), cloudBot: +now.cloudBot.toFixed(6),
    pricePos: now.pricePos, cloudBullish: now.cloudBullish,
    futureBullish: now.futureBullish, chikou: now.chikouClear,
    trendScore,
    trend: trendScore >= 25 ? 'STRONG_BULL' : trendScore >= 10 ? 'BULL' : trendScore <= -25 ? 'STRONG_BEAR' : trendScore <= -10 ? 'BEAR' : 'NEUTRAL',
    setups,
    updated: new Date().toISOString(),
  };
  state.analysis[`${coin}:${tf}`] = analysis;
  return analysis;
}

function fireSignals(a) {
  a.setups.forEach(s => {
    state.signalCount++;
    const urgency = s.weight >= 50 ? 'HIGH' : s.weight >= 30 ? 'MEDIUM' : 'LOW';
    const signal = {
      source_agent: 'KAISEN-01_Ichimoku',
      asset: a.coin,
      timeframe: a.tf.toUpperCase(),
      signal: s.type,
      direction: s.dir,
      urgency,
      metrics: {
        current_price: a.price,
        kijun_support: a.kijun,
        cloud_top: a.cloudTop,
        cloud_bottom: a.cloudBot,
        trend: a.trend,
        trend_score: a.trendScore,
        ...(s.target ? { edge_target: +s.target.toFixed(6) } : {}),
      },
      generated: new Date().toISOString(),
    };
    state.signals.unshift(signal);
    if (state.signals.length > 100) state.signals.pop();
    emit('SIGNAL', 'keisen.signal', signal, urgency === 'HIGH' ? 'HIGH' : 'INFO');
  });
}

// ─── Hyperliquid Candle Fetch ─────────────────────────────────────────────────
async function fetchCandles(coin, interval) {
  const lookbackMs = interval === '4h' ? 120 * 4 * 3600e3 : 120 * 3600e3;
  const body = { type: 'candleSnapshot', req: { coin, interval, startTime: Date.now() - lookbackMs, endTime: Date.now() } };
  const res = await fetch(HL_API, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body), timeout: 15000,
  });
  if (!res.ok) throw new Error(`Hyperliquid ${res.status} for ${coin} ${interval}`);
  const raw = await res.json();
  if (!Array.isArray(raw)) throw new Error(`Unexpected response for ${coin}`);
  return raw.map(k => ({ t: k.t, o: +k.o, h: +k.h, l: +k.l, c: +k.c, v: +k.v }));
}

// ─── Hyperliquid Universe Discovery (ASSETS=ALL mode) ────────────────────────
async function fetchUniverse() {
  const res = await fetch(HL_API, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'metaAndAssetCtxs' }), timeout: 15000,
  });
  if (!res.ok) throw new Error(`Hyperliquid universe ${res.status}`);
  const [meta, ctxs] = await res.json();
  const ranked = meta.universe
    .map((u, i) => ({ name: u.name, vol: parseFloat(ctxs[i]?.dayNtlVlm || 0), delisted: !!u.isDelisted }))
    .filter(u => !u.delisted && u.vol > 0)
    .sort((a, b) => b.vol - a.vol)
    .slice(0, MAX_ASSETS)
    .map(u => u.name);
  return ranked;
}

async function scanAll() {
  state.scanCount++;
  if (ALL_MODE) {
    try {
      ASSETS = await fetchUniverse();
      emit('SYS', 'keisen.universe', { mode: 'ALL', topByVolume: ASSETS.length, sample: ASSETS.slice(0, 10) });
    } catch (err) {
      emit('SYS', 'keisen.universe', { mode: 'ALL', error: err.message, usingPrevious: ASSETS.length });
      if (!ASSETS.length) return;
    }
  }
  emit('SYS', 'keisen.scan.start', { scan: state.scanCount, assets: ASSETS.length, timeframes: TIMEFRAMES });
  let ok = 0, failed = 0;
  for (const coin of ASSETS) {
    for (const tf of TIMEFRAMES) {
      try {
        const candles = await fetchCandles(coin, tf);
        if (candles.length < 80) { failed++; continue; }
        state.candles[`${coin}:${tf}`] = candles;
        const a = screen(coin, tf, candles);
        if (a) { ok++; emit('SCAN', 'keisen.analysis', a); fireSignals(a); }
        await new Promise(r => setTimeout(r, SCAN_PACING_MS));
      } catch (err) {
        failed++;
        state.errors.push({ time: new Date().toISOString(), coin, tf, message: err.message });
        state.errors = state.errors.slice(-15);
      }
    }
  }
  emit('SYS', 'keisen.scan.complete', { scan: state.scanCount, analyzed: ok, failed, signalsTotal: state.signalCount });
}

// ─── App Setup ───────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

function broadcast(event) {
  const payload = JSON.stringify({ ...event, agentId: 'KAISEN-01', timestamp: new Date().toISOString() });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
}
function emit(type, topic, data, severity = 'INFO') {
  broadcast({ type, topic, data, severity });
  console.log(`[${new Date().toISOString()}] [${type}] [${topic}] ${JSON.stringify(data).substring(0, 120)}`);
}

// ─── TradingView Webhook Receiver ────────────────────────────────────────────
app.post('/webhook', (req, res) => {
  if (WEBHOOK_SECRET && req.body?.secret !== WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Invalid webhook secret' });
  }
  state.webhookCount++;
  const w = req.body || {};
  const signal = {
    source_agent: 'KAISEN-01_TradingView',
    asset: w.asset || w.ticker || 'UNKNOWN',
    timeframe: (w.timeframe || w.interval || '—').toString().toUpperCase(),
    signal: w.signal || w.alert || 'TV_ALERT',
    direction: (w.direction || w.side || 'NEUTRAL').toString().toUpperCase(),
    urgency: (w.urgency || 'MEDIUM').toString().toUpperCase(),
    metrics: w.metrics || { current_price: w.price || null },
    generated: new Date().toISOString(),
  };
  state.signals.unshift(signal);
  if (state.signals.length > 100) state.signals.pop();
  emit('WEBHOOK', 'keisen.webhook', signal, 'INFO');
  res.json({ ok: true, received: signal });
});

// ─── GECKO-01 Network Link ────────────────────────────────────────────────────
let geckoWs = null;
function connectGecko() {
  console.log(`Connecting to GECKO-01 at ${GECKO_URL}...`);
  geckoWs = new WebSocket(GECKO_URL);
  geckoWs.on('open', () => { state.geckoConnected = true; emit('SYS', 'keisen.gecko.connected', { url: GECKO_URL }); });
  geckoWs.on('close', () => { state.geckoConnected = false; emit('SYS', 'keisen.gecko.disconnected', {}); setTimeout(connectGecko, 5000); });
  geckoWs.on('error', () => {});
  geckoWs.on('message', () => {});
}
connectGecko();
setInterval(() => {
  if (geckoWs?.readyState === WebSocket.OPEN) {
    geckoWs.send(JSON.stringify({ type: 'PING' }));
    geckoWs.send(JSON.stringify({
      type: 'STATUS', agentId: 'KAISEN-01',
      stats: { signals: state.signalCount, scans: state.scanCount, mode: 'active' },
    }));
  }
}, 15000);

// ─── Dashboard WebSocket ──────────────────────────────────────────────────────
wss.on('connection', ws => {
  ws.send(JSON.stringify({
    type: 'SYS', topic: 'keisen.handshake', agentId: 'KAISEN-01', timestamp: new Date().toISOString(),
    data: {
      message: 'Connected to KAISEN-01 Ichimoku screener',
      geckoConnected: state.geckoConnected,
      assets: ASSETS, timeframes: TIMEFRAMES,
      analysis: state.analysis,
      signals: state.signals.slice(0, 30),
      stats: { uptime: Date.now() - state.startTime, scans: state.scanCount, signals: state.signalCount, webhooks: state.webhookCount },
    },
  }));
  ws.on('message', raw => {
    try {
      const m = JSON.parse(raw.toString());
      if (m.type === 'PING') ws.send(JSON.stringify({ type: 'PONG', agentId: 'KAISEN-01' }));
      if (m.type === 'SCAN') scanAll();
    } catch (e) {}
  });
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/health', (_, res) => res.json({
  agent: 'KAISEN-01', status: 'LIVE', geckoConnected: state.geckoConnected,
  uptime: Date.now() - state.startTime, scans: state.scanCount,
  signals: state.signalCount, webhooks: state.webhookCount,
  assets: ASSETS, timeframes: TIMEFRAMES, errors: state.errors.slice(-3),
}));
app.get('/signals', (_, res) => res.json({ agent: 'KAISEN-01', count: state.signalCount, signals: state.signals }));
app.get('/analysis', (_, res) => res.json({ agent: 'KAISEN-01', timestamp: new Date().toISOString(), analysis: state.analysis }));
app.get('/analysis/:coin', (req, res) => {
  const out = {};
  TIMEFRAMES.forEach(tf => { const a = state.analysis[`${req.params.coin.toUpperCase()}:${tf}`]; if (a) out[tf] = a; });
  if (!Object.keys(out).length) return res.status(404).json({ error: 'No analysis for coin' });
  res.json(out);
});
app.post('/scan', (_, res) => { scanAll(); res.json({ ok: true, message: 'Scan started' }); });

// ─── Boot ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log('');
  console.log('╔════════════════════════════════════════════════╗');
  console.log('║     KAISEN-01 Ichimoku Screener Agent          ║');
  console.log('║     Venue: Hyperliquid · Multi-timeframe       ║');
  console.log('╚════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  HTTP    →  http://localhost:${PORT}`);
  console.log(ALL_MODE ? `  Assets  →  ALL (top ${MAX_ASSETS} by 24h volume, auto-discovered)` : `  Assets  →  ${ASSETS.join(', ')}`);
  console.log(`  Scan    →  every ${SCAN_INTERVAL_MS / 60000} min`);
  console.log(`  Webhook →  POST /webhook (TradingView)`);
  console.log('');
  scanAll();
  setInterval(scanAll, SCAN_INTERVAL_MS);
});

process.on('SIGTERM', () => { console.log('KAISEN-01 shutting down...'); process.exit(0); });
process.on('SIGINT',  () => { console.log('KAISEN-01 shutting down...'); process.exit(0); });
