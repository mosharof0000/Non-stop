'use strict';
require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const EventEmitter = require('events');
const path = require('path');
const cors = require('cors');

// ─── CONFIG ────────────────────────────────────────────────────────────────────
const CONFIG = {
  DEMO_MODE:        process.env.DEMO_MODE !== 'false',
  DEMO_CAPITAL:     parseFloat(process.env.DEMO_CAPITAL || 1000),
  POLYMARKET_KEY:   process.env.POLYMARKET_API_KEY || '',
  GAMMA_URL:        'https://gamma-api.polymarket.com',
  CLOB_URL:         'https://clob.polymarket.com',
  PORT:             parseInt(process.env.PORT || 3000),

  // Trading
  UP_SHARES_PER_BUY:   20,          // shares per UP order
  UP_INTERVAL_SEC:     10,          // every 10 seconds
  DOWN_SHARES_PER_BUY: 40,          // shares per DOWN order
  DOWN_INTERVAL_SEC:   20,          // every 20 seconds
  STOP_BUYING_SEC:     280,         // stop at 4m40s = 280 seconds into window
  WINDOW_SEC:          300,         // 5 minutes
  TAKE_PROFIT:         0.99,

  ASSETS: ['btc', 'eth'],           // markets to trade
};

// ─── STATE ─────────────────────────────────────────────────────────────────────
const state = {
  capital:         CONFIG.DEMO_CAPITAL,
  startCapital:    CONFIG.DEMO_CAPITAL,

  // Current active windows per asset: { btc: WindowState, eth: WindowState }
  activeWindows:   {},

  // All windows waiting for resolution
  waitingResolution: [],

  // Fully closed/resolved history
  history:         [],

  // Activity log
  logs:            [],

  // Live market prices { btc: {up, down, marketId, slug}, eth: {...} }
  prices:          {},
};

const emitter = new EventEmitter();
emitter.setMaxListeners(50);

// ─── WINDOW STATE SCHEMA ───────────────────────────────────────────────────────
function makeWindow(asset, slug, marketId, windowStart) {
  return {
    id:           uuidv4(),
    asset,                        // 'btc' | 'eth'
    slug,
    marketId,
    windowStart,                  // unix seconds (the timestamp in slug)
    windowEnd:    windowStart + CONFIG.WINDOW_SEC,
    status:       'BUYING',       // BUYING | WAITING | RESOLVED

    up: {
      orders:       [],           // [{shares, price, cost, time}]
      totalShares:  0,
      totalCost:    0,
      avgPrice:     0,
      lastOrderAt:  null,
    },
    down: {
      orders:       [],
      totalShares:  0,
      totalCost:    0,
      avgPrice:     0,
      lastOrderAt:  null,
    },

    tpHit:        null,           // null | 'UP' | 'DOWN'
    resolution:   null,           // null | 'UP' | 'DOWN'
    resolvedAt:   null,
    pnl:          0,
    openedAt:     new Date().toISOString(),
  };
}

// ─── LOGGING ───────────────────────────────────────────────────────────────────
function log(level, msg, data = null) {
  const entry = { id: uuidv4(), ts: new Date().toISOString(), level, msg, data };
  state.logs.unshift(entry);
  if (state.logs.length > 300) state.logs.pop();
  console.log(`[${level.toUpperCase()}] ${msg}`, data ? JSON.stringify(data) : '');
  emitter.emit('log', entry);
}

// ─── SLUG + TIMING ─────────────────────────────────────────────────────────────
function currentWindowTimestamp() {
  return Math.floor(Math.floor(Date.now() / 1000) / 300) * 300;
}

function secondsIntoWindow() {
  const nowSec = Math.floor(Date.now() / 1000);
  const winStart = currentWindowTimestamp();
  return nowSec - winStart;
}

function secondsUntilWindowEnd() {
  return CONFIG.WINDOW_SEC - secondsIntoWindow();
}

function makeSlug(asset, timestamp) {
  return `${asset}-updown-5m-${timestamp}`;
}

// ─── POLYMARKET API ────────────────────────────────────────────────────────────
async function fetchMarketBySlug(slug) {
  try {
    const url = `${CONFIG.GAMMA_URL}/markets?slug=${encodeURIComponent(slug)}`;
    const headers = CONFIG.POLYMARKET_KEY
      ? { Authorization: `Bearer ${CONFIG.POLYMARKET_KEY}` } : {};
    const res = await axios.get(url, { headers, timeout: 8000 });
    const markets = Array.isArray(res.data) ? res.data : (res.data?.markets || [res.data]);
    return markets.find(m => m.slug === slug || m.conditionId) || null;
  } catch (err) {
    log('error', `fetchMarketBySlug failed: ${slug}`, { error: err.message });
    return null;
  }
}

async function fetchLivePrices(asset) {
  const ts = currentWindowTimestamp();
  const slug = makeSlug(asset, ts);

  try {
    const market = await fetchMarketBySlug(slug);
    if (!market) {
      if (CONFIG.DEMO_MODE) return demoPrices(asset, slug, ts);
      return null;
    }

    // Parse outcome prices — Polymarket returns outcomes as ["Up","Down"] with outcomePrices
    const outcomes  = market.outcomes || [];
    const prices    = market.outcomePrices || [];

    let upPrice   = null;
    let downPrice = null;

    outcomes.forEach((o, i) => {
      const name = o.toLowerCase();
      if (name === 'up')   upPrice   = parseFloat(prices[i]);
      if (name === 'down') downPrice = parseFloat(prices[i]);
    });

    // Fallback: if only 2 outcomes and names don't match, assume [up, down]
    if (upPrice === null && prices.length >= 2) {
      upPrice   = parseFloat(prices[0]);
      downPrice = parseFloat(prices[1]);
    }

    return {
      slug,
      marketId:  market.id || market.conditionId,
      up:        upPrice,
      down:      downPrice,
      volume:    market.volume || 0,
      liquidity: market.liquidity || 0,
      live:      true,
    };
  } catch (err) {
    log('error', `fetchLivePrices failed for ${asset}`, { error: err.message });
    if (CONFIG.DEMO_MODE) return demoPrices(asset, slug, ts);
    return null;
  }
}

function demoPrices(asset, slug, ts) {
  // Simulate realistic prices that drift each call
  const existing = state.prices[asset];
  let up   = existing?.up   ?? (0.45 + Math.random() * 0.10);
  let down = existing?.down ?? (1 - up);
  // small drift
  up   = Math.min(0.98, Math.max(0.02, up   + (Math.random() - 0.5) * 0.02));
  down = Math.min(0.98, Math.max(0.02, 1 - up));
  return {
    slug,
    marketId: `demo-${asset}-${ts}`,
    up:   parseFloat(up.toFixed(4)),
    down: parseFloat(down.toFixed(4)),
    volume: 50000 + Math.random() * 50000,
    liquidity: 20000,
    live: false,
  };
}

async function checkResolution(slug) {
  try {
    const market = await fetchMarketBySlug(slug);
    if (!market) return null;

    // Market resolved when closed=true and there's a winner
    if (!market.closed && !market.resolved) return null;

    const outcomes = market.outcomes || [];
    const prices   = market.outcomePrices || [];

    // Winner = outcome whose price resolved to 1.0
    for (let i = 0; i < outcomes.length; i++) {
      if (parseFloat(prices[i]) >= 0.99) {
        const name = outcomes[i].toLowerCase();
        if (name === 'up')   return 'UP';
        if (name === 'down') return 'DOWN';
        // fallback by index
        return i === 0 ? 'UP' : 'DOWN';
      }
    }
    return null;
  } catch (err) {
    log('error', 'checkResolution failed', { slug, error: err.message });
    return null;
  }
}

// ─── PLACE ORDER ───────────────────────────────────────────────────────────────
async function placeOrder(window, side, shares, price) {
  const cost = shares * price;

  if (CONFIG.DEMO_MODE) {
    log('info', `[DEMO] ${window.asset.toUpperCase()} ${side} ${shares} shares @ ${price.toFixed(4)}`, {
      cost: cost.toFixed(2), capital: state.capital.toFixed(2),
    });
    return { success: true, orderId: `demo-${uuidv4()}` };
  }

  try {
    const headers = {
      Authorization: `Bearer ${CONFIG.POLYMARKET_KEY}`,
      'Content-Type': 'application/json',
    };
    const body = {
      market:     window.marketId,
      side:       side.toLowerCase(),
      price,
      size:       shares,
      type:       'limit',
      timeInForce: 'GTC',
    };
    const res = await axios.post(`${CONFIG.CLOB_URL}/order`, body, { headers, timeout: 8000 });
    return { success: true, orderId: res.data.id };
  } catch (err) {
    log('error', `Order failed ${side}`, { error: err.message });
    return { success: false };
  }
}

// ─── RECORD ORDER INTO WINDOW ──────────────────────────────────────────────────
function recordOrder(window, side, shares, price) {
  const leg    = window[side.toLowerCase()];
  const cost   = shares * price;
  leg.orders.push({ shares, price, cost, time: new Date().toISOString() });
  leg.totalShares += shares;
  leg.totalCost   += cost;
  leg.avgPrice     = leg.totalCost / leg.totalShares;
  leg.lastOrderAt  = new Date().toISOString();
  state.capital   -= cost;
}

// ─── BUY TICK: UP ──────────────────────────────────────────────────────────────
async function buyUpTick(asset) {
  const window = state.activeWindows[asset];
  if (!window || window.status !== 'BUYING') return;

  const sec = secondsIntoWindow();
  if (sec >= CONFIG.STOP_BUYING_SEC) return; // past 4:40

  const price = state.prices[asset]?.up;
  if (!price) return;

  const shares = CONFIG.UP_SHARES_PER_BUY;
  if (state.capital < shares * price) {
    log('warn', `Insufficient capital for UP buy`, { asset, needed: (shares * price).toFixed(2) });
    return;
  }

  const order = await placeOrder(window, 'UP', shares, price);
  if (order.success) {
    recordOrder(window, 'UP', shares, price);
    log('info', `⬆ ${asset.toUpperCase()} UP +${shares} @ ${price.toFixed(4)} | Total: ${window.up.totalShares} shares | Avg: ${window.up.avgPrice.toFixed(4)}`, {});
    emitter.emit('state_update', getPublicState());
  }
}

// ─── BUY TICK: DOWN ────────────────────────────────────────────────────────────
async function buyDownTick(asset) {
  const window = state.activeWindows[asset];
  if (!window || window.status !== 'BUYING') return;

  const sec = secondsIntoWindow();
  if (sec >= CONFIG.STOP_BUYING_SEC) return;

  const price = state.prices[asset]?.down;
  if (!price) return;

  const shares = CONFIG.DOWN_SHARES_PER_BUY;
  if (state.capital < shares * price) {
    log('warn', `Insufficient capital for DOWN buy`, { asset, needed: (shares * price).toFixed(2) });
    return;
  }

  const order = await placeOrder(window, 'DOWN', shares, price);
  if (order.success) {
    recordOrder(window, 'DOWN', shares, price);
    log('info', `⬇ ${asset.toUpperCase()} DOWN +${shares} @ ${price.toFixed(4)} | Total: ${window.down.totalShares} shares | Avg: ${window.down.avgPrice.toFixed(4)}`, {});
    emitter.emit('state_update', getPublicState());
  }
}

// ─── WINDOW LIFECYCLE ──────────────────────────────────────────────────────────
let upIntervals   = {};  // asset -> intervalId
let downIntervals = {};
let priceIntervals = {};
let windowCheckerInterval = null;

async function startWindow(asset) {
  const ts      = currentWindowTimestamp();
  const slug    = makeSlug(asset, ts);

  // Check we don't already have this window open
  const existing = state.activeWindows[asset];
  if (existing && existing.windowStart === ts) return;

  log('info', `🟢 New window starting: ${slug}`);

  // Fetch market info
  let marketId = `demo-${asset}-${ts}`;
  if (!CONFIG.DEMO_MODE) {
    const market = await fetchMarketBySlug(slug);
    if (!market) {
      log('error', `Market not found: ${slug} — skipping`);
      return;
    }
    marketId = market.id || market.conditionId;
  }

  const window = makeWindow(asset, slug, marketId, ts);
  state.activeWindows[asset] = window;

  // Fetch initial prices
  await refreshPrices(asset);

  // Start UP buy interval (every 10s)
  clearInterval(upIntervals[asset]);
  upIntervals[asset] = setInterval(() => buyUpTick(asset), CONFIG.UP_INTERVAL_SEC * 1000);
  // Buy immediately on start too
  buyUpTick(asset);

  // Start DOWN buy interval (every 20s)
  clearInterval(downIntervals[asset]);
  downIntervals[asset] = setInterval(() => buyDownTick(asset), CONFIG.DOWN_INTERVAL_SEC * 1000);
  buyDownTick(asset);

  emitter.emit('state_update', getPublicState());
}

async function closeWindow(asset) {
  const window = state.activeWindows[asset];
  if (!window) return;

  window.status = 'WAITING';
  log('info', `⏸ Window ended, waiting for resolution: ${window.slug}`);

  // Stop buy intervals
  clearInterval(upIntervals[asset]);
  clearInterval(downIntervals[asset]);

  // Move to waiting list
  state.waitingResolution.push(window);
  delete state.activeWindows[asset];

  emitter.emit('state_update', getPublicState());
}

// ─── PRICE REFRESH ─────────────────────────────────────────────────────────────
async function refreshPrices(asset) {
  const data = await fetchLivePrices(asset);
  if (data) {
    state.prices[asset] = data;

    // Check TP for active window
    const win = state.activeWindows[asset];
    if (win && win.status === 'BUYING') {
      if (data.up >= CONFIG.TAKE_PROFIT && win.up.totalShares > 0) {
        win.tpHit = 'UP';
        const profit = win.up.totalShares * CONFIG.TAKE_PROFIT - win.up.totalCost;
        win.pnl += profit;
        state.capital += win.up.totalShares * CONFIG.TAKE_PROFIT;
        log('info', `🎯 TP HIT UP for ${asset.toUpperCase()}! Profit: $${profit.toFixed(2)}`);
      }
      if (data.down >= CONFIG.TAKE_PROFIT && win.down.totalShares > 0) {
        win.tpHit = 'DOWN';
        const profit = win.down.totalShares * CONFIG.TAKE_PROFIT - win.down.totalCost;
        win.pnl += profit;
        state.capital += win.down.totalShares * CONFIG.TAKE_PROFIT;
        log('info', `🎯 TP HIT DOWN for ${asset.toUpperCase()}! Profit: $${profit.toFixed(2)}`);
      }
    }

    emitter.emit('prices', { asset, ...data });
    emitter.emit('state_update', getPublicState());
  }
}

// ─── RESOLUTION CHECKER ────────────────────────────────────────────────────────
async function checkAllResolutions() {
  for (const win of state.waitingResolution) {
    if (win.resolution) continue; // already resolved

    let result = null;
    if (CONFIG.DEMO_MODE) {
      // In demo, resolve randomly after window ends
      const nowSec = Math.floor(Date.now() / 1000);
      if (nowSec > win.windowEnd + 30) { // 30s after window end
        result = Math.random() > 0.5 ? 'UP' : 'DOWN';
      }
    } else {
      result = await checkResolution(win.slug);
    }

    if (!result) continue;

    win.resolution  = result;
    win.resolvedAt  = new Date().toISOString();
    win.status      = 'RESOLVED';

    // Calculate PnL
    const winLeg  = win[result.toLowerCase()];
    const loseLeg = win[result === 'UP' ? 'down' : 'up'];

    // Winning side gets paid at 1.0 (resolution)
    const winReturn = winLeg.totalShares * 1.0;
    const lossCost  = loseLeg.totalCost; // losing side worthless

    win.pnl = winReturn - winLeg.totalCost - lossCost;
    state.capital += winReturn;

    log('info', `✅ RESOLVED ${win.asset.toUpperCase()} → ${result} | PnL: ${win.pnl >= 0 ? '+' : ''}$${win.pnl.toFixed(2)}`, {
      winShares: winLeg.totalShares,
      loseShares: loseLeg.totalShares,
    });

    emitter.emit('state_update', getPublicState());
  }

  // Move resolved windows to history
  const resolved = state.waitingResolution.filter(w => w.status === 'RESOLVED');
  state.history.push(...resolved);
  state.waitingResolution = state.waitingResolution.filter(w => w.status !== 'RESOLVED');
}

// ─── MAIN LOOP ─────────────────────────────────────────────────────────────────
function startMainLoop() {
  // Price refresh every 3 seconds for all assets
  CONFIG.ASSETS.forEach(asset => {
    clearInterval(priceIntervals[asset]);
    priceIntervals[asset] = setInterval(() => refreshPrices(asset), 3000);
  });

  // Window manager: every 5 seconds check if we need to start/close windows
  clearInterval(windowCheckerInterval);
  windowCheckerInterval = setInterval(async () => {
    const sec = secondsIntoWindow();
    const ts  = currentWindowTimestamp();

    for (const asset of CONFIG.ASSETS) {
      const win = state.activeWindows[asset];

      // Need a new window?
      if (!win) {
        await startWindow(asset);
        continue;
      }

      // Window from a previous timestamp? Close it.
      if (win.windowStart !== ts && win.status === 'BUYING') {
        await closeWindow(asset);
        await startWindow(asset);
        continue;
      }

      // Past 4:40 → stop buying (intervals already guard this, but log once)
      if (sec >= CONFIG.STOP_BUYING_SEC && win.status === 'BUYING') {
        log('info', `⏱ ${asset.toUpperCase()} past 4:40 — no more buys this window`);
      }

      // Past 5:00 → close window
      if (sec >= CONFIG.WINDOW_SEC - 2 && win.status === 'BUYING') {
        await closeWindow(asset);
      }
    }

    // Check resolutions for waiting windows
    await checkAllResolutions();

  }, 5000);

  log('info', '🤖 Bot started — trading BTC & ETH 5-minute windows');
}

// ─── PUBLIC STATE ───────────────────────────────────────────────────────────────
function getPublicState() {
  const nowSec = secondsIntoWindow();
  const secsLeft = Math.max(0, CONFIG.WINDOW_SEC - nowSec);
  const stoppedBuying = nowSec >= CONFIG.STOP_BUYING_SEC;

  // Total invested across active windows
  let totalInvested = 0;
  Object.values(state.activeWindows).forEach(w => {
    totalInvested += w.up.totalCost + w.down.totalCost;
  });
  state.waitingResolution.forEach(w => {
    totalInvested += w.up.totalCost + w.down.totalCost;
  });

  const totalPnl = state.history.reduce((s, w) => s + w.pnl, 0);
  const wins     = state.history.filter(w => w.pnl > 0).length;
  const losses   = state.history.filter(w => w.pnl <= 0).length;
  const winRate  = (wins + losses) > 0 ? ((wins / (wins + losses)) * 100).toFixed(1) : '0.0';

  return {
    capital:          parseFloat(state.capital.toFixed(2)),
    startCapital:     state.startCapital,
    totalInvested:    parseFloat(totalInvested.toFixed(2)),
    totalPnl:         parseFloat(totalPnl.toFixed(2)),
    totalReturn:      parseFloat(((state.capital - state.startCapital) / state.startCapital * 100).toFixed(2)),
    wins, losses, winRate,

    activeWindows:    Object.fromEntries(
      Object.entries(state.activeWindows).map(([k, v]) => [k, serializeWindow(v)])
    ),
    waitingResolution: state.waitingResolution.map(serializeWindow),
    history:          state.history.slice(-30).map(serializeWindow),

    prices:           state.prices,
    windowSecsLeft:   secsLeft,
    windowSecsIn:     nowSec,
    stoppedBuying,
    currentTs:        currentWindowTimestamp(),
    logs:             state.logs.slice(0, 80),
    demoMode:         CONFIG.DEMO_MODE,
    config:           CONFIG,
    timestamp:        new Date().toISOString(),
  };
}

function serializeWindow(w) {
  return {
    ...w,
    up: {
      ...w.up,
      avgPrice: parseFloat((w.up.avgPrice || 0).toFixed(4)),
      totalCost: parseFloat(w.up.totalCost.toFixed(2)),
    },
    down: {
      ...w.down,
      avgPrice: parseFloat((w.down.avgPrice || 0).toFixed(4)),
      totalCost: parseFloat(w.down.totalCost.toFixed(2)),
    },
  };
}

// ─── EXPRESS + WEBSOCKET ───────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

function broadcast(type, data) {
  const msg = JSON.stringify({ type, data, ts: new Date().toISOString() });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

wss.on('connection', ws => {
  console.log('[WS] Client connected');
  ws.send(JSON.stringify({ type: 'FULL_STATE', data: getPublicState() }));
  ws.on('error', err => console.error('[WS]', err.message));
});

emitter.on('state_update', data => broadcast('STATE_UPDATE', data));
emitter.on('log',          entry => broadcast('LOG', entry));
emitter.on('prices',       data  => broadcast('PRICES', data));

// REST
app.get('/api/state',  (req, res) => res.json(getPublicState()));
app.get('/api/health', (req, res) => res.json({ ok: true, uptime: process.uptime(), demo: CONFIG.DEMO_MODE }));
app.get('*',           (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── START ─────────────────────────────────────────────────────────────────────
server.listen(CONFIG.PORT, async () => {
  console.log(`
╔══════════════════════════════════════════════════╗
║   POLYMARKET BTC/ETH 5M BOT v2.0 — ONLINE       ║
╠══════════════════════════════════════════════════╣
║   Dashboard : http://localhost:${CONFIG.PORT}              ║
║   Mode      : ${CONFIG.DEMO_MODE ? 'DEMO (paper trading)   ' : 'LIVE (real trades!)  '}      ║
║   Assets    : BTC, ETH 5-minute windows          ║
╚══════════════════════════════════════════════════╝
  `);
  startMainLoop();
});
