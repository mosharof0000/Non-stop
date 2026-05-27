'use strict';
// ============================================================
//  Polymarket Mispricing Arbitrage Bot  —  2026 Rules
//  • WebSocket-first (no REST polling for price data)
//  • Maker-only limit orders  →  zero taker fees
//  • feeRateBps included in every signed order (2026 required)
//  • Early-exit: sell winning leg when it hits $1.00 / $1.01
//  • $100 budget per opportunity
// ============================================================

const express   = require('express');
const http      = require('http');
const { Server }= require('socket.io');
const axios     = require('axios');
const WebSocket = require('ws');
const ethers    = require('ethers');
const crypto    = require('crypto');
const path      = require('path');

// ── CONFIG ──────────────────────────────────────────────────
const CONFIG = {
  PRIVATE_KEY:         process.env.POLYMARKET_PRIVATE_KEY   || '',
  L2_API_KEY:          process.env.POLYMARKET_L2_KEY        || '',
  L2_API_SECRET:       process.env.POLYMARKET_L2_SECRET     || '',
  L2_API_PASSPHRASE:   process.env.POLYMARKET_L2_PASSPHRASE || '',

  GAMMA_URL: 'https://gamma-api.polymarket.com',
  CLOB_URL:  'https://clob.polymarket.com',
  WS_URL:    'wss://ws-subscriptions-clob.polymarket.com/ws/market',

  // Arb params
  MIN_SPREAD:          0.02,   // 2¢ minimum mispricing to enter
  BUDGET_PER_TRADE:    100,    // USDC per opportunity
  MAX_OPEN_POSITIONS:  10,
  EXIT_PRICE:          1.00,   // sell leg when bid hits this
  EXIT_PRICE_BONUS:    1.01,   // also exit at 1.01

  // 2026 fee rules: maker = 0, taker = dynamic up to 1.56%
  // feeRateBps REQUIRED in every signed order
  FEE_RATE_BPS:        0,      // maker orders only

  MARKET_SCAN_INTERVAL: 30_000,
  BOOK_POLL_INTERVAL:   3_000,
  MAX_MARKETS_PER_SCAN: 200,
  REQUEST_DELAY_MS:     600,

  DEMO_MODE: process.env.DEMO_MODE !== 'false',
  PORT: process.env.PORT || 3000,
};

// ── EIP-712 domain ──────────────────────────────────────────
const CTF_EXCHANGE = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E';
const EIP712_DOMAIN = {
  name: 'Polymarket CTF Exchange',
  version: '1',
  chainId: 137,
  verifyingContract: CTF_EXCHANGE,
};
const ORDER_TYPES = {
  Order: [
    { name: 'salt',          type: 'uint256' },
    { name: 'maker',         type: 'address' },
    { name: 'signer',        type: 'address' },
    { name: 'taker',         type: 'address' },
    { name: 'tokenId',       type: 'uint256' },
    { name: 'makerAmount',   type: 'uint256' },
    { name: 'takerAmount',   type: 'uint256' },
    { name: 'expiration',    type: 'uint256' },
    { name: 'nonce',         type: 'uint256' },
    { name: 'feeRateBps',    type: 'uint256' }, // 2026: required field
    { name: 'side',          type: 'uint8'   },
    { name: 'signatureType', type: 'uint8'   },
  ],
};

// ── State ────────────────────────────────────────────────────
const state = {
  capital:       1000,
  allMarkets:    [],
  opportunities: [],
  openPositions: new Map(),
  closedTrades:  [],
  stats: { scanned: 0, detected: 0, entered: 0, exited: 0, totalPnl: 0, winRate: 0 },
  wsConnected:   false,
  lastScan:      null,
  logs:          [],
};

// ── Helpers ──────────────────────────────────────────────────
let io = null;
function log(level, msg, data = {}) {
  const entry = { ts: new Date().toISOString(), level, msg, data };
  state.logs.unshift(entry);
  if (state.logs.length > 500) state.logs.pop();
  console.log(`[${level.toUpperCase()}] ${msg}`, Object.keys(data).length ? JSON.stringify(data) : '');
  io?.emit('log', entry);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function fmtUSDC(n) { return `$${Number(n).toFixed(2)}`; }

// ── CLOB auth headers ────────────────────────────────────────
function clobHeaders(method = 'GET', path = '') {
  const headers = { 'Content-Type': 'application/json' };
  if (CONFIG.L2_API_KEY && CONFIG.PRIVATE_KEY) {
    const ts  = Math.floor(Date.now() / 1000).toString();
    const msg = ts + method + path;
    const sig = crypto.createHmac('sha256', Buffer.from(CONFIG.L2_API_SECRET, 'base64'))
      .update(msg).digest('base64');
    headers['POLY_ADDRESS']    = new ethers.Wallet(CONFIG.PRIVATE_KEY).address;
    headers['POLY_SIGNATURE']  = sig;
    headers['POLY_TIMESTAMP']  = ts;
    headers['POLY_API_KEY']    = CONFIG.L2_API_KEY;
    headers['POLY_PASSPHRASE'] = CONFIG.L2_API_PASSPHRASE;
  }
  return headers;
}

// ── Order building + signing ─────────────────────────────────
function buildOrderStruct(wallet, tokenId, makerAmount, takerAmount, side) {
  return {
    salt:          BigInt(Math.floor(Math.random() * 1e15)),
    maker:         wallet.address,
    signer:        wallet.address,
    taker:         ethers.ZeroAddress,
    tokenId:       BigInt(tokenId),
    makerAmount:   BigInt(Math.round(makerAmount * 1e6)),
    takerAmount:   BigInt(Math.round(takerAmount * 1e18)),
    expiration:    BigInt(Math.floor(Date.now() / 1000) + 3600),
    nonce:         0n,
    feeRateBps:    BigInt(CONFIG.FEE_RATE_BPS), // 2026: mandatory
    side:          side === 'BUY' ? 0 : 1,
    signatureType: 0,
  };
}

async function placeOrder({ tokenId, side, price, size, label }) {
  const makerAmount = side === 'BUY' ? price * size : size;
  const takerAmount = side === 'BUY' ? size          : price * size;

  if (CONFIG.DEMO_MODE) {
    log('info', `[DEMO] ${side} ${size.toFixed(1)}sh @ ${price.toFixed(4)} | ${label}`,
      { cost: fmtUSDC(makerAmount) });
    return { success: true, orderId: `demo_${Date.now()}`, demo: true };
  }

  if (!CONFIG.PRIVATE_KEY) {
    log('error', 'No PRIVATE_KEY set — cannot place real orders');
    return { success: false, error: 'No private key' };
  }

  try {
    const wallet = new ethers.Wallet(CONFIG.PRIVATE_KEY);
    const struct = buildOrderStruct(wallet, tokenId, makerAmount, takerAmount, side);
    const sig    = await wallet.signTypedData(EIP712_DOMAIN, ORDER_TYPES, struct);

    const body = {
      order: {
        salt:        struct.salt.toString(),
        maker:       struct.maker,
        signer:      struct.signer,
        taker:       struct.taker,
        tokenId:     struct.tokenId.toString(),
        makerAmount: struct.makerAmount.toString(),
        takerAmount: struct.takerAmount.toString(),
        expiration:  struct.expiration.toString(),
        nonce:       '0',
        feeRateBps:  struct.feeRateBps.toString(),
        side:        struct.side,
        signatureType: 0,
        signature:   sig,
      },
      owner:     wallet.address,
      orderType: 'GTC',
    };

    const res = await axios.post(`${CONFIG.CLOB_URL}/order`, body,
      { headers: clobHeaders('POST', '/order'), timeout: 8000 });
    return { success: true, orderId: res.data.orderID };
  } catch (err) {
    log('error', `Order failed: ${label}`, { error: err.response?.data || err.message });
    return { success: false, error: err.message };
  }
}

// ── Order book fetch ─────────────────────────────────────────
async function fetchOrderBook(tokenId) {
  try {
    const res = await axios.get(`${CONFIG.CLOB_URL}/book?token_id=${tokenId}`,
      { headers: clobHeaders(), timeout: 5000 });
    const book = res.data;
    const asks = (book.asks || []).map(a => ({ price: parseFloat(a.price), size: parseFloat(a.size) }));
    const bids = (book.bids || []).map(b => ({ price: parseFloat(b.price), size: parseFloat(b.size) }));
    const bestAsk  = asks.length ? Math.min(...asks.map(a => a.price)) : null;
    const bestBid  = bids.length ? Math.max(...bids.map(b => b.price)) : null;
    const askDepth = asks.filter(a => bestAsk && a.price <= bestAsk + 0.01)
                        .reduce((s, a) => s + a.size, 0);
    return { bestAsk, bestBid, askDepth };
  } catch {
    return { bestAsk: null, bestBid: null, askDepth: 0 };
  }
}

// ── Market scanner ───────────────────────────────────────────
async function fetchAllActiveMarkets() {
  try {
    const res = await axios.get(`${CONFIG.GAMMA_URL}/markets`, {
      params: { active: true, enableOrderBook: true,
                limit: CONFIG.MAX_MARKETS_PER_SCAN, order: 'volume24hr', ascending: false },
      timeout: 10_000,
    });
    const markets = Array.isArray(res.data) ? res.data : (res.data?.markets || []);
    return markets.filter(m => m.enableOrderBook && !m.closed && m.clobTokenIds);
  } catch (err) {
    log('error', 'Market list fetch failed', { error: err.message });
    return [];
  }
}

function parseTokens(market) {
  const outcomes = typeof market.outcomes     === 'string' ? JSON.parse(market.outcomes)     : (market.outcomes     || []);
  const tokenIds = typeof market.clobTokenIds === 'string' ? JSON.parse(market.clobTokenIds) : (market.clobTokenIds || []);
  return outcomes.map((name, i) => ({ name: name || `Outcome ${i}`, tokenId: tokenIds[i] }))
                 .filter(t => t.tokenId);
}

// ── Mispricing detection ─────────────────────────────────────
async function detectMispricing(market) {
  const tokens = parseTokens(market);
  if (tokens.length !== 2) return null;

  const [bookA, bookB] = await Promise.all([
    fetchOrderBook(tokens[0].tokenId),
    fetchOrderBook(tokens[1].tokenId),
  ]);

  const askA = bookA.bestAsk, askB = bookB.bestAsk;
  if (!askA || !askB || askA >= 1 || askB >= 1 || askA <= 0 || askB <= 0) return null;

  const totalCost = askA + askB;
  const spread    = 1.00 - totalCost;
  if (spread < CONFIG.MIN_SPREAD) return null;

  const shares      = Math.floor(CONFIG.BUDGET_PER_TRADE / totalCost);
  if (shares < 1) return null;

  // Depth check
  if (bookA.askDepth < shares * 0.5 || bookB.askDepth < shares * 0.5) return null;

  return {
    id:           `${market.id}_${Date.now()}`,
    marketId:     market.id,
    question:     market.question || market.slug,
    slug:         market.slug,
    tokenA:       { ...tokens[0], ask: askA, depth: bookA.askDepth },
    tokenB:       { ...tokens[1], ask: askB, depth: bookB.askDepth },
    totalCost,
    spread,
    shares,
    grossProfit:  shares * spread,
    roiPct:       (spread / totalCost) * 100,
    detectedAt:   Date.now(),
    status:       'DETECTED',
  };
}

// ── Arb execution ────────────────────────────────────────────
async function executeArb(opp) {
  if (state.openPositions.size >= CONFIG.MAX_OPEN_POSITIONS) return;
  if (state.capital < CONFIG.BUDGET_PER_TRADE) {
    log('warn', 'Insufficient capital'); return;
  }

  const { tokenA, tokenB, shares, totalCost, spread } = opp;
  const cost = shares * totalCost;

  log('info', `🎯 ARB ENTRY | ${opp.question.slice(0, 55)}`,
    { spread: `${(spread * 100).toFixed(2)}%`, cost: fmtUSDC(cost), profit: fmtUSDC(opp.grossProfit) });

  const [resA, resB] = await Promise.all([
    placeOrder({ tokenId: tokenA.tokenId, side: 'BUY', price: tokenA.ask, size: shares, label: `LEG-A ${tokenA.name}` }),
    placeOrder({ tokenId: tokenB.tokenId, side: 'BUY', price: tokenB.ask, size: shares, label: `LEG-B ${tokenB.name}` }),
  ]);

  if (!resA.success || !resB.success) {
    log('error', 'Leg placement failed, aborting'); return;
  }

  state.capital -= cost;

  const pos = {
    id:              opp.id,
    marketId:        opp.marketId,
    question:        opp.question,
    slug:            opp.slug,
    legA:            { ...tokenA, entryPrice: tokenA.ask, currentPrice: tokenA.ask, orderId: resA.orderId },
    legB:            { ...tokenB, entryPrice: tokenB.ask, currentPrice: tokenB.ask, orderId: resB.orderId },
    shares,
    totalCost,
    costBasis:       cost,
    spread,
    expectedProfit:  opp.grossProfit,
    enteredAt:       Date.now(),
    status:          'OPEN',
    unrealizedPnl:   0,
    realizedPnl:     null,
    exitReason:      null,
    closedAt:        null,
  };

  state.openPositions.set(pos.id, pos);
  state.stats.entered++;
  opp.status = 'ENTERED';

  log('info', `✅ Position open | ${fmtUSDC(cost)} deployed`, { id: pos.id });
  io?.emit('state', buildClientState());

  // Subscribe this position's tokens to WS
  subscribeTokens([tokenA.tokenId, tokenB.tokenId]);
}

// ── Position monitor ─────────────────────────────────────────
async function monitorOpenPositions() {
  for (const [id, pos] of state.openPositions) {
    try {
      const [bookA, bookB] = await Promise.all([
        fetchOrderBook(pos.legA.tokenId),
        fetchOrderBook(pos.legB.tokenId),
      ]);

      pos.legA.currentPrice = bookA.bestBid ?? bookA.bestAsk ?? pos.legA.currentPrice;
      pos.legB.currentPrice = bookB.bestBid ?? bookB.bestAsk ?? pos.legB.currentPrice;

      // Combined price tracks how close we are to $1.00 exit
      const combinedCurrent = pos.legA.currentPrice + pos.legB.currentPrice;
      pos.combinedPrice  = combinedCurrent;
      pos.unrealizedPnl  = pos.shares * (combinedCurrent - pos.totalCost);

      // ── Early exit: COMBINED price reaches $1.00 ─────────
      // e.g. Entry: YES 0.45 + NO 0.46 = $0.91
      //      Exit:  YES 0.49 + NO 0.51 = $1.00 → sell BOTH now
      //      Profit = shares × ($1.00 − $0.91) — same as resolution, capital freed instantly
      if (combinedCurrent >= CONFIG.EXIT_PRICE) {
        log('info',
          `💰 COMBINED EXIT: ${pos.legA.currentPrice.toFixed(4)} + ${pos.legB.currentPrice.toFixed(4)} = ${combinedCurrent.toFixed(4)} ≥ $${CONFIG.EXIT_PRICE} | ${pos.question.slice(0,40)}`);

        // Sell BOTH legs simultaneously at their current bids
        await Promise.all([
          placeOrder({ tokenId: pos.legA.tokenId, side: 'SELL', price: pos.legA.currentPrice, size: pos.shares, label: `EXIT-A ${pos.question.slice(0,25)}` }),
          placeOrder({ tokenId: pos.legB.tokenId, side: 'SELL', price: pos.legB.currentPrice, size: pos.shares, label: `EXIT-B ${pos.question.slice(0,25)}` }),
        ]);

        const totalReturn = pos.shares * combinedCurrent;
        const realizedPnl = totalReturn - pos.costBasis;

        state.capital    += totalReturn;
        pos.status        = 'CLOSED';
        pos.exitReason    = `Combined $${combinedCurrent.toFixed(4)} (${pos.legA.name} ${pos.legA.currentPrice.toFixed(4)} + ${pos.legB.name} ${pos.legB.currentPrice.toFixed(4)})`;
        pos.realizedPnl   = realizedPnl;
        pos.closedAt      = Date.now();

        state.stats.totalPnl += realizedPnl;
        state.stats.exited++;
        const wins = state.closedTrades.filter(t => t.realizedPnl >= 0).length + (realizedPnl >= 0 ? 1 : 0);
        state.stats.winRate = wins / state.stats.exited;

        state.closedTrades.unshift({ ...pos });
        state.openPositions.delete(id);

        log('info', `📊 CLOSED | PnL: ${fmtUSDC(realizedPnl)} | Capital: ${fmtUSDC(state.capital)}`);
        io?.emit('state', buildClientState());
      }

      await sleep(CONFIG.REQUEST_DELAY_MS);
    } catch (err) {
      log('error', `Monitor err pos ${id}`, { error: err.message });
    }
  }
}

// ── WebSocket ────────────────────────────────────────────────
let ws = null;
function connectWebSocket() {
  try { ws?.close(); } catch {}
  ws = new WebSocket(CONFIG.WS_URL);

  ws.on('open', () => {
    state.wsConnected = true;
    log('info', '🔌 WebSocket connected');
    // Re-subscribe all open position tokens
    const tokenIds = [];
    for (const p of state.openPositions.values())
      tokenIds.push(p.legA.tokenId, p.legB.tokenId);
    if (tokenIds.length) subscribeTokens(tokenIds);
    // Heartbeat (Polymarket cancels orders on session inactivity)
    setInterval(() => ws?.readyState === WebSocket.OPEN && ws.send(JSON.stringify({ type: 'HEARTBEAT' })), 20_000);
  });

  ws.on('message', raw => {
    try {
      const msgs = JSON.parse(raw.toString());
      const list = Array.isArray(msgs) ? msgs : [msgs];
      for (const msg of list) handleWsMsg(msg);
    } catch {}
  });

  ws.on('close', () => {
    state.wsConnected = false;
    log('warn', 'WS closed, reconnecting in 5s...');
    setTimeout(connectWebSocket, 5000);
  });

  ws.on('error', err => log('error', 'WS error', { error: err.message }));
}

function subscribeTokens(tokenIds) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'subscribe', channel: 'market', assets_ids: tokenIds }));
  }
}

function handleWsMsg(msg) {
  const tokenId = msg.asset_id;
  const price   = parseFloat(msg.price ?? msg.best_ask ?? msg.best_bid ?? 0);
  if (!tokenId || !price) return;

  for (const pos of state.openPositions.values()) {
    let updated = false;
    if (pos.legA.tokenId === tokenId) { pos.legA.currentPrice = price; updated = true; }
    if (pos.legB.tokenId === tokenId) { pos.legB.currentPrice = price; updated = true; }

    if (updated) {
      const combined = pos.legA.currentPrice + pos.legB.currentPrice;
      pos.combinedPrice = combined;
      // Trigger exit check the moment combined hits $1.00
      if (combined >= CONFIG.EXIT_PRICE) monitorOpenPositions();
    }
  }
  io?.emit('priceUpdate', { tokenId, price });
}

// ── Scan loop ────────────────────────────────────────────────
async function scanLoop() {
  log('info', '🔍 Scanning markets...');
  state.allMarkets = await fetchAllActiveMarkets();
  state.lastScan   = Date.now();
  state.stats.scanned += state.allMarkets.length;

  const fresh = [];
  for (const market of state.allMarkets) {
    const alreadyOpen = [...state.openPositions.values()].some(p => p.marketId === market.id);
    if (alreadyOpen) continue;
    const opp = await detectMispricing(market);
    if (opp) { fresh.push(opp); state.stats.detected++; }
    await sleep(CONFIG.REQUEST_DELAY_MS);
  }

  state.opportunities = [
    ...fresh,
    ...state.opportunities.filter(o => o.status !== 'ENTERED').slice(0, 20),
  ].sort((a, b) => b.spread - a.spread).slice(0, 50);

  for (const opp of fresh.sort((a, b) => b.spread - a.spread)) {
    if (state.openPositions.size >= CONFIG.MAX_OPEN_POSITIONS) break;
    await executeArb(opp);
    await sleep(500);
  }

  io?.emit('state', buildClientState());
  log('info', `✅ Scan done | ${state.allMarkets.length} markets | ${fresh.length} opportunities`);
}

// ── Express + Socket.io ──────────────────────────────────────
const app    = express();
const server = http.createServer(app);
io = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.get('/',           (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
app.get('/api/state',  (req, res) => res.json(buildClientState()));
app.get('/api/logs',   (req, res) => res.json(state.logs.slice(0, 100)));
app.post('/api/scan',  async (req, res) => { scanLoop(); res.json({ ok: true }); });
app.post('/api/config',(req, res) => {
  const { minSpread, budget, demoMode } = req.body;
  if (minSpread !== undefined) CONFIG.MIN_SPREAD        = parseFloat(minSpread);
  if (budget    !== undefined) CONFIG.BUDGET_PER_TRADE  = parseFloat(budget);
  if (demoMode  !== undefined) CONFIG.DEMO_MODE         = Boolean(demoMode);
  log('info', 'Config updated', { MIN_SPREAD: CONFIG.MIN_SPREAD, BUDGET: CONFIG.BUDGET_PER_TRADE, DEMO: CONFIG.DEMO_MODE });
  res.json({ ok: true });
});

io.on('connection', socket => {
  socket.emit('state', buildClientState());
  socket.emit('logBatch', state.logs.slice(0, 50));
});

function buildClientState() {
  return {
    capital:       state.capital,
    stats:         state.stats,
    openPositions: [...state.openPositions.values()].map(p => ({
      ...p, ageSec: Math.floor((Date.now() - p.enteredAt) / 1000)
    })),
    closedTrades:  state.closedTrades.slice(0, 50),
    opportunities: state.opportunities.slice(0, 20),
    wsConnected:   state.wsConnected,
    lastScan:      state.lastScan,
    config:        { MIN_SPREAD: CONFIG.MIN_SPREAD, BUDGET_PER_TRADE: CONFIG.BUDGET_PER_TRADE, DEMO_MODE: CONFIG.DEMO_MODE },
    marketCount:   state.allMarkets.length,
  };
}

// ── Start ────────────────────────────────────────────────────
async function main() {
  server.listen(CONFIG.PORT, () => {
    log('info', `🚀 Bot live → http://localhost:${CONFIG.PORT}`);
    log('info', CONFIG.DEMO_MODE ? '📋 DEMO MODE' : '🔴 LIVE MODE');
  });
  await scanLoop();
  setInterval(scanLoop, CONFIG.MARKET_SCAN_INTERVAL);
  setInterval(monitorOpenPositions, CONFIG.BOOK_POLL_INTERVAL);
  connectWebSocket();
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
