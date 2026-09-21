// ============================================================
// PIZZA DA FAMÍLIA - BACKEND v3
// Node.js + Express + SSE (cliente e ADM) + Web Push
// + Persistência em arquivo + Exportação CSV + Push para ADM
// ============================================================

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const webpush = require('web-push');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// ---------- Variáveis de ambiente ----------
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || 'troque-este-token-em-producao';
const VAPID_PUBLIC  = process.env.VAPID_PUBLIC  || '';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE || '';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:contato@pizzadafamilia.com';

// ---------- Web Push ----------
let pushEnabled = false;
if (VAPID_PUBLIC && VAPID_PRIVATE) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC, VAPID_PRIVATE);
  pushEnabled = true;
  console.log('✅ Web Push habilitado');
} else {
  console.log('⚠️  Web Push desabilitado (VAPID não configurado)');
}

// ---------- CORS ----------
const ALLOWED_ORIGINS = [
  'https://pizzadafamilia.netlify.app',
  'https://pizzadafamilia-test.netlify.app',
  'https://admindafamilia.netlify.app',
  'https://admindafamilia-test.netlify.app',
  'http://localhost:3000',
  'http://localhost:5500',
  'http://127.0.0.1:5500'
];
app.use(cors({
  origin: (origin, cb) => {
    if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    console.warn('CORS bloqueado:', origin);
    cb(new Error('Origem não permitida'));
  }
}));
app.use(express.json({ limit: '100kb' }));

// ============================================================
// PERSISTÊNCIA EM ARQUIVO (/tmp — funciona durante a sessão do Render)
// ============================================================
const DATA_DIR = '/tmp/pizza-data';
const ORDERS_FILE = path.join(DATA_DIR, 'orders.json');
const PUSH_ADM_FILE = path.join(DATA_DIR, 'push-adm.json');
const MAX_ORDERS = 500;

function ensureDataDir() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  } catch (e) { console.warn('Erro ao criar dir de dados:', e.message); }
}

function persistOrders() {
  try {
    ensureDataDir();
    const data = {
      orders: Array.from(orders.values()),
      tokens: Array.from(orderTokens.entries()),
      pushSubs: Array.from(pushSubscriptions.entries())
    };
    fs.writeFileSync(ORDERS_FILE, JSON.stringify(data), 'utf8');
  } catch (e) { console.warn('Erro ao persistir pedidos:', e.message); }
}

function restoreOrders() {
  try {
    ensureDataDir();
    if (!fs.existsSync(ORDERS_FILE)) return;
    const raw = fs.readFileSync(ORDERS_FILE, 'utf8');
    const parsed = JSON.parse(raw);

    // Formato novo (objeto com orders, tokens, pushSubs)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      (parsed.orders || []).forEach(o => { if (o && o.id) orders.set(o.id, o); });
      (parsed.tokens || []).forEach(([id, tk]) => orderTokens.set(id, tk));
      (parsed.pushSubs || []).forEach(([id, sub]) => pushSubscriptions.set(id, sub));
      console.log(`📦 Restaurado: ${orders.size} pedidos, ${orderTokens.size} tokens, ${pushSubscriptions.size} push subs`);
    }
    // Formato antigo (array simples) — compatibilidade
    else if (Array.isArray(parsed)) {
      parsed.forEach(o => { if (o && o.id) orders.set(o.id, o); });
      console.log(`📦 ${orders.size} pedidos restaurados (formato antigo, sem tokens)`);
    }
  } catch (e) { console.warn('Erro ao restaurar:', e.message); }
}

function persistAdmPush() {
  try {
    ensureDataDir();
    const arr = Array.from(admPushSubscriptions.values());
    fs.writeFileSync(PUSH_ADM_FILE, JSON.stringify(arr), 'utf8');
  } catch (e) { console.warn('Erro ao persistir push ADM:', e.message); }
}

function restoreAdmPush() {
  try {
    ensureDataDir();
    if (!fs.existsSync(PUSH_ADM_FILE)) return;
    const raw = fs.readFileSync(PUSH_ADM_FILE, 'utf8');
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return;
    arr.forEach(sub => {
      if (sub && sub.endpoint) admPushSubscriptions.set(sub.endpoint, sub);
    });
    console.log(`📦 ${admPushSubscriptions.size} subscriptions ADM restauradas`);
  } catch (e) { console.warn('Erro ao restaurar push ADM:', e.message); }
}

// ============================================================
// ESTADO
// ============================================================
const orders = new Map();                    // id -> order
const orderTokens = new Map();               // id -> token do cliente
const clientStreams = new Map();             // id -> Set(res)
const adminStreams = new Set();              // Set(res)
const pushSubscriptions = new Map();         // id -> subscription (cliente)
const admPushSubscriptions = new Map();      // endpoint -> subscription (ADM)

const TIME_LIMITS = {
  'Centro': 20,
  'Novo Horizonte': 25,
  'Baixa': 25,
  'Vila': 25,
  'Tamboril': 40,
  'Boi Morto': 40,
  "Buraco D'água": 40,
  'Pitombeira': 40,
  'Retirada no local': 15
};
const DEFAULT_TIME_LIMIT = 30;

// ============================================================
// HELPERS
// ============================================================
function sendToClientStreams(orderId, payload) {
  const streams = clientStreams.get(orderId);
  if (!streams) return;
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of streams) {
    try { res.write(data); } catch { streams.delete(res); }
  }
}

function broadcastToAdmins(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of adminStreams) {
    try { res.write(data); } catch { adminStreams.delete(res); }
  }
}

async function sendWebPush(orderId, payload) {
  if (!pushEnabled) return;
  const sub = pushSubscriptions.get(orderId);
  if (!sub) return;
  try {
    await webpush.sendNotification(sub, JSON.stringify(payload));
    console.log(`📲 Push cliente enviado (${orderId.slice(0,6)})`);
  } catch (err) {
    console.warn('Falha no push cliente:', err.statusCode);
    if (err.statusCode === 404 || err.statusCode === 410) {
      pushSubscriptions.delete(orderId);
    }
  }
}

async function sendAdmPush(payload) {
  if (!pushEnabled || admPushSubscriptions.size === 0) return;
  const data = JSON.stringify(payload);
  const toDelete = [];
  for (const [endpoint, sub] of admPushSubscriptions) {
    try {
      await webpush.sendNotification(sub, data);
      console.log('📲 Push ADM enviado');
    } catch (err) {
      console.warn('Falha push ADM:', err.statusCode);
      if (err.statusCode === 404 || err.statusCode === 410) toDelete.push(endpoint);
    }
  }
  toDelete.forEach(ep => admPushSubscriptions.delete(ep));
  if (toDelete.length) persistAdmPush();
}

function notifyClient(orderId, status, message) {
  const payload = { type: 'status', status, message, timestamp: new Date().toISOString() };
  sendToClientStreams(orderId, payload);
  sendWebPush(orderId, { title: 'Pizza da Família 🍕', body: message, url: '/' });
}

function requireAdmin(req, res, next) {
  const token = req.headers['x-admin-token'] || req.query.token;
  if (token !== ADMIN_TOKEN) return res.status(401).json({ error: 'Não autorizado' });
  next();
}

function validateOrder(body) {
  const errors = [];
  if (!body || typeof body !== 'object') errors.push('Payload inválido');
  if (!body.customer?.name) errors.push('Nome do cliente é obrigatório');
  if (!body.customer?.phone) errors.push('Telefone é obrigatório');
  if (!Array.isArray(body.items) || body.items.length === 0) errors.push('Pedido sem itens');
  if (typeof body.total !== 'number') errors.push('Total inválido');

  const isRetirada = body.deliveryType === 'retirada';
  if (!isRetirada && !body.customer?.neighborhood) {
    errors.push('Bairro é obrigatório para entrega');
  }
  return errors;
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v).replace(/"/g, '""');
  return `"${s}"`;
}

// ============================================================
// ROTAS PÚBLICAS (cliente)
// ============================================================

app.post('/api/orders', (req, res) => {
  const errors = validateOrder(req.body);
  if (errors.length > 0) {
    return res.status(400).json({ error: 'Dados inválidos', details: errors });
  }

  const orderId = crypto.randomUUID();
  const clientToken = crypto.randomBytes(24).toString('hex');

  const isRetirada = req.body.deliveryType === 'retirada';
  const timeLimit = isRetirada
    ? TIME_LIMITS['Retirada no local']
    : (TIME_LIMITS[req.body.customer?.neighborhood] || DEFAULT_TIME_LIMIT);

  const order = {
    id: orderId,
    createdAt: new Date().toISOString(),
    status: 'novo',
    timeLimitMinutes: timeLimit,
    deliveryType: req.body.deliveryType || 'entrega',
    customer: req.body.customer,
    items: req.body.items,
    payment: req.body.payment,
    change: req.body.change || '',
    subtotal: req.body.subtotal,
    deliveryFee: req.body.deliveryFee,
    total: req.body.total
  };

  orders.set(orderId, order);
  orderTokens.set(orderId, clientToken);

  // Limpa pedidos antigos entregues
  if (orders.size > MAX_ORDERS) {
    const toDelete = [];
    for (const [id, o] of orders) {
      if (o.status === 'entregue') toDelete.push(id);
      if (toDelete.length >= 50) break;
    }
    toDelete.forEach(id => {
      orders.delete(id);
      orderTokens.delete(id);
      clientStreams.delete(id);
      pushSubscriptions.delete(id);
    });
  }

  persistOrders();
  broadcastToAdmins({ type: 'new_order', order });

  // Push para o ADM (mesmo que o painel esteja fechado)
  sendAdmPush({
    title: '🍕 Novo pedido!',
    body: `${order.customer.name} • ${isRetirada ? 'Retirada' : order.customer.neighborhood} • R$ ${order.total.toFixed(2)}`,
    url: '/'
  });

  const local = isRetirada ? 'Retirada' : (order.customer.neighborhood || '—');
  console.log(`[NOVO] ${order.customer.name} • ${local} • R$ ${order.total.toFixed(2)}`);

  res.status(201).json({
    ok: true,
    orderId,
    clientToken,
    timeLimitMinutes: timeLimit,
    vapidPublicKey: pushEnabled ? VAPID_PUBLIC : null
  });
});

app.get('/api/orders/:id/stream', (req, res) => {
  const { id } = req.params;
  const token = req.query.token;
  const expected = orderTokens.get(id);
  if (!expected || token !== expected) {
    return res.status(401).json({ error: 'Token inválido' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const order = orders.get(id);
  if (order) {
    res.write(`data: ${JSON.stringify({
      type: 'init',
      status: order.status,
      timeLimitMinutes: order.timeLimitMinutes,
      createdAt: order.createdAt
    })}\n\n`);
  }

  const heartbeat = setInterval(() => res.write(': ping\n\n'), 25000);
  if (!clientStreams.has(id)) clientStreams.set(id, new Set());
  clientStreams.get(id).add(res);

  req.on('close', () => {
    clearInterval(heartbeat);
    const set = clientStreams.get(id);
    if (set) {
      set.delete(res);
      if (set.size === 0) clientStreams.delete(id);
    }
  });
});

app.post('/api/push/subscribe', (req, res) => {
  const { orderId, clientToken, subscription } = req.body;
  const expected = orderTokens.get(orderId);
  if (!expected || clientToken !== expected) {
    return res.status(401).json({ error: 'Token inválido' });
  }
  if (!subscription || !subscription.endpoint) {
    return res.status(400).json({ error: 'Subscription inválida' });
  }
  pushSubscriptions.set(orderId, subscription);
  persistOrders();
  res.json({ ok: true });
});

app.get('/api/vapid-public-key', (req, res) => {
  res.json({ key: pushEnabled ? VAPID_PUBLIC : null });
});

// ============================================================
// ROTAS PROTEGIDAS (ADM)
// ============================================================

app.get('/api/admin/orders', requireAdmin, (req, res) => {
  const list = Array.from(orders.values())
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json(list);
});

app.get('/api/admin/orders/stream', requireAdmin, (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const initial = Array.from(orders.values())
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.write(`data: ${JSON.stringify({ type: 'init', orders: initial })}\n\n`);

  const heartbeat = setInterval(() => res.write(': ping\n\n'), 25000);
  adminStreams.add(res);
  console.log(`[SSE-ADM] conectado. Total: ${adminStreams.size}`);

  req.on('close', () => {
    clearInterval(heartbeat);
    adminStreams.delete(res);
  });
});

app.patch('/api/admin/orders/:id/status', requireAdmin, (req, res) => {
  const { id } = req.params;
  const { status } = req.body;

  const validStatuses = ['novo', 'preparando', 'saiu_entrega', 'entregue'];
  if (!validStatuses.includes(status)) {
    return res.status(400).json({ error: 'Status inválido' });
  }

  const order = orders.get(id);
  if (!order) return res.status(404).json({ error: 'Pedido não encontrado' });

  order.status = status;
  order.updatedAt = new Date().toISOString();

  persistOrders();
  broadcastToAdmins({ type: 'order_updated', order });

  const isRetirada = order.deliveryType === 'retirada';
  const messages = {
    preparando:    isRetirada ? 'Seu pedido está sendo preparado! 🔥' : 'Sua pizza está no forno! 🔥',
    saiu_entrega:  isRetirada ? 'Seu pedido está pronto para retirada! 🏠' : 'Sua pizza saiu para entrega! 🛵',
    entregue:      isRetirada ? 'Pedido retirado. Bom apetite! 🍕' : 'Pedido entregue. Bom apetite! 🍕',
    novo:          'Pedido recebido!'
  };
  notifyClient(id, status, messages[status] || `Status: ${status}`);

  console.log(`[STATUS] ${id.slice(0,6)} → ${status}`);
  res.json({ ok: true, order });
});

app.post('/api/admin/orders/:id/message', requireAdmin, (req, res) => {
  const { id } = req.params;
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'Mensagem vazia' });
  const order = orders.get(id);
  if (!order) return res.status(404).json({ error: 'Pedido não encontrado' });

  sendToClientStreams(id, { type: 'message', text });
  sendWebPush(id, { title: 'Pizza da Família 🍕', body: text, url: '/' });
  res.json({ ok: true });
});

// ---------- Nova: limpar todos os pedidos (Encerrar expediente) ----------
app.delete('/api/admin/orders', requireAdmin, (req, res) => {
  const count = orders.size;
  orders.clear();
  orderTokens.clear();
  clientStreams.clear();
  pushSubscriptions.clear();
  persistOrders();

  // Notifica todos os ADMs conectados
  broadcastToAdmins({ type: 'all_cleared' });

  console.log(`[LIMPEZA] ${count} pedidos removidos pelo ADM`);
  res.json({ ok: true, cleared: count });
});

// ---------- Nova: exportar CSV ----------
app.get('/api/admin/orders/export', requireAdmin, (req, res) => {
  const list = Array.from(orders.values())
    .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));

  const headers = [
    'ID', 'Data', 'Hora', 'Status', 'Tipo',
    'Cliente', 'Telefone', 'Bairro', 'Rua', 'Número', 'Complemento', 'Referência',
    'Itens', 'Qtd Itens', 'Subtotal', 'Frete', 'Total',
    'Pagamento', 'Troco',
    'Criado em', 'Atualizado em', 'Tempo total (min)'
  ];

  const rows = list.map(o => {
    const created = new Date(o.createdAt);
    const updated = o.updatedAt ? new Date(o.updatedAt) : null;
    const tempoMin = updated ? Math.round((updated - created) / 60000) : '';

    const itens = (o.items || []).map(i => {
      let s = `${i.quantity}x ${i.type === 'pizza' ? 'Pizza ' : ''}${i.name}`;
      if (i.size) s += ` (${i.size})`;
      if (i.flavors && i.flavors.length > 1) s += ` [${i.flavors.join(' + ')}]`;
      if (i.border && i.border !== 'Sem borda') s += ` +borda ${i.border}`;
      if (i.obs) s += ` obs: ${i.obs}`;
      return s;
    }).join(' | ');

    const qtdItens = (o.items || []).reduce((s, i) => s + (i.quantity || 0), 0);

    return [
      o.id,
      created.toLocaleDateString('pt-BR'),
      created.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }),
      o.status,
      o.deliveryType || 'entrega',
      o.customer?.name || '',
      o.customer?.phone || '',
      o.customer?.neighborhood || '',
      o.customer?.street || '',
      o.customer?.number || '',
      o.customer?.complement || '',
      o.customer?.reference || '',
      itens,
      qtdItens,
      (o.subtotal || 0).toFixed(2).replace('.', ','),
      (o.deliveryFee || 0).toFixed(2).replace('.', ','),
      (o.total || 0).toFixed(2).replace('.', ','),
      o.payment || '',
      o.change || '',
      o.createdAt,
      o.updatedAt || '',
      tempoMin
    ].map(csvEscape).join(';');
  });

  const csv = '\uFEFF' + [headers.map(csvEscape).join(';'), ...rows].join('\r\n');
  const filename = `relatorio-pizza-${new Date().toISOString().slice(0,10)}.csv`;

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(csv);

  console.log(`[EXPORT] CSV gerado com ${list.length} pedidos`);
});

// ---------- Push do ADM ----------
app.post('/api/admin/push/subscribe', requireAdmin, (req, res) => {
  const { subscription } = req.body;
  if (!subscription || !subscription.endpoint) {
    return res.status(400).json({ error: 'Subscription inválida' });
  }
  admPushSubscriptions.set(subscription.endpoint, subscription);
  persistAdmPush();
  console.log(`[PUSH-ADM] Subscription registrada (${admPushSubscriptions.size} total)`);
  res.json({ ok: true });
});

// ============================================================
// HEALTH
// ============================================================
app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    orders: orders.size,
    adminStreams: adminStreams.size,
    clientStreams: clientStreams.size,
    admPushSubs: admPushSubscriptions.size,
    push: pushEnabled
  });
});

// ============================================================
// START
// ============================================================
restoreOrders();
restoreAdmPush();

app.listen(PORT, () => {
  console.log(`🍕 Backend Pizza da Família v3 na porta ${PORT}`);
});
