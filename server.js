// ============================================================
// PIZZA DA FAMÍLIA - BACKEND v2
// Node.js + Express + SSE (cliente e ADM) + Web Push
// ============================================================

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const webpush = require('web-push');

const app = express();
const PORT = process.env.PORT || 3000;

// ---------- Variáveis de ambiente ----------
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || 'troque-este-token-em-producao';
const VAPID_PUBLIC  = process.env.VAPID_PUBLIC  || '';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE || '';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:contato@pizzadafamilia.com';

// ---------- Configurar Web Push (se as chaves existirem) ----------
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

// ---------- Estado em memória ----------
const orders = new Map();
const orderTokens = new Map();
const clientStreams = new Map();
const adminStreams = new Set();
const pushSubscriptions = new Map();
const MAX_ORDERS = 200;

// ---------- Tempo alvo por bairro (minutos) ----------
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
    console.log(`📲 Push enviado para pedido ${orderId.slice(0,6)}`);
  } catch (err) {
    console.warn('Falha no push:', err.statusCode);
    if (err.statusCode === 404 || err.statusCode === 410) {
      pushSubscriptions.delete(orderId);
    }
  }
}

function notifyClient(orderId, status, message) {
  const payload = {
    type: 'status',
    status,
    message,
    timestamp: new Date().toISOString()
  };
  sendToClientStreams(orderId, payload);
  sendWebPush(orderId, {
    title: 'Pizza da Família 🍕',
    body: message,
    url: '/'
  });
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

  // Bairro só é obrigatório se for ENTREGA (não retirada)
  const isRetirada = body.deliveryType === 'retirada';
  if (!isRetirada && !body.customer?.neighborhood) {
    errors.push('Bairro é obrigatório para entrega');
  }
  return errors;
}

// ============================================================
// ROTAS PÚBLICAS (cliente)
// ============================================================

// ---------- POST /api/orders — cria pedido ----------
app.post('/api/orders', (req, res) => {
  const errors = validateOrder(req.body);
  if (errors.length > 0) {
    return res.status(400).json({ error: 'Dados inválidos', details: errors });
  }

  const orderId = crypto.randomUUID();
  const clientToken = crypto.randomBytes(24).toString('hex');

  // ---------- Tempo alvo: retirada ou por bairro ----------
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
      if (toDelete.length >= 20) break;
    }
    toDelete.forEach(id => {
      orders.delete(id);
      orderTokens.delete(id);
      clientStreams.delete(id);
      pushSubscriptions.delete(id);
    });
  }

  broadcastToAdmins({ type: 'new_order', order });

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

// ---------- GET /api/orders/:id/stream — SSE do cliente ----------
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
  console.log(`[SSE-CLIENTE] Pedido ${id.slice(0,6)} conectado`);

  req.on('close', () => {
    clearInterval(heartbeat);
    const set = clientStreams.get(id);
    if (set) {
      set.delete(res);
      if (set.size === 0) clientStreams.delete(id);
    }
    console.log(`[SSE-CLIENTE] Pedido ${id.slice(0,6)} desconectado`);
  });
});

// ---------- POST /api/push/subscribe — registro de push ----------
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
  console.log(`[PUSH] Subscription registrada para pedido ${orderId.slice(0,6)}`);
  res.json({ ok: true });
});

// ---------- GET /api/vapid-public-key ----------
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
    console.log(`[SSE-ADM] desconectado. Total: ${adminStreams.size}`);
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

  broadcastToAdmins({ type: 'order_updated', order });

  const isRetirada = order.deliveryType === 'retirada';
  const messages = {
    preparando:    isRetirada ? 'Seu pedido está sendo preparado! 🔥' : 'Sua pizza está no forno! 🔥',
    saiu_entrega:  isRetirada ? 'Seu pedido está pronto para retirada! 🏠' : 'Sua pizza saiu para entrega! 🛵',
    entregue:      isRetirada ? 'Pedido retirado. Bom apetite! 🍕' : 'Pedido entregue. Bom apetite! 🍕',
    novo:          'Pedido recebido!'
  };
  notifyClient(id, status, messages[status] || `Status: ${status}`);

  console.log(`[STATUS] Pedido ${id.slice(0,6)} → ${status}`);
  res.json({ ok: true, order });
});

app.post('/api/admin/orders/:id/message', requireAdmin, (req, res) => {
  const { id } = req.params;
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'Mensagem vazia' });

  const order = orders.get(id);
  if (!order) return res.status(404).json({ error: 'Pedido não encontrado' });

  sendToClientStreams(id, { type: 'message', text });
  sendWebPush(id, {
    title: 'Pizza da Família 🍕',
    body: text,
    url: '/'
  });
  res.json({ ok: true });
});

// ============================================================
// HEALTH CHECK
// ============================================================
app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    orders: orders.size,
    adminStreams: adminStreams.size,
    clientStreams: clientStreams.size,
    push: pushEnabled
  });
});

// ============================================================
// START
// ============================================================
app.listen(PORT, () => {
  console.log(`🍕 Backend Pizza da Família na porta ${PORT}`);
});
