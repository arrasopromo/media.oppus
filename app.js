require("dotenv").config();

const express = require("express");
const session = require("express-session");
const path = require("path");
const crypto = require("crypto");
const { HttpsProxyAgent } = require("https-proxy-agent");
const PQueue = require("p-queue").default;
const LinkManager = require("./linkManager");
const GoogleDriveManager = require("./googleDriveManager");
const BaserowManager = require("./baserowManager");
const axios = require('axios');
const fs = require('fs');
const EfiPay = require('gn-api-sdk-node');
const { validatePrice, verifyPrice } = require('./pricing');

const app = express();
app.set("trust proxy", true); // Confiar em cabeçalhos de proxy

// Configuração de sessão
app.use(session({
    secret: "agencia-oppus-secret-key",
    resave: false,
    saveUninitialized: true,
    cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 } // 24 horas
}));

// Middleware para parsing de JSON e URL encoded
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  const isSandbox = process.env.EFI_SANDBOX === 'true';
  res.locals.EFI_SANDBOX = isSandbox;
  res.locals.EFI_PAYEE_CODE = process.env.EFI_PAYEE_CODE || '';
  const official = isSandbox
    ? 'https://sandbox.efipay.com.br/v1/payment-token.js'
    : 'https://payment-token.efipay.com.br/v1/payment-token.js';
  res.locals.EFI_SCRIPT_URL = official;
  res.locals.EFI_SCRIPT_FALLBACK_URL = 'https://cdn.jsdelivr.net/gh/efipay/js-payment-token-efi/dist/payment-token-efi.min.js';
  next();
});

// Middleware melhorado para capturar IP real (útil quando atrás de proxy)
app.use((req, res, next) => {
    // Tentar diferentes headers para capturar o IP real
    let ip = req.headers['x-forwarded-for'] || 
             req.headers['x-real-ip'] || 
             req.headers['cf-connecting-ip'] || 
             req.headers['x-client-ip'] || 
             req.headers['x-forwarded'] || 
             req.headers['forwarded-for'] || 
             req.headers['forwarded'] ||
             req.connection.remoteAddress || 
             req.socket.remoteAddress || 
             req.ip || 
             'unknown';
    
    // Se x-forwarded-for contém múltiplos IPs, pegar o primeiro
    if (ip && ip.includes(',')) {
        ip = ip.split(',')[0].trim();
    }
    
    // Normalizar IPv6 mapeado para IPv4
    const ipNormalized = ip.replace('::ffff:', '');
    
    // Atribuir o IP real à requisição
    req.realIP = ipNormalized;
    req.ip = ipNormalized; // Também sobrescrever req.ip
    
    next();
});

// Middleware de autenticação Admin
const requireAdmin = (req, res, next) => {
    if (req.session && req.session.adminUser) {
        return next();
    }
    if (req.xhr || (req.headers.accept && req.headers.accept.indexOf('json') > -1)) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    return res.redirect('/login');
};

// Helper: Hash password (SHA-256)
function hashPassword(password) {
    return crypto.createHash('sha256').update(password).digest('hex');
}

function normalizeProviderResponseData(data) {
    if (!data) return {};
    if (typeof data === 'object') return data;
    if (typeof data === 'string') {
        const t = data.trim();
        if (!t) return {};
        try { return JSON.parse(t); } catch (_) { return { raw: data }; }
    }
    return { raw: data };
}

function extractProviderOrderId(data) {
    const d = normalizeProviderResponseData(data);
    const candidates = [
        d.order, d.id, d.order_id, d.orderId,
        d.data && d.data.order, d.data && d.data.id, d.data && d.data.order_id, d.data && d.data.orderId,
        d.result && d.result.order, d.result && d.result.id, d.result && d.result.order_id, d.result && d.result.orderId,
        d.response && d.response.order, d.response && d.response.id, d.response && d.response.order_id, d.response && d.response.orderId
    ];
    for (const v of candidates) {
        if (typeof v === 'number' && Number.isFinite(v) && v > 0) return String(v);
        if (typeof v === 'string') {
            const t = v.trim();
            if (t && t.toLowerCase() !== 'unknown' && t.toLowerCase() !== 'null' && t.toLowerCase() !== 'undefined') return t;
        }
    }
    return null;
}

async function postFormWithRetry(url, formBodyString, timeoutMs, maxAttempts) {
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    let lastErr = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await axios.post(url, formBodyString, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: timeoutMs });
        } catch (err) {
            lastErr = err;
            const msg = String(err?.message || '').toLowerCase();
            const code = String(err?.code || '').toLowerCase();
            const status = err?.response?.status || 0;
            const isTimeout = msg.includes('timeout') || code === 'ecconnaborted';
            const isRetryable = isTimeout || !status || status >= 500;
            if (attempt < maxAttempts && isRetryable) {
                await sleep(1200 * attempt);
                continue;
            }
            throw err;
        }
    }
    throw lastErr || new Error('request_failed');
}

// Ensure default admin exists on startup
async function ensureAdminUser() {
    try {
        const { getCollection } = require('./mongodbClient');
        const admins = await getCollection('admins');
        const adminUser = await admins.findOne({ username: 'admin' });
        
        if (!adminUser) {
            console.log('Creating default admin user...');
            await admins.insertOne({
                username: 'admin',
                password: hashPassword('Rr12415721@'), // Initial password
                role: 'admin',
                createdAt: new Date()
            });
            console.log('✅ Default admin user created.');
        }
    } catch (e) {
        console.error('❌ Error ensuring admin user:', e);
    }
}
// Run the check
ensureAdminUser();

app.get('/login', (req, res) => {
    if (req.session && req.session.adminUser) return res.redirect('/painel');
    res.render('login');
});

app.post('/login', async (req, res) => {
    try {
        const { username, password } = req.body;
        const { getCollection } = require('./mongodbClient');
        const admins = await getCollection('admins');
        const user = await admins.findOne({ username });

        if (user && user.password === hashPassword(password)) {
            req.session.adminUser = { username: user.username, role: user.role };
            return res.json({ success: true });
        }
        return res.status(401).json({ success: false, error: 'Credenciais inválidas' });
    } catch (e) {
        console.error('Login error:', e);
        return res.status(500).json({ success: false, error: 'Erro interno' });
    }
});

app.get('/logout', (req, res) => {
    req.session.destroy();
    res.redirect('/login');
});

// Configurar view engine ANTES de qualquer render
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use((req, res, next) => { res.locals.PIXEL_ID = process.env.PIXEL_ID || ''; next(); });

// Configuração Efí Bank (Homologação/Produção)
const efiOptions = {
    sandbox: process.env.EFI_SANDBOX === 'true',
    client_id: process.env.EFI_CLIENT_ID_HM,
    client_secret: process.env.EFI_CLIENT_SECRET_HM,
    certificate: path.join(__dirname, 'certs', 'productionCertificate.p12'), // Opcional para cartão se não usar PIX com certificado, mas o SDK pode exigir
    pem: false // Para cartão geralmente não precisa de certificado mTLS obrigatório como Pix, mas vamos ver
};
// Nota: Para cartão de crédito, o SDK usa apenas client_id e client_secret para OAuth.
// Se der erro de certificado, ajustamos.

const cookieProfiles = require("./instagramProfiles.json");

// Adicionar campos de controle de erro e tempo de reuso aos perfis
cookieProfiles.forEach(profile => {
    profile.errorCount = profile.errorCount || 0;
    profile.lastUsed = profile.lastUsed || 0;
    profile.disabledUntil = profile.disabledUntil || 0;
});

console.log(`📊 Perfis de cookie carregados: ${cookieProfiles.length} perfis disponíveis`);

// Lock de cookies para evitar uso simultâneo
const cookieLocks = new Set();

function isCookieLocked(cookieId) {
    return cookieLocks.has(cookieId);
}

function lockCookie(cookieId) {
    cookieLocks.add(cookieId);
}

function unlockCookie(cookieId) {
  cookieLocks.delete(cookieId);
}



// Dispatcher de serviços pendentes (organicos -> Fornecedor Social)
async function dispatchPendingOrganicos() {
  try {
    const col = await getCollection('checkout_orders');
    const cursor = await col.find({
      $and: [
        { $or: [ { status: 'pago' }, { 'woovi.status': 'pago' } ] },
        { $or: [ { tipo: 'organicos' }, { tipoServico: 'organicos' } ] },
        { 
          $and: [
            { 'fornecedor_social.orderId': { $exists: false } },
            { 'fornecedor_social.status': { $ne: 'processing' } },
            { 'fornecedor_social.status': { $ne: 'created' } }
          ]
        }
      ]
    }).sort({ createdAt: -1 }).limit(5);
    const pending = await cursor.toArray();
    for (const record of pending) {
      try {
        // SECURITY CHECK: Verify if paid value matches expected value
        if (record.expectedValueCents && record.valueCents && record.valueCents < record.expectedValueCents) {
            console.warn(`🛑 Dispatcher BLOCKED for order ${record._id}: Paid value (${record.valueCents}) < Expected (${record.expectedValueCents}).`);
            await col.updateOne({ _id: record._id }, { 
                $set: { 
                    status: 'divergent_value', 
                    mismatchDetails: { reason: 'value_underpaid_at_dispatcher', expected: record.expectedValueCents, paid: record.valueCents } 
                } 
            });
            continue;
        }

        // Atomic lock attempt
        const lockUpdate = await col.updateOne(
            { 
                _id: record._id, 
                'fornecedor_social.status': { $ne: 'processing' },
                'fornecedor_social.orderId': { $exists: false }
            },
            { $set: { 'fornecedor_social.status': 'processing', 'fornecedor_social.attemptedAt': new Date().toISOString() } }
        );
        
        if (lockUpdate.modifiedCount === 0) continue;

        const additionalInfoMap = record.additionalInfoMapPaid || (Array.isArray(record.additionalInfoPaid) ? record.additionalInfoPaid.reduce((acc, it) => { acc[it.key] = it.value; return acc; }, {}) : {});
        const tipo = additionalInfoMap['tipo_servico'] || record.tipo || record.tipoServico || '';
        const categoriaServ = String(additionalInfoMap['categoria_servico'] || '').toLowerCase();
        if (categoriaServ === 'curtidas' || categoriaServ === 'visualizacoes') continue;
        if (!/organicos/i.test(String(tipo))) continue;
        const qtdBase = Number(additionalInfoMap['quantidade'] || record.quantidade || record.qtd || 0) || 0;
        const instaUserRaw = additionalInfoMap['instagram_username'] || record.instagramUsername || record.instauser || '';
        const instaUser = (/^https?:\/\//i.test(String(instaUserRaw))) ? String(instaUserRaw) : `https://instagram.com/${String(instaUserRaw)}`;
        const bumpsStr0 = additionalInfoMap['order_bumps'] || (record.additionalInfoPaid || []).find(it => it && it.key === 'order_bumps')?.value || '';
        let upgradeAdd = 0;
        if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000 && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) upgradeAdd = 1000;
        else if (/(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
          const map = { 150: 150, 500: 200, 1000: 1000, 3000: 1000, 5000: 2500, 10000: 5000 };
          upgradeAdd = map[qtdBase] || 0;
        }
        const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
        const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
        if (!keyFS || !instaUser || !qtd) {
          console.log('ℹ️ Dispatcher FS: ignorando', { hasKeyFS: !!keyFS, instaUser, qtd });
          // Unlock if invalid
          await col.updateOne({ _id: record._id }, { $set: { 'fornecedor_social.status': 'invalid_data' } });
          continue;
        }
        const axios = require('axios');
        const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
        const payloadFS = new URLSearchParams({ key: keyFS, action: 'add', service: String(serviceFS), link: String(instaUser), quantity: String(qtd) });
        console.log('➡️ Dispatcher enviando FornecedorSocial', { url: 'https://fornecedorsocial.com/api/v2', payload: Object.fromEntries(payloadFS.entries()) });
        const respFS = await axios.post('https://fornecedorsocial.com/api/v2', payloadFS.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
        const dataFS = respFS.data || {};
        const orderIdFS = dataFS.order || dataFS.id || null;
        await col.updateOne({ _id: record._id }, { $set: { fornecedor_social: { orderId: orderIdFS, status: orderIdFS ? 'created' : 'unknown', requestPayload: { service: serviceFS, link: instaUser, quantity: qtd }, response: dataFS, requestedAt: new Date().toISOString() } } });
        console.log('✅ Dispatcher FornecedorSocial', { status: respFS.status, orderIdFS });
      } catch (err) {
        console.error('❌ Dispatcher FS erro', err?.response?.data || err?.message || String(err));
        await col.updateOne({ _id: record._id }, { $set: { 'fornecedor_social.status': 'error', 'fornecedor_social.error': err?.message || String(err) } });
      }
    }
  } catch (e) {
    console.error('❌ Dispatcher FS falhou', e?.message || String(e));
  }
}

setInterval(dispatchPendingOrganicos, 60000);

let globalIndex = 0; // Variável global para round-robin

const instagramQueue = new PQueue({ concurrency: cookieProfiles.length > 0 ? cookieProfiles.length : 3 }); // Concorrência dinâmica baseada no número de perfis

// Função para agendar exclusão da imagem do Google Drive após 5 minutos
function scheduleDeleteGoogleDriveImage(fileId) {
  if (!fileId) return;
  setTimeout(async () => {
    try {
      await driveManager.deleteFile(fileId);
      console.log(`🗑️ Imagem do Google Drive ${fileId} excluída após 5 minutos.`);
    } catch (err) {
      console.error('Erro ao excluir imagem do Google Drive:', err.message || err);
    }
  }, 5 * 60 * 1000); // 5 minutos
}

// Função para baixar e servir imagem localmente
async function downloadAndServeImage(imageUrl, username, httpsAgent = null) {
  try {
    // Criar pasta de cache se não existir
    const cacheDir = path.join(__dirname, 'temp_images');
    if (!fs.existsSync(cacheDir)) {
      fs.mkdirSync(cacheDir, { recursive: true });
    }
    
    // Nome do arquivo local
    const fileName = `${username}_${Date.now()}.jpg`;
    const filePath = path.join(cacheDir, fileName);
    
    // Baixar imagem
    const response = await axios.get(imageUrl, {
      responseType: 'arraybuffer',
      timeout: 10000,
      httpsAgent: httpsAgent || undefined
    });
    
    // Salvar localmente
    fs.writeFileSync(filePath, response.data);
    
    // Agendar exclusão após 5 minutos
    setTimeout(() => {
      try {
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
          console.log(`🗑️ Imagem local excluída: ${fileName}`);
        }
      } catch (err) {
        console.error('Erro ao excluir imagem local:', err.message);
      }
    }, 5 * 60 * 1000); // 5 minutos
    
    return `/temp-images/${fileName}`;
  } catch (error) {
    console.error('Erro ao baixar imagem:', error.message);
    return null;
  }
}

// Função utilitária para gerar fingerprint
function generateFingerprint(ip, userAgent) {
    return crypto.createHash('md5').update(ip + '|' + userAgent).digest('hex');
}

// Função para buscar posts do Instagram e extrair IDs (Wrapper para fetchInstagramRecentPosts)
async function fetchInstagramPosts(username) {
    try {
        console.log(`🔍 Buscando posts do Instagram para: @${username} (via API/Cookies)`);
        // Reutiliza a função otimizada com suporte a cookies paralelos
        const result = await fetchInstagramRecentPosts(username);
        return result;
    } catch (error) {
        console.error('Erro em fetchInstagramPosts:', error);
        return { success: false, error: error.message };
    }
}

// Rota para buscar informações do perfil (usada no checkout novo)
app.get('/api/instagram/info', async (req, res) => {
    try {
        const username = req.query.username;
        if (!username) {
            return res.status(400).json({ success: false, error: 'Username é obrigatório' });
        }

        const userAgent = req.get('User-Agent') || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
        const ip = req.ip || '127.0.0.1';

        const result = await verifyInstagramProfile(username, userAgent, ip, req, res);
        
        if (result && result.success && result.profile) {
            return res.json({ 
                success: true,
                username: result.profile.username,
                profilePicUrl: result.profile.profilePicUrl,
                followers: result.profile.followersCount,
                following: result.profile.followingCount,
                postsCount: result.profile.postsCount,
                isPrivate: result.profile.isPrivate,
                isVerified: result.profile.isVerified
            });
        } else {
            return res.status(404).json({ 
                success: false, 
                error: result.error || 'Perfil não encontrado.' 
            });
        }
    } catch (error) {
        console.error('Erro na rota /api/instagram/info:', error);
        res.status(500).json({ success: false, error: 'Erro interno ao buscar perfil' });
    }
});

const PROFILE_CACHE = new Map();
const PROFILE_INFLIGHT = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000;
const NEGATIVE_CACHE_TTL_MS = 2 * 60 * 1000;

function getCachedProfile(username) {
    const key = String(username || '').toLowerCase();
    const entry = PROFILE_CACHE.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
        PROFILE_CACHE.delete(key);
        return null;
    }
    return entry.value;
}

function setCache(username, value, ttlMs) {
    const key = String(username || '').toLowerCase();
    PROFILE_CACHE.set(key, { value, expiresAt: Date.now() + (ttlMs || CACHE_TTL_MS) });
}

async function verifyInstagramProfile(username, userAgent, ip, req, res, bypassCache = false) {
    const key = String(username || '').toLowerCase();

    if (!bypassCache) {
        const cached = getCachedProfile(username);
        if (cached) {
            console.log(`✅ Perfil @${username} retornado do cache`);
            return cached;
        }
        const inflight = PROFILE_INFLIGHT.get(key);
        if (inflight) return inflight;
    }

    const exec = (async () => {
        console.log(`🔍 Iniciando verificação do perfil (APIFY): @${username} (bypassCache: ${bypassCache})`);

        try {
            // ---------------------------------------------------------
            // TENTATIVA 1: ROCKETAPI (Rápido: ~2-5s)
            // ---------------------------------------------------------
            if (process.env.ROCKETAPI_TOKEN && (!global.rocketApiDisabledUntil || Date.now() > global.rocketApiDisabledUntil)) {
                try {
                    console.log(`🚀 Tentando RocketAPI para @${username}`);
                    const rocketUrl = 'https://v1.rocketapi.io/instagram/user/get_info';
                    const rocketResp = await axios.post(rocketUrl, { username }, { 
                        headers: { 'Authorization': `Token ${process.env.ROCKETAPI_TOKEN}` },
                        timeout: 15000 
                    });
                    
                    const rData = rocketResp.data;
                    const isRocketOk = rData && (rData.status === 'ok' || rData.status === 'done');
                    
                    if (isRocketOk && rData.response && rData.response.body && rData.response.body.data && rData.response.body.data.user) {
                        const rUser = rData.response.body.data.user;
                        console.log(`✅ RocketAPI retornou dados para @${username}`);
                        
                        global.rocketApiDisabledUntil = 0;

                        let rExtractedPosts = [];
                        const rEdges = (rUser.edge_owner_to_timeline_media && rUser.edge_owner_to_timeline_media.edges) ? rUser.edge_owner_to_timeline_media.edges : [];
                        
                        if (rEdges.length > 0) {
                            rExtractedPosts = rEdges.map(e => e.node ? ({
                                shortcode: e.node.shortcode,
                                takenAt: e.node.taken_at_timestamp,
                                isVideo: !!e.node.is_video,
                                displayUrl: e.node.display_url || e.node.thumbnail_src,
                                videoUrl: e.node.video_url,
                                typename: e.node.__typename
                            }) : null).filter(Boolean).slice(0, 12);
                        }

                        if (rExtractedPosts.length === 0 && !rUser.is_private && rUser.id) {
                             try {
                                console.log(`🚀 Buscando posts extras (RocketAPI) para ID: ${rUser.id}`);
                                const mediaUrl = 'https://v1.rocketapi.io/instagram/user/get_media';
                                const mediaResp = await axios.post(mediaUrl, { id: rUser.id, count: 12 }, { 
                                    headers: { 'Authorization': `Token ${process.env.ROCKETAPI_TOKEN}` },
                                    timeout: 10000 
                                });
                                const mItems = mediaResp.data?.response?.body?.items || [];
                                if (mItems.length > 0) {
                                    rExtractedPosts = mItems.map(item => ({
                                        shortcode: item.code,
                                        takenAt: item.taken_at,
                                        isVideo: item.media_type === 2 || !!item.video_versions,
                                        displayUrl: item.image_versions2?.candidates?.[0]?.url,
                                        videoUrl: item.video_versions?.[0]?.url,
                                        typename: item.media_type === 2 ? 'GraphVideo' : 'GraphImage'
                                    }));
                                    console.log(`✅ RocketAPI trouxe ${rExtractedPosts.length} posts via get_media.`);
                                }
                            } catch (eMedia) {
                                console.warn('⚠️ Falha ao buscar media RocketAPI:', eMedia.message);
                            }
                        }

                        const rProfileData = {
                            username: rUser.username,
                            fullName: rUser.full_name || "",
                            profilePicUrl: rUser.profile_pic_url_hd || rUser.profile_pic_url || "https://upload.wikimedia.org/wikipedia/commons/a/ac/Default_pfp.jpg",
                            originalProfilePicUrl: rUser.profile_pic_url_hd || rUser.profile_pic_url,
                            driveImageUrl: null,
                            followersCount: (rUser.edge_followed_by ? rUser.edge_followed_by.count : 0),
                            followingCount: (rUser.edge_follow ? rUser.edge_follow.count : 0),
                            postsCount: (rUser.edge_owner_to_timeline_media ? rUser.edge_owner_to_timeline_media.count : 0),
                            isPrivate: !!rUser.is_private,
                            isVerified: !!rUser.is_verified,
                            alreadyTested: false,
                            latestPosts: rExtractedPosts
                        };

                        try { rProfileData.alreadyTested = await checkInstauserExists(username); } catch (e) {}

                        try {
                            const vu = await getCollection('validated_insta_users');
                            const linkId = (req && req.session) ? req.session.linkSlug : (req && (req.query.id || req.body.id));
                            const doc = {
                                username: String(rProfileData.username).toLowerCase(),
                                fullName: rProfileData.fullName,
                                profilePicUrl: rProfileData.profilePicUrl,
                                isVerified: rProfileData.isVerified,
                                isPrivate: rProfileData.isPrivate,
                                followersCount: rProfileData.followersCount,
                                checkedAt: new Date().toISOString(),
                                linkId: linkId || null,
                                ip: String(ip || ''),
                                userAgent: String(userAgent || ''),
                                source: 'verifyInstagramProfile_ROCKETAPI',
                                latestPosts: rExtractedPosts
                            };
                            await vu.updateOne({ username: doc.username }, { $set: doc, $setOnInsert: { createdAt: new Date().toISOString() } }, { upsert: true });
                        } catch (dbErr) { console.error('Erro mongo ROCKET:', dbErr.message); }

                        if (typeof isAllowedImageHost === 'function' && rProfileData.profilePicUrl && isAllowedImageHost(rProfileData.profilePicUrl)) {
                             rProfileData.profilePicUrl = `/image-proxy?url=${encodeURIComponent(rProfileData.profilePicUrl)}`;
                        }

                        if (rProfileData.isPrivate) {
                             console.log(`⚠️ Perfil @${username} é privado (RocketAPI), mas será permitido.`);
                        }

                        const resultRocket = { success: true, status: 200, profile: rProfileData };
                        setCache(username, resultRocket, CACHE_TTL_MS);
                        return resultRocket;
                    } else {
                        const msg = JSON.stringify(rData).substring(0, 300);
                        console.warn(`⚠️ RocketAPI status inválido ou dados incompletos para @${username}:`, msg);
                        
                        if (msg.toLowerCase().includes('expired') || msg.toLowerCase().includes('renew') || msg.toLowerCase().includes('plan is')) {
                            console.warn('⛔ RocketAPI Plano Expirado (detectado por msg). Desabilitando temporariamente por 10 minutos.');
                            global.rocketApiDisabledUntil = Date.now() + (10 * 60 * 1000);
                        }
                    }
                } catch (eRocket) {
                    console.error('❌ Erro RocketAPI (fallback p/ Apify):', eRocket.message, eRocket.response?.data || '');
                    if (String(eRocket.response?.data || '').toLowerCase().includes('expired')) {
                         global.rocketApiDisabledUntil = Date.now() + (10 * 60 * 1000);
                    }
                }
            }

            const apifyToken = process.env.APIFY_TOKEN;
            if (!apifyToken) {
                console.error("❌ Erro: APIFY_TOKEN não configurado");
                throw new Error("APIFY_TOKEN_MISSING");
            }
            
            const apifyUrl = `https://api.apify.com/v2/acts/apify~instagram-profile-scraper/run-sync-get-dataset-items?token=${apifyToken}`;
            const payload = { 
                usernames: [username],
                resultsLimit: 1
            };

            console.log(`🚀 Enviando requisição para Apify: @${username}`);
            const response = await axios.post(apifyUrl, payload, { 
                headers: { 'Content-Type': 'application/json' },
                timeout: 120000
            });

            const items = response.data;
            
            if (!Array.isArray(items) || items.length === 0 || (items[0] && items[0].error)) {
                 console.warn(`⚠️ Apify não encontrou @${username} ou retornou erro.`);
                 const result404 = { success: false, status: 404, error: "Perfil não localizado." };
                 setCache(username, result404, NEGATIVE_CACHE_TTL_MS);
                 return result404;
            }

            const item = items[0];
            console.log(`✅ Apify retornou dados para @${username}`);

            const isPrivate = typeof item.private !== 'undefined' ? item.private : (typeof item.isPrivate !== 'undefined' ? item.isPrivate : false);
            const isVerified = typeof item.verified !== 'undefined' ? item.verified : (typeof item.isVerified !== 'undefined' ? item.isVerified : false);
            
            let extractedPosts = [];
            if (item.latestPosts && Array.isArray(item.latestPosts)) {
                console.log(`✅ Apify trouxe ${item.latestPosts.length} posts para @${username}`);
                extractedPosts = item.latestPosts.map(p => {
                    let ts = null;
                    if (p.timestamp) {
                        ts = new Date(p.timestamp).getTime() / 1000;
                    } else if (p.date) {
                        ts = new Date(p.date).getTime() / 1000;
                    } else if (p.takenAtTimestamp) {
                        ts = Number(p.takenAtTimestamp);
                    }
                    
                    return {
                        shortcode: p.shortCode || p.shortcode,
                        takenAt: ts,
                        isVideo: p.type === 'Video' || p.isVideo,
                        displayUrl: p.displayUrl || p.displayURL || p.imageUrl || p.thumbnailSrc,
                        videoUrl: p.videoUrl || p.videoURL,
                        typename: p.type === 'Video' ? 'GraphVideo' : 'GraphImage'
                    };
                }).slice(0, 12);
            } else {
                console.log(`⚠️ Apify NÃO trouxe posts para @${username}. Campos disponíveis: ${Object.keys(item).join(', ')}`);
            }

            const profileData = {
                username: item.username || username,
                fullName: item.fullName || "",
                profilePicUrl: item.profilePicUrlHD || item.profilePicUrl || "https://upload.wikimedia.org/wikipedia/commons/a/ac/Default_pfp.jpg",
                originalProfilePicUrl: item.profilePicUrlHD || item.profilePicUrl, 
                driveImageUrl: null,
                followersCount: item.followersCount || 0,
                followingCount: item.followsCount || 0,
                postsCount: item.postsCount || 0,
                isPrivate: isPrivate,
                isVerified: isVerified,
                alreadyTested: false,
                latestPosts: extractedPosts
            };

            try {
                profileData.alreadyTested = await checkInstauserExists(username);
            } catch (e) {}

            try {
                const vu = await getCollection('validated_insta_users');
                const linkId = (req && req.session) ? req.session.linkSlug : (req && (req.query.id || req.body.id));
                const doc = {
                    username: String(profileData.username).toLowerCase(),
                    fullName: profileData.fullName,
                    profilePicUrl: profileData.profilePicUrl,
                    isVerified: profileData.isVerified,
                    isPrivate: profileData.isPrivate,
                    followersCount: profileData.followersCount,
                    checkedAt: new Date().toISOString(),
                    linkId: linkId || null,
                    ip: String(ip || ''),
                    userAgent: String(userAgent || ''),
                    source: 'verifyInstagramProfile_APIFY',
                    latestPosts: extractedPosts
                };
                await vu.updateOne({ username: doc.username }, { $set: doc, $setOnInsert: { createdAt: new Date().toISOString() } }, { upsert: true });
                console.log('🗃️ MongoDB: validação Apify salva em validated_insta_users com posts');
            } catch (dbErr) {
                console.error('Erro mongo:', dbErr.message);
            }

            if (profileData.profilePicUrl && isAllowedImageHost(profileData.profilePicUrl)) {
                 profileData.profilePicUrl = `/image-proxy?url=${encodeURIComponent(profileData.profilePicUrl)}`;
            }

            if (isPrivate) {
                 console.log(`⚠️ Perfil @${username} é privado, mas será permitido.`);
            }

            const okResult = { success: true, status: 200, profile: profileData };
            setCache(username, okResult, CACHE_TTL_MS);
            return okResult;

        } catch (error) {
            console.error(`❌ Erro Apify: ${error.message}`);
            if (error.response) {
                console.error('❌ Detalhes Erro Apify:', error.response.status, error.response.data);
            }
            const errorResult = { success: false, status: 500, error: "Erro de usuário. configra o nome digitado e tente novamente." };
            setCache(username, errorResult, NEGATIVE_CACHE_TTL_MS);
            return errorResult;
        }
    })();

    if (!bypassCache) {
        PROFILE_INFLIGHT.set(key, exec);
    }

    try {
        return await exec;
    } finally {
        if (!bypassCache) {
            const current = PROFILE_INFLIGHT.get(key);
            if (current === exec) PROFILE_INFLIGHT.delete(key);
        }
    }
}

app.get('/teste-embed', (req, res) => {
  res.render('teste-embed');
});

app.get('/api/test-embed-data', async (req, res) => {
  try {
    const username = String(req.query.username || '').trim();
    if (!username) return res.json({ ok: false, error: 'Username missing' });

    // Use verifyInstagramProfile instead of scraping embed
    // This function handles cookies, proxies, and rotation automatically
    const userAgent = req.get('User-Agent') || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
    const ip = req.ip || '127.0.0.1';

    // Reuse the robust verification logic
    const result = await verifyInstagramProfile(username, userAgent, ip, req, res);

    if (result.success && result.profile) {
      // Adapt the output to match what might be expected by consumers of this endpoint
      // mimicking the structure found in scraping attempts where possible
      const adaptedUser = {
          username: result.profile.username,
          full_name: result.profile.fullName,
          profile_pic_url: result.profile.profilePicUrl,
          edge_followed_by: { count: result.profile.followersCount },
          edge_follow: { count: result.profile.followingCount || 0 },
          edge_owner_to_timeline_media: { count: result.profile.postsCount || 0 },
          is_private: result.profile.isPrivate,
          is_verified: result.profile.isVerified
      };

      return res.json({ 
          ok: true, 
          source: 'web_profile_info_via_proxy', 
          user: adaptedUser 
      });
    } else {
      return res.json({ 
          ok: false, 
          error: result.error || 'Erro desconhecido ao verificar perfil',
          details: result
      });
    }

  } catch (e) {
    console.error('[TEST-EMBED] Erro fatal:', e);
    return res.json({ ok: false, error: e.message || String(e) });
  }
});

const port = process.env.PORT ? Number(process.env.PORT) : 3000;

// (Removido) Handler ASAP de /checkout antes do view engine
// Motivo: estava tentando renderizar antes de configurar a engine,
// causando respostas vazias (Content-Length: 0). A rota oficial de
// checkout é registrada após a configuração da view engine.

// Inicializar gerenciadores
const linkManager = new LinkManager();
const driveManager = new GoogleDriveManager();
const baserowManager = new BaserowManager("https://baserow.atendimento.info", process.env.BASEROW_TOKEN);
const { connect: connectMongo, getCollection } = require('./mongodbClient');
try { connectMongo(); } catch(_) {}

// Configurar Baserow
const baserowToken = process.env.BASEROW_TOKEN || "manus";
if (baserowToken) {
    baserowManager.setToken(baserowToken);
    console.log("🗄️ Baserow configurado com sucesso");
} else {
    console.warn("⚠️ Token do Baserow não configurado");
}

// IDs das tabelas do Baserow
const BASEROW_TABLES = {
    CONTROLE: Number(process.env.BASEROW_CONTROLE_TABLE_ID || 631), // Tabela controle criada pelo usuário
    ACCESS_LOGS: process.env.BASEROW_ACCESS_LOG_TABLE_ID || null,
    PROFILES: process.env.BASEROW_PROFILES_TABLE_ID || null,
    WEBHOOKS: process.env.BASEROW_WEBHOOKS_TABLE_ID || null
};
// Mapeamento configurável dos nomes de campos na tabela CONTROLE
const CONTROLE_FIELDS = {
    USER_AGENT: process.env.BASEROW_FIELD_USER_AGENT || 'user-agent',
    IP: process.env.BASEROW_FIELD_IP || 'ip',
    INSTAUSER: process.env.BASEROW_FIELD_INSTAUSER || 'instauser',
    LINKPOST: process.env.BASEROW_FIELD_LINKPOST || 'linkpost',
    TESTE: process.env.BASEROW_FIELD_TESTE || 'teste',
    STATUSHTTP: process.env.BASEROW_FIELD_STATUSHTTP || 'statushttp',
    CRIADO: process.env.BASEROW_FIELD_CRIADO || 'criado',
    LINK: process.env.BASEROW_FIELD_LINK || 'link',
    TEL: process.env.BASEROW_FIELD_TEL || 'tel'
};

// IDs dos campos (opcional). Defaults conforme tabela informada.
const CONTROLE_FIELD_IDS = {
    USER_AGENT: process.env.BASEROW_FIELDID_USER_AGENT || 'field_6023', // fingerprint
    INSTAUSER: process.env.BASEROW_FIELDID_INSTAUSER || 'field_6025',
    LINKPOST: process.env.BASEROW_FIELDID_LINKPOST || null,
    TESTE: process.env.BASEROW_FIELDID_TESTE || 'field_6026',
    STATUSHTTP: process.env.BASEROW_FIELDID_STATUSHTTP || 'field_6027',
    CRIADO: process.env.BASEROW_FIELDID_CRIADO || 'field_6028',
    TEL: process.env.BASEROW_FIELDID_TEL || 'field_6029',
    LINK: process.env.BASEROW_FIELDID_LINK || 'field_6030',
    COMPROU: process.env.BASEROW_FIELDID_COMPROU || 'field_6038',
    IP: process.env.BASEROW_FIELDID_IP || null // se houver coluna IP, preencha no .env
};

function mapControleData(data) {
    const mapped = {};
    const uaVal = data['user-agent'] || data.userAgent;
    if (uaVal) {
        mapped[CONTROLE_FIELDS.USER_AGENT] = uaVal;
        mapped[CONTROLE_FIELD_IDS.USER_AGENT] = uaVal;
    }
    if (typeof data.ip !== 'undefined') {
        mapped[CONTROLE_FIELDS.IP] = data.ip;
        if (CONTROLE_FIELD_IDS.IP) mapped[CONTROLE_FIELD_IDS.IP] = data.ip;
    }
    if (typeof data.instauser !== 'undefined') {
        mapped[CONTROLE_FIELDS.INSTAUSER] = data.instauser;
        mapped[CONTROLE_FIELD_IDS.INSTAUSER] = data.instauser;
    }
    if (typeof data.linkpost !== 'undefined') {
        mapped[CONTROLE_FIELDS.LINKPOST] = data.linkpost;
        if (CONTROLE_FIELD_IDS.LINKPOST) mapped[CONTROLE_FIELD_IDS.LINKPOST] = data.linkpost;
    }
    if (typeof data.teste !== 'undefined') {
        mapped[CONTROLE_FIELDS.TESTE] = data.teste;
        mapped[CONTROLE_FIELD_IDS.TESTE] = data.teste;
    }
    if (typeof data.statushttp !== 'undefined') {
        mapped[CONTROLE_FIELDS.STATUSHTTP] = data.statushttp;
        mapped[CONTROLE_FIELD_IDS.STATUSHTTP] = data.statushttp;
    }
    if (typeof data.criado !== 'undefined') {
        mapped[CONTROLE_FIELDS.CRIADO] = data.criado;
        mapped[CONTROLE_FIELD_IDS.CRIADO] = data.criado;
    }
    if (typeof data.link !== 'undefined') {
        mapped[CONTROLE_FIELDS.LINK] = data.link;
        mapped[CONTROLE_FIELD_IDS.LINK] = data.link;
    }
    if (typeof data.tel !== 'undefined') {
        mapped[CONTROLE_FIELDS.TEL] = data.tel;
        mapped[CONTROLE_FIELD_IDS.TEL] = data.tel;
    }
    return mapped;
}

// ==================== FUNÇÕES DE CONTROLE DE ACESSO ====================

// Verificar se usuário já existe na tabela controle
async function checkUserInControle(userAgent, ip, instauser) {
    return null; // DISABLED: Baserow validation removed
    /*
    try {
        // Otimização: Filtrar por instauser no Baserow
        const fieldName = CONTROLE_FIELDS.INSTAUSER || 'instauser';
        const filters = {};
        if (instauser) {
            filters[`filter__${fieldName}__equal`] = instauser;
        } else {
            // Se não tiver instauser, tentar por IP
             const ipField = CONTROLE_FIELDS.IP || 'ip';
             filters[`filter__${ipField}__equal`] = ip;
        }
        filters['user_field_names'] = 'true';

        const result = await baserowManager.getTableRows(BASEROW_TABLES.CONTROLE, {
            filters,
            size: 20 // Um pouco maior pois pode haver vários checks do mesmo user/ip
        });
        
        if (!result.success) {
            console.error("Erro ao buscar linhas da tabela controle:", result.error);
            return null;
        }
        
        // Verificar se já existe registro com mesmo user-agent, ip e instauser
        const existingRecord = result.rows.find(row => 
            (row[CONTROLE_FIELDS.USER_AGENT] === userAgent) &&  // user-agent
            (row[CONTROLE_FIELDS.IP] === ip) &&                 // ip
            (row[CONTROLE_FIELDS.INSTAUSER] === instauser)      // instauser
        );
        
        return existingRecord;
    } catch (error) {
        console.error("Erro ao verificar usuário na tabela controle:", error);
        return null;
    }
    */
}

// Registrar usuário na tabela controle
async function registerUserInControle(userAgent, ip, instauser, statushttp) {
    return null; // DISABLED
    /*
    try {
        const fingerprint = generateFingerprint(ip, userAgent);
        
        const data = {
            "user-agent": fingerprint,
            "ip": ip,
            "instauser": instauser,
            "teste": "",
            "statushttp": statushttp,
            "criado": new Date().toISOString()
        };
        
        const result = await baserowManager.createRow(BASEROW_TABLES.CONTROLE, mapControleData(data));
        
        if (result.success) {
            console.log("✅ Usuário registrado na tabela controle:", result.row.id);
            return result.row;
        } else {
            console.error("❌ Erro ao registrar usuário na tabela controle:", result.error);
            return null;
        }
    } catch (error) {
        console.error("❌ Erro ao registrar usuário na tabela controle:", error);
        return null;
    }
    */
}

// Atualizar status do serviço na tabela controle
async function updateTesteStatus(recordId, testeStatus) {
    return null; // DISABLED
    /*
    try {
        // Primeiro fazer GET para verificar se a linha existe
        console.log(`📋 Buscando linha ${recordId} no Baserow...`);
        const getResult = await baserowManager.getRow(BASEROW_TABLES.CONTROLE, recordId);
        
        if (!getResult.success) {
            console.error("❌ Erro ao buscar linha:", getResult.error);
            return null;
        }
        
        console.log("📋 Linha encontrada:", getResult.row);
        
        // Preparar dados para atualização usando nome do campo
        const data = {
            "teste": testeStatus  // usar nome do campo teste
        };
        
        console.log(`📝 Atualizando linha ${recordId} com dados:`, data);
        const result = await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, recordId, mapControleData(data));
        
        if (result.success) {
            console.log("✅ Status do teste atualizado:", recordId, testeStatus);
            console.log("📋 Linha atualizada:", result.row);
            return result.row;
        } else {
            console.error("❌ Erro ao atualizar status do teste:", result.error);
            return null;
        }
    } catch (error) {
        console.error("Erro ao atualizar status do serviço:", error);
        return null;
    }
    */
}

// Verificar se instauser já foi usado
async function checkInstauserExists(instauser) {
    return false; // DISABLED: Always return false to skip validation
    /*
    try {
        console.log(`🔍 Verificando se instauser '${instauser}' já foi usado...`);
        // Otimização: Usar filtro do Baserow em vez de baixar tudo
        // Assumindo que o campo se chama 'instauser' ou o valor de CONTROLE_FIELDS.INSTAUSER
        const fieldName = CONTROLE_FIELDS.INSTAUSER || 'instauser';
        const filters = {};
        filters[`filter__${fieldName}__equal`] = instauser;
        filters['user_field_names'] = 'true'; // Garantir uso de nomes de campo

        const result = await baserowManager.getTableRows(BASEROW_TABLES.CONTROLE, {
            filters,
            size: 5 // Pegar alguns para garantir
        });

        if (!result.success) {
            console.error("❌ Erro ao buscar linhas (checkInstauserExists):", result.error);
            return false;
        }

        // Verificar se alguma linha retornada tem teste === 'OK'
        const existingUser = result.rows.find(row => {
            const testeValue = row[CONTROLE_FIELDS.TESTE];
            return testeValue === 'OK';
        });
        
        if (existingUser) {
            console.log(`❌ Instauser '${instauser}' já foi usado na linha ${existingUser.id} (teste=OK)`);
            return true;
        }
        console.log(`✅ Instauser '${instauser}' está disponível`);
        return false;
    } catch (error) {
        console.error("Erro ao verificar instauser:", error);
        return false; // Em caso de erro, permitir continuar
    }
    */
}

// Função para atualizar o campo 'teste' para 'OK' na linha correta do Baserow
async function updateBaserowTesteStatus(instauser) {
    return; // DISABLED
    /*
  try {
    // Buscar a linha pelo instauser usando filtro
    const fieldName = CONTROLE_FIELDS.INSTAUSER || 'instauser';
    const filters = {};
    filters[`filter__${fieldName}__equal`] = instauser;
    filters['user_field_names'] = 'true';

    const result = await baserowManager.getTableRows(BASEROW_TABLES.CONTROLE, {
        filters,
        order_by: '-id', // Tentar pegar o mais recente pelo ID (assumindo auto-increment ou cronológico)
        size: 5
    });

    if (!result.success) {
      console.error('Erro ao buscar linhas do Baserow:', result.error);
      return;
    }
    
    // Encontrar a linha mais recente pelo instauser (primeira do array pois ordenamos por -id)
    // Se order_by não funcionar como esperado, filtramos em memória
    const matchingRows = result.rows.filter(r => 
      (r.instauser && r.instauser.toLowerCase() === instauser.toLowerCase())
    );
    
    console.log(`🔍 Encontradas ${matchingRows.length} linhas para instauser: ${instauser}`);
    
    // Pegar a linha mais recente (primeira da lista filtrada se a API ordenou, ou sort manual)
    // Baserow retorna na ordem pedida.
    const row = matchingRows[0];
    
    if (row) {
      console.log(`📋 Linha encontrada: ID ${row.id}, instauser: ${row.instauser}, teste atual: ${row.teste}`);
    }
    
    if (!row) {
      console.warn('Linha do Baserow não encontrada para instauser:', instauser);
      return;
    }
    
    // Atualizar o campo 'teste' para 'OK' usando o nome do campo
    const updateData = { teste: 'OK' };
    const updateResult = await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, updateData);
    
    if (updateResult.success) {
      console.log(`✅ Campo 'teste' atualizado para OK na linha ${row.id} do instauser: ${instauser}`);
    } else {
      console.error('Erro ao atualizar campo teste:', updateResult.error);
    }
  } catch (err) {
    console.error('Erro ao atualizar campo teste no Baserow:', err.message || err);
  }
  */
}

// Lista de fingerprints bloqueados manualmente
const blockedFingerprints = ['e7DMDkz0nWbVn4O3OPoE'];
// Remover whitelist de fingerprints
// const allowedFingerprints = ['e7DMDkz0nWbVn4O3OPoE'];

// ==================== ROTAS ====================

// Rota de diagnóstico simples
app.get('/ping', (req, res) => {
  console.log('🏓 Ping recebido');
  res.type('text/plain').send('pong');
});

// SSE para atualização instantânea de pagamento
const paymentSubscribers = [];
function addPaymentSubscriber(identifier, correlationID, res) {
  paymentSubscribers.push({ identifier, correlationID, res });
}
function removePaymentSubscriber(res) {
  const idx = paymentSubscribers.findIndex((c) => c.res === res);
  if (idx >= 0) paymentSubscribers.splice(idx, 1);
}
async function broadcastPaymentPaid(identifier, correlationID) {
  const ident = String(identifier || '').trim();
  const corr = String(correlationID || '').trim();
  let orderIdFS = null;
  let orderIdFama = null;
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const conds = [];
    if (ident) { conds.push({ 'woovi.identifier': ident }); conds.push({ identifier: ident }); }
    if (corr) { conds.push({ correlationID: corr }); }
    const filter = conds.length ? { $or: conds } : {};
    const doc = await col.findOne(filter);
    orderIdFS = doc && doc.fornecedor_social && doc.fornecedor_social.orderId ? doc.fornecedor_social.orderId : null;
    orderIdFama = doc && doc.fama24h && doc.fama24h.orderId ? doc.fama24h.orderId : null;
  } catch(_) {}
  paymentSubscribers.forEach(({ identifier: id, correlationID: cid, res }) => {
    if ((ident && id === ident) || (corr && cid === corr)) {
      try {
        res.write(`event: paid\n`);
        res.write(`data: ${JSON.stringify({ identifier: ident, correlationID: corr, fornecedor_social_orderId: orderIdFS, fama24h_orderId: orderIdFama })}\n\n`);
      } catch(_) {}
    }
  });
}

async function ensureRefilLink(identifier, correlationID, req) {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const conds = [];
    const ident = String(identifier || '').trim();
    const corr = String(correlationID || '').trim();
    if (ident) { conds.push({ 'woovi.identifier': ident }); conds.push({ identifier: ident }); }
    if (corr) { conds.push({ correlationID: corr }); }
    const filter = conds.length ? { $or: conds } : {};
    let doc = null;
    try {
      const arr = await col.find(filter, { projection: { _id: 1, instauser: 1, instagramUsername: 1, additionalInfoPaid: 1, additionalInfo: 1, additionalInfoMapPaid: 1, additionalInfoMap: 1, customer: 1, comment: 1, paidAt: 1, createdAt: 1, woovi: 1 } })
        .sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 })
        .limit(1)
        .toArray();
      doc = (Array.isArray(arr) && arr.length) ? arr[0] : null;
    } catch (_) {
      doc = await col.findOne(filter, { projection: { _id: 1, instauser: 1, instagramUsername: 1, additionalInfoPaid: 1, additionalInfo: 1, additionalInfoMapPaid: 1, additionalInfoMap: 1, customer: 1, comment: 1, paidAt: 1, createdAt: 1, woovi: 1 } });
    }
    if (!doc) return null;
    const arrPaid = Array.isArray(doc?.additionalInfoPaid) ? doc.additionalInfoPaid : [];
    const arrOrig = Array.isArray(doc?.additionalInfo) ? doc.additionalInfo : [];
    const mapBase = Object.assign(
      {},
      (doc && doc.additionalInfoMapPaid && typeof doc.additionalInfoMapPaid === 'object') ? doc.additionalInfoMapPaid : {},
      (doc && doc.additionalInfoMap && typeof doc.additionalInfoMap === 'object') ? doc.additionalInfoMap : {}
    );
    const mapFromArr = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
    const map = Object.assign({}, mapBase, mapFromArr);
    const pickVal = (arr, keyLower) => {
      try {
        const a = Array.isArray(arr) ? arr : [];
        const it = a.find(x => String(x?.key || '').trim().toLowerCase() === keyLower);
        const v = it && typeof it.value !== 'undefined' ? it.value : '';
        const s = String(v || '').trim();
        return s;
      } catch (_) {
        return '';
      }
    };
    const normalizeInstaUser = (v) => {
      try {
        let s = String(v || '').trim();
        if (!s) return '';
        if (/^https?:\/\//i.test(s)) {
          s = s.replace(/^https?:\/\/(www\.)?instagram\.com\//i, '');
          s = s.split('?')[0].split('#')[0];
          s = s.replace(/\/+$/, '');
          const parts = s.split('/').filter(Boolean);
          s = parts.length ? String(parts[parts.length - 1] || '') : s;
        }
        s = s.trim();
        if (s.startsWith('@')) s = s.slice(1);
        return s.toLowerCase().trim();
      } catch (_) {
        return '';
      }
    };
    const iuRaw = doc.instauser || doc.instagramUsername || map['instagram_username'] || '';
    const iu = normalizeInstaUser(iuRaw);
    const phoneFromCustomer = (doc && doc.customer && doc.customer.phone) ? String(doc.customer.phone).replace(/\D/g, '') : '';
    const phoneFromMap = (map['phone'] ? String(map['phone']) : (pickVal(arrPaid, 'phone') || pickVal(arrOrig, 'phone'))).replace(/\D/g, '');
    const phoneDigits = phoneFromCustomer || phoneFromMap || '';
    const tipoServicoRaw = String(map['tipo_servico'] || doc.tipoServico || doc.tipo || '').trim().toLowerCase();
    const categoriaServicoRaw = String(map['categoria_servico'] || doc.categoriaServico || '').trim().toLowerCase();
    const isSeguidores = !categoriaServicoRaw || categoriaServicoRaw.includes('seguidores');
    const isEligibleRefil = isSeguidores && (tipoServicoRaw.includes('mistos') || tipoServicoRaw.includes('brasileir'));
    if (!isEligibleRefil) return null;
    const bumpsStr = String(map['order_bumps'] || pickVal(arrPaid, 'order_bumps') || pickVal(arrOrig, 'order_bumps') || '').trim();
    const bumpQtyMap = (() => {
      const out = {};
      const parts = bumpsStr ? bumpsStr.split(';') : [];
      for (const raw of parts) {
        const part = String(raw || '').trim();
        if (!part) continue;
        const segs = part.split(':');
        const key = String(segs[0] || '').trim().toLowerCase();
        if (!key) continue;
        const qtyRaw = segs.length > 1 ? String(segs[1] || '').trim() : '';
        const qtyParsed = qtyRaw ? Number(qtyRaw) : 1;
        const qty = Number.isFinite(qtyParsed) ? qtyParsed : 1;
        out[key] = Number(out[key] || 0) + qty;
      }
      return out;
    })();
    const hasLifetime = (() => {
      const get = (k) => Number(bumpQtyMap[k] || 0) || 0;
      if (get('warranty_lifetime') > 0 || get('warranty_life') > 0) return true;
      if (get('warranty60') > 0 || get('warranty_60') > 0 || get('warrenty60') > 0 || get('warrenty_60') > 0) return true;
      if (get('warrenty') > 0) return true;
      return false;
    })();
    const warrantyMode = hasLifetime ? 'life' : '30';
    const warrantyDays = hasLifetime ? null : 30;
    const orderBaseMs = (() => {
      const toMs = (v) => {
        try {
          if (!v) return 0;
          if (typeof v === 'number' && Number.isFinite(v)) return v;
          const t = new Date(v).getTime();
          return Number.isFinite(t) ? t : 0;
        } catch (_) {
          return 0;
        }
      };
      const a = toMs(doc?.woovi?.paidAt);
      if (a) return a;
      const b = toMs(doc?.paidAt);
      if (b) return b;
      const c = toMs(doc?.createdAt);
      if (c) return c;
      return Date.now();
    })();
    const brtOffsetMs = 3 * 60 * 60 * 1000;
    const brtYmdFromMs = (ms) => {
      const d = new Date(Number(ms || 0) - brtOffsetMs);
      return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
    };
    const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
    const addMonthsEndOfDayBrtIso = (baseMs, monthsToAdd) => {
      const base = brtYmdFromMs(baseMs);
      let y = base.y;
      let m = base.m + Number(monthsToAdd || 0);
      while (m > 12) { y += 1; m -= 12; }
      while (m < 1) { y -= 1; m += 12; }
      const maxDay = daysInMonth(y, m);
      const d = Math.min(base.d, maxDay);
      const utcMs = Date.UTC(y, m - 1, d, 23, 59, 59, 999) + brtOffsetMs;
      return new Date(utcMs).toISOString();
    };
    const monthsToAdd = warrantyMode === 'life' ? null : 1;
    const desiredExpiresAtIso = warrantyMode === 'life'
      ? new Date('2099-12-31T23:59:59.999Z').toISOString()
      : addMonthsEndOfDayBrtIso(orderBaseMs, monthsToAdd);
    const desiredCreatedAtIso = new Date(orderBaseMs).toISOString();
    const tl = await getCollection('temporary_links');

    if (iu) {
      const existingByInsta = await tl.findOne({ purpose: 'refil', $or: [{ instauser: iu }, { instausers: iu }] });
      if (existingByInsta) {
        await col.updateOne({ _id: doc._id }, { $set: { refilLinkId: existingByInsta.id } });
        const sets = {
          phone: phoneDigits || existingByInsta.phone || null,
          instauser: iu || existingByInsta.instauser || null,
          warrantyMode: warrantyMode || existingByInsta.warrantyMode || null,
          warrantyDays: (typeof warrantyDays === 'number' ? warrantyDays : (typeof existingByInsta.warrantyDays === 'number' ? existingByInsta.warrantyDays : null))
        };
        if (desiredCreatedAtIso) {
          const currentCreatedMs = existingByInsta.createdAt ? new Date(existingByInsta.createdAt).getTime() : 0;
          const desiredCreatedMs = new Date(desiredCreatedAtIso).getTime();
          if (!currentCreatedMs || (desiredCreatedMs && desiredCreatedMs > currentCreatedMs)) {
            sets.createdAt = desiredCreatedAtIso;
            sets.orderId = String(doc._id);
          }
        }
        if (desiredExpiresAtIso) {
          const currentExpMs = existingByInsta.expiresAt ? new Date(existingByInsta.expiresAt).getTime() : 0;
          const desiredExpMs = new Date(desiredExpiresAtIso).getTime();
          if (!currentExpMs || (desiredExpMs && desiredExpMs > currentExpMs)) {
            sets.expiresAt = desiredExpiresAtIso;
          }
        }
        const addToSet = { orders: String(doc._id), instausers: String(iu) };
        const updateFilter = existingByInsta._id ? { _id: existingByInsta._id } : { id: existingByInsta.id };
        await tl.updateOne(updateFilter, { $set: sets, $addToSet: addToSet });
        if (sets.expiresAt) existingByInsta.expiresAt = sets.expiresAt;
        if (sets.warrantyMode) existingByInsta.warrantyMode = sets.warrantyMode;
        return existingByInsta;
      }
    }

    if (phoneDigits) {
      const existingByPhone = await tl.findOne({ purpose: 'refil', phone: phoneDigits });
      if (existingByPhone) {
        const existingInsta = normalizeInstaUser(existingByPhone.instauser || '');
        const existingList = Array.isArray(existingByPhone.instausers) ? existingByPhone.instausers.map(normalizeInstaUser).filter(Boolean) : [];
        const canReuse = !iu || !existingInsta || existingInsta === iu || existingList.includes(iu);
        if (canReuse) {
          await col.updateOne({ _id: doc._id }, { $set: { refilLinkId: existingByPhone.id } });
          const sets = {
            orderId: existingByPhone.orderId ? String(existingByPhone.orderId) : String(doc._id),
            phone: phoneDigits || existingByPhone.phone || null,
            instauser: iu || existingByPhone.instauser || null,
            warrantyMode: warrantyMode || existingByPhone.warrantyMode || null,
            warrantyDays: (typeof warrantyDays === 'number' ? warrantyDays : (typeof existingByPhone.warrantyDays === 'number' ? existingByPhone.warrantyDays : null))
          };
          if (desiredCreatedAtIso) {
            const currentCreatedMs = existingByPhone.createdAt ? new Date(existingByPhone.createdAt).getTime() : 0;
            const desiredCreatedMs = new Date(desiredCreatedAtIso).getTime();
            if (!currentCreatedMs || (desiredCreatedMs && desiredCreatedMs > currentCreatedMs)) {
              sets.createdAt = desiredCreatedAtIso;
              sets.orderId = String(doc._id);
            }
          }
          if (desiredExpiresAtIso) {
            const currentExpMs = existingByPhone.expiresAt ? new Date(existingByPhone.expiresAt).getTime() : 0;
            const desiredExpMs = new Date(desiredExpiresAtIso).getTime();
            if (!currentExpMs || (desiredExpMs && desiredExpMs > currentExpMs)) {
              sets.expiresAt = desiredExpiresAtIso;
            }
          }
          const addToSet = { orders: String(doc._id) };
          if (iu) addToSet.instausers = String(iu);
          const updateFilter = existingByPhone._id ? { _id: existingByPhone._id } : { id: existingByPhone.id };
          await tl.updateOne(updateFilter, { $set: sets, $addToSet: addToSet });
          if (sets.expiresAt) existingByPhone.expiresAt = sets.expiresAt;
          if (sets.warrantyMode) existingByPhone.warrantyMode = sets.warrantyMode;
          return existingByPhone;
        }
      }
    }

    // Compatibilidade: verificar se já existe por orderId
    const existing = await tl.findOne({ orderId: String(doc._id), purpose: 'refil' });
    if (existing) {
      await col.updateOne({ _id: doc._id }, { $set: { refilLinkId: existing.id } });
      if (!existing.instauser && iu) {
        await tl.updateOne({ id: existing.id }, { $set: { instauser: iu } });
        existing.instauser = iu;
      }
      // Se houver telefone, vincular para futura reutilização
      if (phoneDigits) {
        await tl.updateOne({ id: existing.id }, { $set: { phone: phoneDigits }, $addToSet: { orders: String(doc._id) } });
      }
      try {
        const sets = {};
        if (warrantyMode) sets.warrantyMode = warrantyMode;
        if (typeof warrantyDays === 'number') sets.warrantyDays = warrantyDays;
        if (desiredCreatedAtIso) {
          const currentCreatedMs = existing.createdAt ? new Date(existing.createdAt).getTime() : 0;
          const desiredCreatedMs = new Date(desiredCreatedAtIso).getTime();
          if (!currentCreatedMs || (desiredCreatedMs && desiredCreatedMs > currentCreatedMs)) {
            sets.createdAt = desiredCreatedAtIso;
            sets.orderId = String(doc._id);
          }
        }
        if (desiredExpiresAtIso) {
          const currentExpMs = existing.expiresAt ? new Date(existing.expiresAt).getTime() : 0;
          const desiredExpMs = new Date(desiredExpiresAtIso).getTime();
          if (!currentExpMs || (desiredExpMs && desiredExpMs > currentExpMs)) {
            sets.expiresAt = desiredExpiresAtIso;
          }
        }
        if (Object.keys(sets).length) {
          await tl.updateOne({ id: existing.id }, { $set: sets, ...(iu ? { $addToSet: { instausers: String(iu) } } : {}) });
          if (sets.expiresAt) existing.expiresAt = sets.expiresAt;
          if (sets.warrantyMode) existing.warrantyMode = sets.warrantyMode;
        }
      } catch (_) {}
      return existing;
    }

    // Criar novo link e vincular ao telefone (se disponível)
    const info = linkManager.generateLink(req);
    const rec = {
      id: info.id,
      purpose: 'refil',
      orderId: String(doc._id),
      phone: phoneDigits || null,
      orders: [String(doc._id)],
      instauser: iu || null,
      instausers: iu ? [String(iu)] : [],
      warrantyMode: warrantyMode,
      warrantyDays: warrantyDays,
      createdAt: new Date(orderBaseMs).toISOString(),
      expiresAt: desiredExpiresAtIso || new Date(info.expiresAt).toISOString()
    };
    await tl.insertOne(rec);
    await col.updateOne({ _id: doc._id }, { $set: { refilLinkId: info.id } });
    try { console.log('🔗 Link de refil criado:', info.id, '| phone:', phoneDigits || '(none)'); } catch(_) {}
    return rec;
  } catch (e) {
    try { console.warn('⚠️ Falha ao criar link de refil:', e?.message || String(e)); } catch(_) {}
    return null;
  }
}

async function geoLookupIp(ipRaw) {
  try {
    const ip = String(ipRaw || '').split(',')[0].replace('::ffff:', '').trim();
    if (!ip || ip === 'unknown') return null;
    // Use ip-api.com (free, 45 req/min) - more reliable than ipapi.co free tier
    const url = `http://ip-api.com/json/${encodeURIComponent(ip)}`;
    const resp = await fetch(url);
    const data = await resp.json();
    
    if (data.status === 'fail') return null;

    const city = String(data.city || '').trim();
    const region = String(data.regionName || data.region || '').trim();
    const country = String(data.country || '').trim();
    return { city, region, country, source: 'ip-api.com' };
  } catch (_) { return null; }
}

function toSha256(str) {
  try {
    const crypto = require('crypto');
    return crypto.createHash('sha256').update(String(str || ''), 'utf8').digest('hex');
  } catch (_) { return ''; }
}

function buildFbcFromFbclid(fbclid) {
  const click = String(fbclid || '').trim();
  if (!click) return undefined;
  const ts = Math.floor(Date.now() / 1000);
  return `fb.1.${ts}.${click}`;
}

async function trackMetaPurchaseForOrder(identifier, correlationID, req) {
  /* tracking Purchase via Meta CAPI desativado */
  return;
}

app.get('/api/payment/subscribe', (req, res) => {
  const identifier = String(req.query.identifier || '').trim();
  const correlationID = String(req.query.correlationID || '').trim();
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();
  res.write(`: connected\n\n`);
  addPaymentSubscriber(identifier, correlationID, res);
  req.on('close', () => { removePaymentSubscriber(res); });
});

// Diagnóstico: ambiente de execução
app.get('/__debug/env', (req, res) => {
  try {
    const fs = require('fs');
    const info = {
      cwd: process.cwd(),
      dirname: __dirname,
      filename: __filename,
      appMtime: (() => {
        try {
          const st = fs.statSync(__filename);
          return st.mtimeMs;
        } catch (_) { return null; }
      })(),
      node: process.version,
    };
    res.type('application/json').send(JSON.stringify(info, null, 2));
  } catch (e) {
    res.status(500).type('text/plain').send('env_error: ' + (e?.message || 'unknown'));
  }
});

// Diagnóstico: testar a chave da Fama24h (sem expor o valor)
app.get('/__debug/fama24h-balance', async (req, res) => {
  try {
    const apiKey = (process.env.FAMA24H_API_KEY || '').trim();
    if (!apiKey) {
      return res.status(500).json({
        success: false,
        error: 'missing_api_key',
        message: 'FAMA24H_API_KEY não está definida no servidor.'
      });
    }

    const params = new URLSearchParams();
    params.append('key', apiKey);
    params.append('action', 'balance');

    try {
      const axios = require('axios');
      const response = await axios.post('https://fama24h.net/api/v2', params, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' }
      });

      // Normalizar resposta: não expor dados sensíveis
      if (response?.data?.balance !== undefined) {
        return res.json({
          success: true,
          provider: 'fama24h',
          hasKey: true,
          maskedKey: apiKey.slice(0, 6) + '***',
          balance: response.data.balance
        });
      }
      return res.status(400).json({
        success: false,
        provider: 'fama24h',
        hasKey: true,
        maskedKey: apiKey.slice(0, 6) + '***',
        error: response?.data?.error || 'api_error',
        response: response?.data || null
      });
    } catch (err) {
      return res.status(500).json({
        success: false,
        provider: 'fama24h',
        hasKey: true,
        maskedKey: apiKey.slice(0, 6) + '***',
        error: err?.message || 'request_error'
      });
    }
  } catch (e) {
    res.status(500).json({ success: false, error: e?.message || 'unknown' });
  }
});

// Admin: Normalizar expiração dos temporary_links para N dias a partir da criação
app.get('/__admin/temporary-links/normalize-expiration', async (req, res) => {
  try {
    const days = Math.max(1, Number(req.query.days || 7) || 7);
    const ms = days * 24 * 60 * 60 * 1000;
    const { getCollection } = require('./mongodbClient');
    const tl = await getCollection('temporary_links');
    const cursor = tl.find({}, { projection: { id: 1, createdAt: 1, expiresAt: 1 } });
    const docs = await cursor.toArray();
    const ops = [];
    let updated = 0;
    for (const d of docs) {
      try {
        const createdRaw = d?.createdAt;
        let createdMs = null;
        if (createdRaw instanceof Date) createdMs = createdRaw.getTime();
        else if (typeof createdRaw === 'string') createdMs = new Date(createdRaw).getTime();
        else if (typeof createdRaw === 'number') createdMs = createdRaw;
        if (!createdMs || Number.isNaN(createdMs)) continue;
        const targetISO = new Date(createdMs + ms).toISOString();
        ops.push({ updateOne: { filter: { id: d.id }, update: { $set: { expiresAt: targetISO } } } });
        updated++;
      } catch(_) {}
    }
    if (ops.length) {
      await tl.bulkWrite(ops, { ordered: false });
    }
    return res.json({ ok: true, total: docs.length, updated, days });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post('/api/admin/temporary-links/normalize-expiration', async (req, res) => {
  try {
    const days = Math.max(1, Number((req.body && req.body.days) || req.query.days || 7) || 7);
    const ms = days * 24 * 60 * 60 * 1000;
    const { getCollection } = require('./mongodbClient');
    const tl = await getCollection('temporary_links');
    const cursor = tl.find({}, { projection: { id: 1, createdAt: 1, expiresAt: 1 } });
    const docs = await cursor.toArray();
    const ops = [];
    let updated = 0;
    for (const d of docs) {
      try {
        const createdRaw = d?.createdAt;
        let createdMs = null;
        if (createdRaw instanceof Date) createdMs = createdRaw.getTime();
        else if (typeof createdRaw === 'string') createdMs = new Date(createdRaw).getTime();
        else if (typeof createdRaw === 'number') createdMs = createdRaw;
        if (!createdMs || Number.isNaN(createdMs)) continue;
        const targetISO = new Date(createdMs + ms).toISOString();
        ops.push({ updateOne: { filter: { id: d.id }, update: { $set: { expiresAt: targetISO } } } });
        updated++;
      } catch(_) {}
    }
    if (ops.length) {
      await tl.bulkWrite(ops, { ordered: false });
    }
    return res.json({ ok: true, total: docs.length, updated, days });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Admin: Unificar temporary_links por telefone (um ID por número)
app.get('/__admin/temporary-links/unify-by-phone', async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const tl = await getCollection('temporary_links');
    const ordersCol = await getCollection('checkout_orders');
    const refils = await tl.find({ purpose: 'refil' }).toArray();
    const groups = refils.reduce((acc, d) => {
      const phone = String(d.phone || '').replace(/\D/g, '');
      if (!phone) return acc;
      (acc[phone] = acc[phone] || []).push(d);
      return acc;
    }, {});
    let phones = 0, dups = 0, ordersUpdated = 0, linksDeleted = 0, linksUpdated = 0;
    for (const [phone, arr] of Object.entries(groups)) {
      if (arr.length <= 1) continue;
      phones++;
      arr.sort((a,b)=> new Date(a.createdAt||0).getTime() - new Date(b.createdAt||0).getTime());
      const canonical = arr[0];
      const canonicalId = canonical.id;
      const setOrders = new Set();
      arr.forEach(x => {
        if (x.orderId) setOrders.add(String(x.orderId));
        (Array.isArray(x.orders) ? x.orders : []).forEach(o => setOrders.add(String(o)));
      });
      const allOrderIds = Array.from(setOrders);
      if (allOrderIds.length) {
        await tl.updateOne({ id: canonicalId }, { $set: { orders: allOrderIds, phone: phone } });
        linksUpdated++;
      }
      for (let i = 1; i < arr.length; i++) {
        const dup = arr[i];
        dups++;
        const updRes = await ordersCol.updateMany({ refilLinkId: dup.id }, { $set: { refilLinkId: canonicalId } });
        ordersUpdated += updRes.modifiedCount || 0;
        if (dup.orderId) {
          await tl.updateOne({ id: canonicalId }, { $addToSet: { orders: String(dup.orderId) } });
        }
        const delRes = await tl.deleteOne({ id: dup.id });
        linksDeleted += delRes.deletedCount || 0;
      }
    }
    return res.json({ ok: true, phonesProcessed: phones, duplicatesResolved: dups, ordersRepointed: ordersUpdated, linksDeleted, linksUpdated });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/admin/temporary-links/unify-by-phone', async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const tl = await getCollection('temporary_links');
    const ordersCol = await getCollection('checkout_orders');
    const refils = await tl.find({ purpose: 'refil' }).toArray();
    const groups = refils.reduce((acc, d) => {
      const phone = String(d.phone || '').replace(/\D/g, '');
      if (!phone) return acc;
      (acc[phone] = acc[phone] || []).push(d);
      return acc;
    }, {});
    let phones = 0, dups = 0, ordersUpdated = 0, linksDeleted = 0, linksUpdated = 0;
    for (const [phone, arr] of Object.entries(groups)) {
      if (arr.length <= 1) continue;
      phones++;
      arr.sort((a,b)=> new Date(a.createdAt||0).getTime() - new Date(b.createdAt||0).getTime());
      const canonical = arr[0];
      const canonicalId = canonical.id;
      const setOrders = new Set();
      arr.forEach(x => {
        if (x.orderId) setOrders.add(String(x.orderId));
        (Array.isArray(x.orders) ? x.orders : []).forEach(o => setOrders.add(String(o)));
      });
      const allOrderIds = Array.from(setOrders);
      if (allOrderIds.length) {
        await tl.updateOne({ id: canonicalId }, { $set: { orders: allOrderIds, phone: phone } });
        linksUpdated++;
      }
      for (let i = 1; i < arr.length; i++) {
        const dup = arr[i];
        dups++;
        const updRes = await ordersCol.updateMany({ refilLinkId: dup.id }, { $set: { refilLinkId: canonicalId } });
        ordersUpdated += updRes.modifiedCount || 0;
        if (dup.orderId) {
          await tl.updateOne({ id: canonicalId }, { $addToSet: { orders: String(dup.orderId) } });
        }
        const delRes = await tl.deleteOne({ id: dup.id });
        linksDeleted += delRes.deletedCount || 0;
      }
    }
    return res.json({ ok: true, phonesProcessed: phones, duplicatesResolved: dups, ordersRepointed: ordersUpdated, linksDeleted, linksUpdated });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Diagnóstico: logar tamanho do corpo enviado para /checkout
app.use((req, res, next) => {
  if (req.path.startsWith('/checkout')) {
    const originalSend = res.send.bind(res);
    res.send = (body) => {
      try {
        const len = typeof body === 'string' ? body.length : (Buffer.isBuffer(body) ? body.length : 0);
        console.log('📦 Enviando body para', req.originalUrl, 'len=', len);
      } catch (_) {}
      return originalSend(body);
    };
  }
  next();
});

// Rota de checkout será tratada mais abaixo por app.get('/checkout')
// Diagnóstico: enviar conteúdo bruto do template de checkout
app.get('/__debug/checkout-raw', (req, res) => {
  try {
    const fs = require('fs');
    const p = path.join(__dirname, 'views', 'checkout.ejs');
    const stat = fs.statSync(p);
    const txt = fs.readFileSync(p, 'utf8');
    res.type('text/plain').send(`path=${p}\nsize=${stat.size}\n---\n${txt}`);
  } catch (e) {
    res.status(500).type('text/plain').send('read_error: ' + (e?.message || 'unknown'));
  }
});

// Diagnóstico: enviar conteúdo bruto do template index
app.get('/__debug/index-raw', (req, res) => {
  try {
    const fs = require('fs');
    const p = path.join(__dirname, 'views', 'index.ejs');
    const stat = fs.statSync(p);
    const txt = fs.readFileSync(p, 'utf8');
    res.type('text/plain').send(`path=${p}\nsize=${stat.size}\n---\n${txt}`);
  } catch (e) {
    res.status(500).type('text/plain').send('read_error: ' + (e?.message || 'unknown'));
  }
});

// Diagnóstico: listar arquivos e tamanhos em views/
app.get('/__debug/views-list', (req, res) => {
  try {
    const fs = require('fs');
    const dir = path.join(__dirname, 'views');
    const files = fs.readdirSync(dir);
    const lines = files.map((f) => {
      const fp = path.join(dir, f);
      try {
        const s = fs.statSync(fp);
        return `${f}\t${s.size}`;
      } catch (e) {
        return `${f}\tstat_error:${e?.message || 'unknown'}`;
      }
    }).join('\n');
    res.type('text/plain').send(`dir=${dir}\n---\n${lines}`);
  } catch (e) {
    res.status(500).type('text/plain').send('read_error: ' + (e?.message || 'unknown'));
  }
});

// Rotas diretas antes de estáticos (mantidas apenas para depuração, se necessário)

// Servir arquivos estáticos
app.use(express.static("public"));
app.use('/temp-images', express.static(path.join(__dirname, 'temp_images')));

app.get('/@vite/client', (req, res) => {
  res.type('application/javascript').send('');
});

app.get('/@react-refresh', (req, res) => {
  res.type('application/javascript').send('');
});

// Helper: validar hosts permitidos para proxy de imagem
function isAllowedImageHost(urlStr) {
  try {
    const u = new URL(urlStr);
    const host = u.hostname.toLowerCase();
    return (
      host.includes('instagram') ||
      host.includes('cdninstagram') ||
      host.includes('fbcdn') ||
      host.includes('scontent')
    );
  } catch (e) {
    return false;
  }
}

// Endpoint de proxy de imagem para evitar bloqueios de CSP/Proxy
app.get('/image-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl || !isAllowedImageHost(targetUrl)) {
    return res.status(400).send('Invalid image URL');
  }
  try {
    const response = await axios.get(targetUrl, {
      responseType: 'arraybuffer',
      timeout: 25000,
      headers: {
        'User-Agent': req.get('User-Agent') || 'Mozilla/5.0',
        'Accept': 'image/*,video/*,*/*;q=0.8'
      }
    });
    const contentType = response.headers['content-type'] || 'image/jpeg';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Cache-Control', 'public, max-age=300'); // 5 minutos
    return res.send(response.data);
  } catch (err) {
    console.error('Erro proxy de imagem:', err.message);
    return res.status(502).send('Failed to fetch image');
  }
});

// (mantido acima)

// Middleware para controlar acesso à página de perfil
function perfilAccessGuard(req, res, next) {
    if (req.session && req.session.perfilAccessAllowed) {
        return next();
    }
    const from = req.originalUrl || '/perfil';
    return res.redirect(`/restrito?from=${encodeURIComponent(from)}`);
}

// Log global de requisições para diagnosticar roteamento
app.use((req, res, next) => {
    try {
        console.log('➡️', req.method, req.originalUrl);
    } catch (_) {}
    next();
});
// Rota crítica para registrar validações (deve estar bem no topo)
app.post('/api/instagram/track-validated', async (req, res) => {
  try {
    const username = String((req.body && req.body.username) || '').trim().toLowerCase();
    if (!username) return res.status(400).json({ ok: false, error: 'missing_username' });
  const vu = await getCollection('validated_insta_users');
    const doc = {
      username,
      checkedAt: new Date().toISOString(),
      ip: req.headers['x-forwarded-for'] || req.ip || null,
      userAgent: req.get('User-Agent') || '',
      source: 'api.track.top'
    };
    await vu.updateOne({ username }, { $setOnInsert: { username, firstSeenAt: new Date().toISOString() }, $set: doc }, { upsert: true });
    try { console.log('🗃️ Track TOP: upsert ok', { username }); } catch(_) {}
    return res.json({ ok: true });
  } catch (e) {
    try { console.error('Track TOP error', e?.message || String(e)); } catch(_) {}
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Rota para selecionar um post para um tipo de serviço (likes, views, comments)
app.post('/api/instagram/select-post-for', (req, res) => {
    try {
        const { username, shortcode, kind } = req.body;
        if (!req.session) return res.json({ success: false, error: 'no_session' });
        
        req.session.selectedFor = req.session.selectedFor || {};
        
        if (shortcode) {
            req.session.selectedFor[kind] = {
                shortcode: shortcode,
                username: username,
                at: Date.now()
            };
            // Compatibilidade com lógica antiga de array único
            req.session.selectedPosts = req.session.selectedPosts || [];
            if (!req.session.selectedPosts.find(p => p.shortcode === shortcode)) {
                req.session.selectedPosts.push({ shortcode, username });
            }
        } else {
            // Se shortcode vazio, remove seleção
            if (req.session.selectedFor[kind]) delete req.session.selectedFor[kind];
        }
        
        return res.json({ success: true, selectedFor: req.session.selectedFor });
    } catch (e) {
        return res.status(500).json({ success: false, error: e.message });
    }
});

// Rota para obter seleções atuais
app.get('/api/instagram/selected-for', (req, res) => {
    const selectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};
    return res.json({ success: true, selectedFor });
});

app.use(async (req, res, next) => {
  try {
    const p = req.session && req.session.instagramProfile;
    if (p && p.username) {
      const uname = String(p.username).trim().toLowerCase();
      const k = `_validet_${uname}`;
      if (!req.session[k]) {
        const col = await getCollection('validated_insta_users');
        try { await col.updateOne({ username: uname }, { $setOnInsert: { username: uname, createdAt: new Date().toISOString() }, $set: { checkedAt: new Date().toISOString(), source: 'middleware.session.profile' } }, { upsert: true }); } catch(_) {}
        req.session[k] = true;
      }
    }
  } catch (_) {}
  next();
});

// Home: renderizar Checkout como página inicial
app.get('/', (req, res) => {
    console.log('🏠 Acessando rota / (home -> checkout)');
    try {
        if (req.session) {
            req.session.selectedOrderID = undefined;
            req.session.lastPaidIdentifier = '';
            req.session.lastPaidCorrelationID = '';
        }
    } catch (_) {}
    res.render('checkout', { 
        PIXEL_ID: process.env.PIXEL_ID || ''
    }, (err, html) => {
        if (err) {
            console.error('❌ Erro ao renderizar home/checkout:', err.message);
            return res.status(500).send('Erro ao renderizar checkout');
        }
        res.type('text/html');
        res.send(html);
    });
});

// Página de Termos de Uso
app.get('/termos', (req, res) => {
  res.render('termos', {}, (err, html) => {
    if (err) {
      console.error('Erro ao renderizar termos:', err.message);
      return res.status(500).send('Erro ao abrir Termos de Uso');
    }
    res.type('text/html');
    res.send(html);
  });
});

app.get('/oppus', (req, res) => {
  res.render('oppus', {}, (err, html) => {
    if (err) {
      console.error('Erro ao renderizar oppus:', err.message);
      return res.status(500).send('Erro ao abrir página Oppus');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página dedicada de Cliente (consulta de pedidos)
app.get('/cliente', (req, res) => {
    console.log('👤 Acessando rota /cliente');
    try {
        if (req.session) {
            req.session.lastPaidIdentifier = '';
            req.session.lastPaidCorrelationID = '';
        }
    } catch (_) {}
    res.render('cliente', {}, (err, html) => {
        if (err) {
            console.error('❌ Erro ao renderizar cliente:', err.message);
            return res.status(500).send('Erro ao abrir página do cliente');
        }
        res.type('text/html');
        res.send(html);
    });
});

// Debug: listar rotas registradas
app.get('/__routes', (req, res) => {
    try {
        const stack = app._router?.stack || [];
        const routes = stack
            .filter((layer) => layer.route)
            .map((layer) => ({
                path: layer.route.path,
                methods: Object.keys(layer.route.methods)
            }));
        res.json(routes);
    } catch (e) {
        res.status(500).json({ error: 'cannot_list_routes', message: e?.message });
    }
});

// Rota especial para teste123 (DEVE vir ANTES da rota /:slug)
app.get('/teste123', (req, res) => {
    req.session.perfilAccessAllowed = true;
    req.session.linkSlug = 'teste123';
    req.session.linkAccessTime = Date.now();
    res.render('index');
});

// Página de Checkout (nova slug dedicada)
app.get('/checkout', (req, res) => {
    console.log('🛒 Acessando rota /checkout');
    // Limpar dados de posts selecionados na sessão para evitar mistura com navegações antigas
    if (req.session) {
        req.session.selectedFor = {};
        req.session.selectedPosts = [];
    }
    res.render('checkout', { 
        PIXEL_ID: process.env.PIXEL_ID || ''
    }, (err, html) => {
        if (err) {
            console.error('❌ Erro ao renderizar checkout:', err.message);
            return res.status(500).send('Erro ao renderizar checkout');
        }
        // Garantir envio explícito do conteúdo para evitar Content-Length: 0
        res.type('text/html');
        res.send(html);
    });
});

// Página Engajamento (duplicada da checkout até plataforma)
app.get('/engajamento', (req, res) => {
  console.log('📈 Acessando rota /engajamento');
  res.render('engajamento', { 
    PIXEL_ID: process.env.PIXEL_ID || '', 
    queryParams: req.query 
  }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar engajamento:', err.message);
      return res.status(500).send('Erro ao renderizar engajamento');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página Engajamento Novo (com seletor de contexto e cards unificados)
app.get('/engajamento-novo', (req, res) => {
  console.log('✨ Acessando rota /engajamento-novo');
  res.render('engajamento-novo', { 
    PIXEL_ID: process.env.PIXEL_ID || '', 
    queryParams: req.query
  }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar engajamento-novo:', err.message);
      return res.status(500).send('Erro ao renderizar engajamento-novo');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página Serviços (três serviços iguais ao principal)
app.get('/servicos', (req, res) => {
  console.log('🧩 Acessando rota /servicos');
  res.render('servicos', { queryParams: req.query }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar servicos:', err.message);
      return res.status(500).send('Erro ao renderizar serviços');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página Serviços Instagram (cópia do checkout)
app.get('/servicos-instagram', (req, res) => {
  console.log('📸 Acessando rota /servicos-instagram');
  res.render('servicos-instagram', { 
    PIXEL_ID: process.env.PIXEL_ID || '', 
    queryParams: req.query 
  }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar servicos-instagram:', err.message);
      return res.status(500).send('Erro ao renderizar servicos-instagram');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página Serviços Visualizações (baseada em servicos-curtidas)
app.get('/servicos-visualizacoes', (req, res) => {
  console.log('▶️ Acessando rota /servicos-visualizacoes');
  res.render('servicos-visualizacoes', { 
    PIXEL_ID: process.env.PIXEL_ID || '', 
    queryParams: req.query 
  }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar servicos-visualizacoes:', err.message);
      return res.status(500).send('Erro ao renderizar servicos-visualizacoes');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página Serviços Curtidas (estrutura similar à de serviços Instagram)
app.get('/servicos-curtidas', (req, res) => {
  console.log('❤️ Acessando rota /servicos-curtidas');
  res.render('servicos-curtidas', { 
    PIXEL_ID: process.env.PIXEL_ID || '', 
    queryParams: req.query 
  }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar servicos-curtidas:', err.message);
      return res.status(500).send('Erro ao renderizar servicos-curtidas');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página de Refil
app.get('/refil', async (req, res) => {
  console.log('🔁 Acessando rota /refil');
  const normalizeRefilToken = (v) => String(v || '').trim().replace(/[^0-9a-z]/gi, '');
  let token = normalizeRefilToken(req.query.token || '');
  const phoneRaw = String(req.query.phone || '').trim();
  const usernameRaw = String(req.query.username || req.query.user || req.query.u || req.query.instauser || req.query.instagram || '').trim();
  let refilDaysLeft = null;
  let refilIsLifetime = false;
  let whatsappHref = '';
  let lastRefilOrderId = '';
  let refilLinkRec = null;
  let refilBaseOrderDoc = null;
  const extractOrderBumpsStr = (o) => {
    try {
      const mapPaid = (o && o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid === 'object') ? o.additionalInfoMapPaid : {};
      if (typeof mapPaid.order_bumps !== 'undefined') return String(mapPaid.order_bumps || '').trim();
      const map = (o && o.additionalInfoMap && typeof o.additionalInfoMap === 'object') ? o.additionalInfoMap : {};
      if (typeof map.order_bumps !== 'undefined') return String(map.order_bumps || '').trim();
      const arrPaid = Array.isArray(o && o.additionalInfoPaid ? o.additionalInfoPaid : null) ? o.additionalInfoPaid : [];
      const itPaid = arrPaid.find(x => String(x?.key || '').trim().toLowerCase() === 'order_bumps');
      if (itPaid && typeof itPaid.value !== 'undefined') return String(itPaid.value || '').trim();
      const arr = Array.isArray(o && o.additionalInfo ? o.additionalInfo : null) ? o.additionalInfo : [];
      const it = arr.find(x => String(x?.key || '').trim().toLowerCase() === 'order_bumps');
      if (it && typeof it.value !== 'undefined') return String(it.value || '').trim();
    } catch (_) {}
    return '';
  };
  const hasLifetimeWarrantyFromBumpsStr = (bumpsStr) => {
    try {
      const s = String(bumpsStr || '').trim();
      if (!s) return false;
      const parts = s.split(';');
      for (const raw of parts) {
        const part = String(raw || '').trim();
        if (!part) continue;
        const segs = part.split(':');
        const key = String(segs[0] || '').trim().toLowerCase();
        const qtyRaw = segs.length > 1 ? String(segs[1] || '').trim() : '';
        const qtyParsed = qtyRaw ? Number(qtyRaw) : 1;
        const qty = Number.isFinite(qtyParsed) ? qtyParsed : 1;
        if (!(qty > 0)) continue;
        if (key === 'warranty_lifetime' || key === 'warranty_life') return true;
      }
    } catch (_) {}
    return false;
  };
  const verifyLifetimeForRefilLink = async (linkRec) => {
    try {
      const linkedIds = new Set();
      if (linkRec?.orderId) linkedIds.add(String(linkRec.orderId));
      const arr = Array.isArray(linkRec?.orders) ? linkRec.orders : [];
      for (const x of arr) { if (x) linkedIds.add(String(x)); }
      if (!linkedIds.size) return false;
      const { ObjectId } = require('mongodb');
      const ids = [];
      for (const x of linkedIds) {
        const s = String(x || '').trim();
        if (/^[0-9a-fA-F]{24}$/.test(s)) {
          try { ids.push(new ObjectId(s)); } catch (_) {}
        }
      }
      if (!ids.length) return false;
      const col = await getCollection('checkout_orders');
      const paidQuery = { $or: [{ status: 'pago' }, { 'woovi.status': 'pago' }, { paidAt: { $exists: true, $ne: null } }, { 'woovi.paidAt': { $exists: true, $ne: null } }] };
      const orders = await col.find({ $and: [paidQuery, { _id: { $in: ids } }] }, { projection: { additionalInfoMapPaid: 1, additionalInfoPaid: 1, additionalInfoMap: 1, additionalInfo: 1 } }).toArray();
      for (const o of (orders || [])) {
        if (hasLifetimeWarrantyFromBumpsStr(extractOrderBumpsStr(o))) return true;
      }
    } catch (_) {}
    return false;
  };
  try {
    let isValid = false;
    if (token) {
      if (/^liberado$/i.test(token)) {
         isValid = true;
         if (req.session) {
            req.session.refilAccessAllowed = true;
            req.session.linkSlug = 'liberado';
            req.session.linkAccessTime = Date.now();
         }
      } else {
        try { const v = linkManager.validateLink(token, req); isValid = !!(v && v.valid); } catch(_) {}
        if (isValid && req.session) {
          req.session.refilAccessAllowed = true;
          req.session.linkSlug = token;
          req.session.linkAccessTime = Date.now();
        }
        if (!isValid) {
            // Tentar recuperar do banco para ver se apenas expirou ou renovar
            try {
                const tl = await getCollection('temporary_links');
                const linkRec = await tl.findOne({ id: token });
                if (linkRec && String(linkRec.purpose || '').toLowerCase() === 'refil') {
                    const nowMs = Date.now();
                    const expMs = linkRec.expiresAt ? new Date(linkRec.expiresAt).getTime() : 0;
                    const isLifetime = await verifyLifetimeForRefilLink(linkRec);
                    let ok = false;
                    let shouldRenew = false;
                    if (isLifetime) {
                      ok = true;
                    } else if (expMs && nowMs <= expMs) {
                      ok = true;
                    } else {
                      const createdMs = linkRec.createdAt ? new Date(linkRec.createdAt).getTime() : 0;
                      if (createdMs) {
                        const brtOffsetMs = 3 * 60 * 60 * 1000;
                        const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
                        const brtYmdFromMs = (ms) => {
                          const d = new Date(Number(ms || 0) - brtOffsetMs);
                          return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
                        };
                        const addMonthsEndOfDayBrtIso = (baseMs, monthsToAdd) => {
                          const base = brtYmdFromMs(baseMs);
                          let y = base.y;
                          let m = base.m + Number(monthsToAdd || 0);
                          while (m > 12) { y += 1; m -= 12; }
                          while (m < 1) { y -= 1; m += 12; }
                          const maxDay = daysInMonth(y, m);
                          const dd = Math.min(base.d, maxDay);
                          const utcMs = Date.UTC(y, m - 1, dd, 23, 59, 59, 999) + brtOffsetMs;
                          return new Date(utcMs).toISOString();
                        };
                        const monthsToAdd = 1;
                        const warrantyEndIso = addMonthsEndOfDayBrtIso(createdMs, monthsToAdd);
                        const warrantyEndMs = warrantyEndIso ? new Date(warrantyEndIso).getTime() : 0;
                        if (warrantyEndMs && nowMs <= warrantyEndMs) {
                          ok = true;
                          shouldRenew = true;
                        }
                      }
                    }
                    if (ok) {
                      if (shouldRenew) {
                        const createdMs = linkRec.createdAt ? new Date(linkRec.createdAt).getTime() : 0;
                        if (createdMs) {
                          const brtOffsetMs = 3 * 60 * 60 * 1000;
                          const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
                          const brtYmdFromMs = (ms) => {
                            const d = new Date(Number(ms || 0) - brtOffsetMs);
                            return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
                          };
                          const addMonthsEndOfDayBrtIso = (baseMs, monthsToAdd) => {
                            const base = brtYmdFromMs(baseMs);
                            let y = base.y;
                            let m = base.m + Number(monthsToAdd || 0);
                            while (m > 12) { y += 1; m -= 12; }
                            while (m < 1) { y -= 1; m += 12; }
                            const maxDay = daysInMonth(y, m);
                            const dd = Math.min(base.d, maxDay);
                            const utcMs = Date.UTC(y, m - 1, dd, 23, 59, 59, 999) + brtOffsetMs;
                            return new Date(utcMs).toISOString();
                          };
                          const monthsToAdd = 1;
                          const newExp = addMonthsEndOfDayBrtIso(createdMs, monthsToAdd);
                          if (newExp) await tl.updateOne({ id: token }, { $set: { expiresAt: newExp } });
                        }
                      }
                      isValid = true;
                      if (req.session) {
                        req.session.refilAccessAllowed = true;
                        req.session.linkSlug = token;
                        req.session.linkAccessTime = nowMs;
                      }
                    }
                }
            } catch(_) {}
        }
      }
    }

    // Fallback: se o token fornecido não funcionou (ou não veio), mas temos telefone
    if (!isValid && phoneRaw) {
      try {
        const digits = phoneRaw.replace(/\D/g, '');
        if (digits) {
           const tl = await getCollection('temporary_links');
           const linkRec = await tl.findOne({ purpose: 'refil', phone: digits });
           if (linkRec && linkRec.id) {
             const nowMs = Date.now();
             const expMs = linkRec.expiresAt ? new Date(linkRec.expiresAt).getTime() : 0;
             const isLifetime = await verifyLifetimeForRefilLink(linkRec);
             let ok = false;
             let shouldRenew = false;
             if (isLifetime) {
               ok = true;
             } else if (expMs && nowMs <= expMs) {
               ok = true;
             } else {
               const createdMs = linkRec.createdAt ? new Date(linkRec.createdAt).getTime() : 0;
               if (createdMs) {
                 const brtOffsetMs = 3 * 60 * 60 * 1000;
                 const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
                 const brtYmdFromMs = (ms) => {
                   const d = new Date(Number(ms || 0) - brtOffsetMs);
                   return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
                 };
                 const addMonthsEndOfDayBrtIso = (baseMs, monthsToAdd) => {
                   const base = brtYmdFromMs(baseMs);
                   let y = base.y;
                   let m = base.m + Number(monthsToAdd || 0);
                   while (m > 12) { y += 1; m -= 12; }
                   while (m < 1) { y -= 1; m += 12; }
                   const maxDay = daysInMonth(y, m);
                   const dd = Math.min(base.d, maxDay);
                   const utcMs = Date.UTC(y, m - 1, dd, 23, 59, 59, 999) + brtOffsetMs;
                   return new Date(utcMs).toISOString();
                 };
                 const monthsToAdd = 1;
                 const warrantyEndIso = addMonthsEndOfDayBrtIso(createdMs, monthsToAdd);
                 const warrantyEndMs = warrantyEndIso ? new Date(warrantyEndIso).getTime() : 0;
                 if (warrantyEndMs && nowMs <= warrantyEndMs) {
                   ok = true;
                   shouldRenew = true;
                 }
               }
             }
             if (ok) {
               token = linkRec.id;
               if (shouldRenew) {
                 const createdMs = linkRec.createdAt ? new Date(linkRec.createdAt).getTime() : 0;
                 if (createdMs) {
                   const brtOffsetMs = 3 * 60 * 60 * 1000;
                   const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
                   const brtYmdFromMs = (ms) => {
                     const d = new Date(Number(ms || 0) - brtOffsetMs);
                     return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
                   };
                   const addMonthsEndOfDayBrtIso = (baseMs, monthsToAdd) => {
                     const base = brtYmdFromMs(baseMs);
                     let y = base.y;
                     let m = base.m + Number(monthsToAdd || 0);
                     while (m > 12) { y += 1; m -= 12; }
                     while (m < 1) { y -= 1; m += 12; }
                     const maxDay = daysInMonth(y, m);
                     const dd = Math.min(base.d, maxDay);
                     const utcMs = Date.UTC(y, m - 1, dd, 23, 59, 59, 999) + brtOffsetMs;
                     return new Date(utcMs).toISOString();
                   };
                   const monthsToAdd = 1;
                   const newExp = addMonthsEndOfDayBrtIso(createdMs, monthsToAdd);
                   if (newExp) await tl.updateOne({ id: token }, { $set: { expiresAt: newExp } });
                 }
               }
               isValid = true;
               if (req.session) {
                 req.session.refilAccessAllowed = true;
                 req.session.linkSlug = token;
                 req.session.linkAccessTime = nowMs;
               }
               console.log('🔁 Refil: Acesso recuperado via telefone:', digits);
             }
           }
        }
      } catch(_) {}
    }

    try {
      const slugFromQuery = token && !/^liberado$/i.test(token) ? normalizeRefilToken(token) : '';
      const slugFromSession = req.session && req.session.refilAccessAllowed ? String(req.session.linkSlug || '').trim() : '';
      const slugFromSessionNorm = (slugFromSession && slugFromSession !== 'liberado') ? normalizeRefilToken(slugFromSession) : '';
      const slug = slugFromQuery || slugFromSessionNorm;
      if (slug) {
        const tl = await getCollection('temporary_links');
        const linkRec = await tl.findOne({ id: slug }, { projection: { createdAt: 1, expiresAt: 1, warrantyMode: 1, warrantyDays: 1, phone: 1, instauser: 1, instausers: 1, orderId: 1, orders: 1, order: 1 } });
        refilLinkRec = linkRec || null;
        if (linkRec && await verifyLifetimeForRefilLink(linkRec)) {
          refilIsLifetime = true;
        } else if (linkRec && linkRec.expiresAt) {
          const exp = new Date(linkRec.expiresAt).getTime();
          if (exp) {
            const brtOffsetMs = 3 * 60 * 60 * 1000;
            const dayMs = 24 * 60 * 60 * 1000;
            const brtYmdFromMs = (ms) => {
              const d = new Date(Number(ms || 0) - brtOffsetMs);
              return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
            };
            const dayNumFromYmd = (y, m, d) => Math.floor(Date.UTC(Number(y), Number(m) - 1, Number(d)) / dayMs);
            const nowYmd = brtYmdFromMs(Date.now());
            const nowDay = dayNumFromYmd(nowYmd.y, nowYmd.m, nowYmd.d);
            const expYmd = brtYmdFromMs(exp);
            const expDay = dayNumFromYmd(expYmd.y, expYmd.m, expYmd.d);
            refilDaysLeft = Math.max(0, expDay - nowDay);
          }
        } else if (linkRec && linkRec.createdAt) {
          const created = new Date(linkRec.createdAt).getTime();
          if (created) {
            const brtOffsetMs = 3 * 60 * 60 * 1000;
            const dayMs = 24 * 60 * 60 * 1000;
            const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
            const brtYmdFromMs = (ms) => {
              const d = new Date(Number(ms || 0) - brtOffsetMs);
              return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
            };
            const dayNumFromYmd = (y, m, d) => Math.floor(Date.UTC(Number(y), Number(m) - 1, Number(d)) / dayMs);
            const addMonthsEndOfDayBrtIso = (baseMs, monthsToAdd) => {
              const base = brtYmdFromMs(baseMs);
              let y = base.y;
              let m = base.m + Number(monthsToAdd || 0);
              while (m > 12) { y += 1; m -= 12; }
              while (m < 1) { y -= 1; m += 12; }
              const maxDay = daysInMonth(y, m);
              const dd = Math.min(base.d, maxDay);
              const utcMs = Date.UTC(y, m - 1, dd, 23, 59, 59, 999) + brtOffsetMs;
              return { iso: new Date(utcMs).toISOString(), y, m, d: dd };
            };
            const monthsToAdd = 1;
            const exp = addMonthsEndOfDayBrtIso(created, monthsToAdd);
            const nowYmd = brtYmdFromMs(Date.now());
            const nowDay = dayNumFromYmd(nowYmd.y, nowYmd.m, nowYmd.d);
            const expDay = dayNumFromYmd(exp.y, exp.m, exp.d);
            refilDaysLeft = Math.max(0, expDay - nowDay);
          }
        }
      }
    } catch(_) {}
  } catch(_) {}
  if (!(req.session && req.session.refilAccessAllowed)) {
    const from = '/refil';
    const qs = token ? `from=${encodeURIComponent(from)}&token=${encodeURIComponent(token)}` : `from=${encodeURIComponent(from)}`;
    return res.redirect(`/restrito?${qs}`);
  }
  if (!refilLinkRec && token && !/^liberado$/i.test(token)) {
    try {
      const tl = await getCollection('temporary_links');
      const linkRec = await tl.findOne({ id: String(token).trim() }, { projection: { createdAt: 1, expiresAt: 1, warrantyMode: 1, warrantyDays: 1, phone: 1, instauser: 1, instausers: 1, orderId: 1, orders: 1, order: 1 } });
      if (linkRec) refilLinkRec = linkRec;
    } catch (_) {}
  }
  try {
    const waPhone = '553173425727';
    const esc = (s) => String(s || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pickPaid = (d) => {
      if (!d) return false;
      const st = String(d.status || '').toLowerCase();
      const wst = String(d?.woovi?.status || '').toLowerCase();
      if (st === 'pago' || wst === 'pago') return true;
      if (d.paidAt) return true;
      if (d?.woovi?.paidAt) return true;
      if (d?.payment?.paidAt) return true;
      return false;
    };
    const pickOrderId = (d) => {
      if (!d) return '';
      const a = d.fama24h && (d.fama24h.orderId || d.fama24h.id) ? (d.fama24h.orderId || d.fama24h.id) : '';
      const b = d.fornecedor_social && (d.fornecedor_social.orderId || d.fornecedor_social.id) ? (d.fornecedor_social.orderId || d.fornecedor_social.id) : '';
      const ident = d.identifier || (d.woovi && d.woovi.identifier) || '';
      const mongoId = d._id ? String(d._id || '').trim() : '';
      return String(a || b || ident || mongoId || '').trim();
    };

    const tokenFromSession = (req.session && req.session.refilAccessAllowed && req.session.linkSlug) ? normalizeRefilToken(req.session.linkSlug || '') : '';
    const refilTokenLookup = (token && !/^liberado$/i.test(token)) ? normalizeRefilToken(token) : ((tokenFromSession && !/^liberado$/i.test(tokenFromSession)) ? tokenFromSession : '');

    const col = await getCollection('checkout_orders');
    if (refilTokenLookup) {
      try {
        const arr = await col.find({ refilLinkId: refilTokenLookup }).sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 }).limit(10).toArray();
        refilBaseOrderDoc = arr.find(pickPaid) || (arr.length ? arr[0] : null);
      } catch (_) {}
    }
    if (!refilBaseOrderDoc && refilLinkRec && refilLinkRec.phone) {
      try {
        const digits = String(refilLinkRec.phone || '').replace(/\D/g, '');
        if (digits) {
          const conds = [
            { 'customer.phone': `+55${digits}` },
            { 'customer.phone': digits },
            { additionalInfo: { $elemMatch: { key: 'phone', value: digits } } },
            { additionalInfoPaid: { $elemMatch: { key: 'phone', value: digits } } }
          ];
          const arr = await col.find({ $or: conds }).sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 }).limit(10).toArray();
          refilBaseOrderDoc = arr.find(pickPaid) || (arr.length ? arr[0] : null);
        }
      } catch (_) {}
    }
    if (!refilBaseOrderDoc && phoneRaw) {
      try {
        const digits = String(phoneRaw || '').replace(/\D/g, '');
        if (digits) {
          const conds = [
            { 'customer.phone': `+55${digits}` },
            { 'customer.phone': digits },
            { additionalInfo: { $elemMatch: { key: 'phone', value: digits } } },
            { additionalInfoPaid: { $elemMatch: { key: 'phone', value: digits } } }
          ];
          const arr = await col.find({ $or: conds }).sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 }).limit(10).toArray();
          refilBaseOrderDoc = arr.find(pickPaid) || (arr.length ? arr[0] : null);
        }
      } catch (_) {}
    }

    if (refilLinkRec && refilLinkRec.order) {
      const fromLink = pickOrderId(refilLinkRec.order);
      if (fromLink) lastRefilOrderId = fromLink;
    }
    if (!lastRefilOrderId && refilLinkRec && refilLinkRec.orderId) {
      lastRefilOrderId = String(refilLinkRec.orderId || '').trim();
    }
    if (!lastRefilOrderId && refilBaseOrderDoc) {
      lastRefilOrderId = pickOrderId(refilBaseOrderDoc);
    }

    whatsappHref = `https://wa.me/${waPhone}?text=${encodeURIComponent(`Perdi seguidores e preciso de reposição. ID do meu pedido: ${lastRefilOrderId || ''}`)}`;

    let username = '';
    if (refilLinkRec) {
      username = String(refilLinkRec.instauser || '').trim();
      if (!username && Array.isArray(refilLinkRec.instausers) && refilLinkRec.instausers.length) {
        username = String(refilLinkRec.instausers[refilLinkRec.instausers.length - 1] || '').trim();
      }
    }
    if (!username && usernameRaw) {
      username = usernameRaw;
    }

    let lastDoc = null;
    try {
      const { ObjectId } = require('mongodb');
      if (refilLinkRec) {
        const oid = String(refilLinkRec.orderId || (refilLinkRec.order && refilLinkRec.order._id) || '').trim();
        if (/^[0-9a-fA-F]{24}$/.test(oid)) {
          try { lastDoc = await col.findOne({ _id: new ObjectId(oid) }); } catch (_) {}
        }
        if (!lastDoc && Array.isArray(refilLinkRec.orders) && refilLinkRec.orders.length) {
          const ids = refilLinkRec.orders.map(v => String(v || '').trim()).filter(v => /^[0-9a-fA-F]{24}$/.test(v)).map(v => new ObjectId(v));
          if (ids.length) {
            const arr = await col.find({ _id: { $in: ids } }).sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 }).limit(10).toArray();
            lastDoc = arr.find(pickPaid) || null;
          }
        }
      }
    } catch (_) {}
    if (!lastDoc && refilBaseOrderDoc) {
      lastDoc = refilBaseOrderDoc;
    }
    if (lastDoc && !username) {
      try {
        const u0 = String(lastDoc.instagramUsername || lastDoc.instauser || '').trim();
        if (u0) username = u0;
        if (!username) {
          const arrPaid = Array.isArray(lastDoc?.additionalInfoPaid) ? lastDoc.additionalInfoPaid : [];
          const arrOrig = Array.isArray(lastDoc?.additionalInfo) ? lastDoc.additionalInfo : [];
          const arr = arrPaid.length ? arrPaid : arrOrig;
          const u1 = (arr || []).find(it => String(it?.key || '').trim() === 'instagram_username');
          const u2 = u1 && u1.value ? String(u1.value || '').trim() : '';
          if (u2) username = u2;
        }
      } catch (_) {}
    }
    if (!lastDoc && username) {
      const re = new RegExp(`^${esc(username)}$`, 'i');
      const arr = await col.find({ $or: [ { instauser: re }, { instagramUsername: re } ] }).sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 }).limit(10).toArray();
      lastDoc = arr.find(pickPaid) || null;
    }

    if (lastDoc && pickPaid(lastDoc)) {
      const oid = pickOrderId(lastDoc);
      if (oid) {
        lastRefilOrderId = oid;
        const msg = `Perdi seguidores e preciso de reposição. ID do meu pedido: ${lastRefilOrderId}`;
        whatsappHref = `https://wa.me/${waPhone}?text=${encodeURIComponent(msg)}`;
      }
    }
  } catch (_) {}

  let refilOrderMongoId = (refilLinkRec && (refilLinkRec.orderId || (refilLinkRec.order && refilLinkRec.order._id))) ? String(refilLinkRec.orderId || (refilLinkRec.order && refilLinkRec.order._id) || '').trim() : '';
  if (!refilOrderMongoId && refilBaseOrderDoc && refilBaseOrderDoc._id) {
    refilOrderMongoId = String(refilBaseOrderDoc._id || '').trim();
  }
  if (!refilOrderMongoId && token && !/^liberado$/i.test(token)) {
    try {
      const tl = await getCollection('temporary_links');
      const linkRec = await tl.findOne({ id: String(token).trim() }, { projection: { orderId: 1, order: 1 } });
      const oid = linkRec && (linkRec.orderId || (linkRec.order && linkRec.order._id)) ? String(linkRec.orderId || (linkRec.order && linkRec.order._id) || '').trim() : '';
      if (oid) refilOrderMongoId = oid;
    } catch (_) {}
  }
  const refilToken = (token && !/^liberado$/i.test(token)) ? String(token || '').trim() : (
    (req.session && req.session.refilAccessAllowed && req.session.linkSlug && !/^liberado$/i.test(req.session.linkSlug))
      ? String(req.session.linkSlug || '').trim().replace(/[^0-9a-z]/gi, '')
      : (token ? String(token || '').trim() : '')
  );
  res.render('refil', { refilDaysLeft, refilIsLifetime, whatsappHref, lastRefilOrderId, refilOrderMongoId, refilToken }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar refil:', err.message);
      return res.status(500).send('Erro ao carregar página de refil');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Rota antiga de refil removida em favor da nova implementação (ver final do arquivo)

// API: criar cobrança Cartão via Efí
app.post('/api/efi/card-charge', async (req, res) => {
    let correlationIDSafe = '';
    let validatedValueCents = null;
    const isSandboxEnv = process.env.EFI_SANDBOX === 'true';
    try {
        const { payment_token, installments, customer, billing_address, items, total_cents, additionalInfo, profile_is_private, comment, utms: reqUtms } = req.body;
        correlationIDSafe = (function () {
            try {
                const c = String(req.body?.correlationID || '').trim();
                if (c) return c;
            } catch (_) {}
            try {
                const crypto = require('crypto');
                if (crypto && typeof crypto.randomUUID === 'function') return crypto.randomUUID();
            } catch (_) {}
            return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
        })();

        // Validação mínima de valor (R$ 1,00 = 100 centavos)
        if (!total_cents || total_cents < 100) {
            return res.status(400).json({ 
                error: 'invalid_amount', 
                message: 'Valor mínimo para cartão de crédito é R$ 1,00.' 
            });
        }

        // Validação de Preço (Backend) - Validação Estrita para TODOS os pedidos
        const addInfoArr = Array.isArray(additionalInfo) ? additionalInfo : [];
        try {
            const hasTc = addInfoArr.some((it) => String(it?.key || '').trim() === 'tc_code');
            if (!hasTc) {
                const cookieHeader = req.headers['cookie'] || '';
                const m = cookieHeader && cookieHeader.match(/(?:^|;\s*)tc_code=([^;]+)/);
                const tc = m && m[1] ? decodeURIComponent(m[1]) : '';
                if (tc) addInfoArr.push({ key: 'tc_code', value: String(tc) });
            }
        } catch (_) {}
        const addInfoMap = addInfoArr.reduce((acc, item) => {
             const k = String(item?.key || '').trim();
             const v = String(item?.value || '').trim();
             acc[k] = v;
             return acc;
        }, {});
        
        const tipoVal = addInfoMap['tipo_servico'] || '';
        const qtdVal = Number(addInfoMap['quantidade'] || 0) || 0;
        
        let validatedPriceCents = null;

        // Tenta validar TODOS os pedidos usando pricing.js
        const verification = await verifyPrice(tipoVal, qtdVal, addInfoArr, Number(total_cents));
        
        if (verification.isValid) {
             // Preço validado com sucesso
             validatedPriceCents = verification.matchedPrice;
        } else {
             // Se falhar, rejeita
             console.warn(`⚠️ Bloqueio de Pagamento (Cartão): Valor incorreto. Tipo=${tipoVal}, Qtd=${qtdVal}, Valor=${total_cents}, Esperado=${verification.expectedPrice}`);
             return res.status(400).json({ 
                 error: 'value_mismatch', 
                 message: 'O valor do pagamento não corresponde ao preço oficial calculado pelo sistema.' 
             });
        }

        const options = {
            sandbox: process.env.EFI_SANDBOX === 'true',
            client_id: process.env.EFI_CLIENT_ID_HM,
            client_secret: process.env.EFI_CLIENT_SECRET_HM
        };

        const efipay = new EfiPay(options);

        const normalizeDigits = (v) => String(v || '').replace(/\D/g, '');
        const normalizeEfiPhone = (raw) => {
            let d = normalizeDigits(raw);
            if (!d) return '';
            d = d.replace(/^0+/, '');
            if (d.startsWith('55') && d.length > 11) d = d.slice(2);
            if (d.length > 11) d = d.slice(-11);
            if (!(d.length === 10 || d.length === 11)) return '';
            if (d.startsWith('0')) return '';
            if (d.length === 11 && d[2] !== '9') {
              const d2 = d.slice(0, 2) + d.slice(3);
              if (/^[1-9]{2}[0-9]{8}$/.test(d2)) d = d2;
            }
            if (!/^[1-9]{2}9?[0-9]{8}$/.test(d)) return '';
            return d;
        };
        const isValidCPF = (cpfRaw) => {
            const cpf = normalizeDigits(cpfRaw);
            if (cpf.length !== 11) return false;
            if (/^(\d)\1+$/.test(cpf)) return false;
            const calc = (base, factor) => {
                let sum = 0;
                for (let i = 0; i < base.length; i++) sum += Number(base[i]) * (factor - i);
                const mod = (sum * 10) % 11;
                return mod === 10 ? 0 : mod;
            };
            const d1 = calc(cpf.slice(0, 9), 10);
            const d2 = calc(cpf.slice(0, 10), 11);
            return d1 === Number(cpf[9]) && d2 === Number(cpf[10]);
        };

        const sanitizeItemName = (s) => {
            const raw = String(s || '').replace(/[<>]/g, '').trim();
            return raw ? raw : 'Checkout OPPUS';
        };
        const safeInt = (n) => {
            const v = Number(n);
            if (!Number.isFinite(v)) return null;
            const iv = Math.round(v);
            return Number.isFinite(iv) ? iv : null;
        };

        const qtdSafe = (Number.isFinite(Number(qtdVal)) && Number(qtdVal) > 0) ? Number(qtdVal) : 1;
        const tipoSafe = String(tipoVal || '').trim();
        const validatedValue = safeInt(validatedPriceCents);
        if (validatedValue === null || validatedValue < 100) {
            return res.status(400).json({ error: 'invalid_amount', message: 'Valor inválido para cartão.' });
        }
        validatedValueCents = validatedValue;

        const efiItems = [{
            name: sanitizeItemName(`${qtdSafe} ${tipoSafe}`),
            value: validatedValue,
            amount: 1
        }];

        const cpfDigits = normalizeDigits(customer?.cpf);
        if (!cpfDigits) {
            return res.status(400).json({ error: 'invalid_cpf', message: 'CPF do titular do cartão é obrigatório.' });
        }
        if (!isValidCPF(cpfDigits)) {
            return res.status(400).json({ error: 'invalid_cpf', message: 'CPF do titular do cartão é inválido.' });
        }

        const phoneRaw = customer?.phone_number || customer?.phone || customer?.telefone || customer?.whatsapp || '';
        const phoneDigits = normalizeEfiPhone(phoneRaw);
        const phoneHasInput = normalizeDigits(phoneRaw).length > 0;
        if (phoneHasInput && !phoneDigits) {
            return res.status(400).json({ error: 'invalid_phone', message: 'Telefone inválido. Informe DDD + número (10 ou 11 dígitos).' });
        }
        if (!phoneDigits) {
            return res.status(400).json({ error: 'missing_phone', message: 'Telefone (DDD + número) é obrigatório para pagamento no cartão.' });
        }

        const customerSafe = Object.assign({}, customer || {}, { cpf: cpfDigits }, { phone_number: phoneDigits });

        const params = {};
        const body = {
            payment: {
                credit_card: {
                    installments: Number(installments) || 1,
                    payment_token: payment_token,
                    billing_address: billing_address || {
                        street: 'Av. Paulista',
                        number: 1000,
                        neighborhood: 'Bela Vista',
                        zipcode: '01310100',
                        city: 'São Paulo',
                        state: 'SP'
                    },
                    customer: Object.assign({
                        name: String(customerSafe?.name || '').trim() || 'Cliente',
                        cpf: cpfDigits,
                        email: customerSafe.email || 'cliente@email.com',
                        birth: customerSafe.birth || '1990-01-01'
                    }, (phoneDigits ? { phone_number: phoneDigits } : {}))
                }
            },
            items: efiItems,
            shippings: [
                { name: 'Frete', value: 0 }
            ]
        };

        console.log('💳 Processando pagamento cartão Efí:', { total_cents, installments, has_payment_token: !!payment_token, item_value: validatedValue });

        const charge = await Promise.race([
            efipay.createOneStepCharge(params, body),
            new Promise((_, reject) => setTimeout(() => reject(Object.assign(new Error('payment_timeout'), { code: 'PAYMENT_TIMEOUT' })), 25000))
        ]);

        if (charge && charge.data && charge.data.status !== 'error') {
            // Validação da Resposta (Garantia de que o valor cobrado é o esperado)
            let mismatchStatus = null;
            if (validatedPriceCents !== null && charge.data.total !== validatedPriceCents) {
                console.error(`🚨 CRITICAL: Efí charge total mismatch! Expected: ${validatedPriceCents}, Got: ${charge.data.total}`);
                mismatchStatus = 'divergent_value';
            }

            // Persistência (adaptada do Woovi)
            const addInfoArr = Array.isArray(additionalInfo) ? additionalInfo : [];
            const addInfo = addInfoArr.reduce((acc, item) => {
                const k = String(item?.key || '').trim();
                const v = String(item?.value || '').trim();
                acc[k] = v;
                return acc;
            }, {});

            let utms = {};
            try {
                if (reqUtms && Object.keys(reqUtms).length > 0) {
                    utms = reqUtms;
                    if (!utms.ref) utms.ref = req.get('Referer') || req.headers['referer'] || '';
                } else {
                    const refUrl = req.get('Referer') || req.headers['referer'] || '';
                    utms = { ref: refUrl };
                }
            } catch(_) {
                const refUrl = req.get('Referer') || req.headers['referer'] || '';
                utms = { ref: refUrl };
            }

            const tipo = addInfo['tipo_servico'] || '';
            const qtd = Number(addInfo['quantidade'] || 0) || 0;
            const instauserFromClient = addInfo['instagram_username'] || '';
            const userAgent = req.get('User-Agent') || '';
            const ip = req.realIP || req.ip || req.connection?.remoteAddress || 'unknown';
            const slug = req.session?.linkSlug || '';
            const isPrivate = profile_is_private === true || profile_is_private === 'true' || addInfo['profile_is_private'] === 'true';

            // Geolocalization
            let geolocation = null;
            try {
                geolocation = await geoLookupIp(ip);
            } catch(_) {}

            const createdIso = new Date().toISOString();
            
            // Status Efí: 'new', 'waiting', 'paid', 'unpaid', 'refunded', 'contested', 'canceled', 'settled'
            let sysStatus = mismatchStatus || 'pendente';
            if (!mismatchStatus && (charge.data.status === 'paid' || charge.data.status === 'settled')) sysStatus = 'pago';

            const record = {
                nomeUsuario: null,
                telefone: phoneDigits || '',
                instauser: instauserFromClient,
                profilePrivacy: { isPrivate: isPrivate, checkedAt: createdIso },
                isPrivate: isPrivate,
                criado: createdIso,
                identifier: String(charge.data.charge_id),
                correlationID: correlationIDSafe,
                status: sysStatus,
                qtd,
                tipo,
                utms,
                geolocation,
                valueCents: total_cents,
                expectedValueCents: validatedPriceCents,
                customer: customerSafe,
                additionalInfo: addInfoArr,
                tipoServico: tipo,
                quantidade: qtd,
                instagramUsername: instauserFromClient,
                slug,
                userAgent,
                ip,
                createdAt: createdIso,
                paymentMethod: 'credit_card',
                efi: {
                    charge_id: charge.data.charge_id,
                    status: charge.data.status,
                    total: charge.data.total,
                    installments: charge.data.payment?.credit_card?.installments,
                    card_mask: charge.data.payment?.credit_card?.card_mask
                }
            };

            const col = await getCollection('checkout_orders');
            const insertResult = await col.insertOne(record);
            try { record._id = insertResult?.insertedId; } catch (_) {}
            console.log('🗃️ MongoDB: pedido cartão persistido (Efí charge_id=', charge.data.charge_id, ')');
            try {
              if (sysStatus === 'pago') {
                await processOrderFulfillment(record, col, req);
              }
            } catch (_) {}

            return res.json({ success: true, charge: charge.data });
        } else {
            console.error('❌ Efí retornou erro ou status inválido:', charge);
            return res.status(400).json({ error: 'payment_failed', details: charge });
        }

    } catch (error) {
        console.error('❌ Erro pagamento cartão Efí:', error);
        if (String(error?.code || '') === 'PAYMENT_TIMEOUT' || String(error?.message || '') === 'payment_timeout') {
            return res.status(504).json({ error: 'payment_timeout', message: 'Tempo esgotado processando o pagamento. Tente novamente.' });
        }
        const errDesc = error?.error_description;
        const errDescObj = (errDesc && typeof errDesc === 'object') ? errDesc : null;
        const errMsg = String(errDescObj?.message || errDesc || error?.message || error?.error || '').trim();
        const errProp = String(errDescObj?.property || '').trim();
        const code = (typeof error?.code !== 'undefined') ? error.code : undefined;
        const status = (typeof error?.http_status !== 'undefined') ? error.http_status : undefined;
        const codeStr = String(code ?? '').trim();
        if (codeStr === '4600037') {
            const cents = Number.isFinite(Number(validatedValueCents)) ? Number(validatedValueCents) : (Number.isFinite(Number(req.body?.total_cents)) ? Number(req.body.total_cents) : null);
            const valueLabel = Number.isFinite(cents) ? ` (valor: R$ ${(cents / 100).toFixed(2).replace('.', ',')})` : '';
            const envLabel = isSandboxEnv ? 'sandbox' : 'produção';
            return res.status(400).json({
                error: 'efi_operational_limit',
                message: `O valor da cobrança${valueLabel} é superior ao limite operacional da conta Efí (${envLabel}). Aumente o limite no painel da Efí (no mesmo ambiente) ou use PIX.`,
                code,
                status,
                correlationID: correlationIDSafe,
                value_cents: Number.isFinite(cents) ? cents : undefined,
                sandbox: isSandboxEnv
            });
        }
        if (String(error?.error || '') === 'validation_error') {
            if (errProp === '/payment/credit_card/customer/phone_number') {
                const low = String(errMsg || '').toLowerCase();
                const required = /obrigat|required/.test(low);
                return res.status(400).json({ error: required ? 'missing_phone' : 'invalid_phone', message: required ? 'Telefone (DDD + número) é obrigatório para pagamento no cartão.' : 'Telefone inválido. Informe DDD + número (10 ou 11 dígitos).', code, status });
            }
            if (errProp === '/payment/credit_card/customer/cpf') {
                return res.status(400).json({ error: 'invalid_cpf', message: 'CPF do titular do cartão é inválido.', code, status });
            }
            return res.status(400).json({ error: 'validation_error', message: errMsg || 'Erro de validação na Efí.', code, status });
        }
        return res.status(500).json({ error: 'payment_error', message: errMsg || 'Erro ao processar pagamento.', code, status });
    }
});

// API: criar cobrança PIX via Woovi
app.post('/api/woovi/charge', async (req, res) => {
    // DEBUG: Log incoming request body
    console.log('DEBUG: /api/woovi/charge called', JSON.stringify(req.body, null, 2));

    // ... (código existente da Woovi)

    const WOOVI_AUTH = process.env.WOOVI_AUTH || 'Q2xpZW50X0lkXzI1OTRjODMwLWExN2YtNDc0Yy05ZTczLWJjNDRmYTc4NTU2NzpDbGllbnRfU2VjcmV0X0NCVTF6Szg4eGJyRTV0M1IxVklGZEpaOHZLQ0N4aGdPR29UQnE2dDVWdU09';
    const {
        correlationID,
        value,
        comment,
        customer,
        additionalInfo,
        profile_is_private
    } = req.body || {};

    if (!value || typeof value !== 'number' || value < 10) {
        return res.status(400).json({ error: 'invalid_value', message: 'Campo value (centavos) é obrigatório e deve ser no mínimo 10 (R$ 0,10).' });
    }

    // Função para remover emojis (pares substitutos) e normalizar travessões para hífen
    const sanitizeText = (s) => {
        if (typeof s !== 'string') return s;
        return s
            .replace(/[\u2012-\u2015]/g, '-') // dashes (figura, en, em)
            .replace(/[\uD800-\uDFFF]/g, '')  // surrogate pairs (emojis)
            .trim();
    };

    const sanitizedAdditional = Array.isArray(additionalInfo)
        ? additionalInfo.map((item) => ({
            key: sanitizeText(String(item?.key ?? '')),
            value: sanitizeText(String(item?.value ?? '')),
          }))
        : [];
    const sanitizedAdditionalFiltered = sanitizedAdditional
        .filter((it) => typeof it.key === 'string' && it.key.trim().length > 0 && typeof it.value === 'string' && it.value.trim().length > 0)
        .map((it) => ({ key: it.key.trim(), value: it.value.trim() }));

    try {
        const cookieHeader = req.headers['cookie'] || '';
        const m = cookieHeader && cookieHeader.match(/(?:^|;\s*)tc_code=([^;]+)/);
        const tc = m && m[1] ? decodeURIComponent(m[1]) : '';
        if (tc) {
            for (let i = sanitizedAdditionalFiltered.length - 1; i >= 0; i--) {
                if (sanitizedAdditionalFiltered[i].key === 'tc_code') {
                    sanitizedAdditionalFiltered.splice(i, 1);
                }
            }
            sanitizedAdditionalFiltered.push({ key: 'tc_code', value: tc });
        }
    } catch (_) {}

    // Validação de Preço (Backend Woovi) - Validação Estrita para TODOS os pedidos
    const addInfoMap = sanitizedAdditionalFiltered.reduce((acc, item) => {
         acc[item.key] = item.value;
         return acc;
    }, {});
    
    const tipoVal = addInfoMap['tipo_servico'] || '';
    const qtdVal = Number(addInfoMap['quantidade'] || 0) || 0;
    
    // DEBUG: Log detalhado da requisição para diagnosticar erro 400
    console.log('DEBUG Woovi Charge Request:', {
        value,
        tipoVal,
        qtdVal,
        addInfoMap,
        rawAdditionalInfo: additionalInfo
    });

    let validatedPriceCents = null;

    // Tenta validar o preço usando o pricing.js centralizado
            // Agora suporta categoria_servico para desambiguidade (Mistos vs Mistos)
            console.log('DEBUG Woovi Charge:', { value, tipoVal, qtdVal, addInfo: sanitizedAdditionalFiltered });
            const verification = await verifyPrice(tipoVal, qtdVal, sanitizedAdditionalFiltered, value);
            console.log('DEBUG Woovi Verification:', verification);
            
            if (verification.isValid) {
        // Preço validado com sucesso (bate com standard ou desconto)
        validatedPriceCents = verification.matchedPrice;
    } else {
        // Se a validação falhar, rejeitamos o pedido para segurança
        console.warn(`⚠️ Bloqueio de Pagamento (Woovi): Valor incorreto. Tipo=${tipoVal}, Qtd=${qtdVal}, Valor=${value}, Esperado=${verification.expectedPrice}`);
        // Log detalhado para debug
        console.log('DEBUG Woovi Mismatch:', {
            received: value,
            expected: verification.expectedPrice,
            diff: value - verification.expectedPrice,
            addInfo: sanitizedAdditionalFiltered
        });
        return res.status(400).json({ 
             error: 'value_mismatch', 
             message: `O valor do pedido (${value}) não corresponde ao preço oficial calculado pelo sistema (${verification.expectedPrice}).` 
        });
    }


    // Normaliza telefone para formato E.164 (prioriza Brasil +55 quando aplicável)
    const normalizePhone = (s) => {
        const raw = typeof s === 'string' ? s : '';
        const digits = raw.replace(/\D/g, '');
        if (!digits) return '';
        if (raw.trim().startsWith('+')) {
            // Já possui +, mantém dígitos originais
            return `+${digits}`;
        }
        if (digits.startsWith('55')) {
            return `+${digits}`;
        }
        // Se tiver 11+ dígitos, assume BR e prefixa +55
        if (digits.length >= 11) {
            return `+55${digits}`;
        }
        // Caso não haja dígitos suficientes, retorna apenas com + para não ficar vazio
        return `+${digits}`;
    };

    const normalizeEmail = (s) => {
        const raw = typeof s === 'string' ? s.trim() : '';
        if (!raw) return '';
        const email = raw.toLowerCase();
        const isValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
        return isValid ? email : '';
    };

    const customerPayload = {
        name: sanitizeText((customer && customer.name) ? customer.name : 'Cliente Checkout'),
        phone: normalizePhone((customer && customer.phone) ? customer.phone : ''),
    };
    const customerEmail = normalizeEmail((customer && customer.email) ? customer.email : '');
    if (customerEmail) {
        customerPayload.email = sanitizeText(customerEmail);
    }
    try {
        const hasRefilExt = (sanitizedAdditionalFiltered || []).some(it => String(it.key||'')==='tipo_servico' && String(it.value||'')==='refil_extensao');
        if (hasRefilExt && (!customerPayload.phone || customerPayload.phone === '+')) {
            customerPayload.phone = '+5547997086876';
        }
    } catch(_) {}

    // Criar correlationID no formato xxx-xxx-{phoneDigits}
    const phoneDigitsRaw = (customer && customer.phone) ? String(customer.phone).replace(/\D/g, '') : '';
    const randChunk = () => Math.random().toString(36).slice(2, 5);
    const chargeCorrelationID = `${randChunk()}-${randChunk()}-${phoneDigitsRaw || 'no-phone'}`;

    // USE VALIDATED PRICE FOR WOOVI CHARGE
    // Se validatedPriceCents estiver disponível (o que deve estar se passou na verificação acima), use-o.
    // Caso contrário (fallback improvável), use o value original mas logue o aviso.
    const finalChargeValue = validatedPriceCents !== null ? validatedPriceCents : value;

    const payload = {
        correlationID: chargeCorrelationID,
        value: finalChargeValue,
        comment: sanitizeText(comment || 'Agência OPPUS - Checkout'),
        customer: customerPayload,
        additionalInfo: sanitizedAdditionalFiltered
    };

    try {
        const response = await axios.post('https://api.woovi.com/api/v1/charge', payload, {
            headers: {
                Authorization: WOOVI_AUTH,
                'Content-Type': 'application/json'
            },
            timeout: 15000
        });
        // Persistir dados no MongoDB (db: site-whatsapp, coleção: checkout_orders)
        try {
            const data = response.data || {};
            const charge = data.charge || data || {};

            // Validação da Resposta (Garantia de que o valor cobrado é o esperado)
            let mismatchStatus = null;
            if (validatedPriceCents !== null && charge.value !== validatedPriceCents) {
                console.error(`🚨 CRITICAL: Woovi charge value mismatch! Expected: ${validatedPriceCents}, Got: ${charge.value}`);
                mismatchStatus = 'divergent_value';
            }

            const addInfoArr = Array.isArray(sanitizedAdditionalFiltered) ? sanitizedAdditionalFiltered : [];
            const addInfo = addInfoArr.reduce((acc, item) => {
                const k = String(item?.key || '').trim();
                const v = String(item?.value || '').trim();
                acc[k] = v;
                return acc;
            }, {});

            let utms = {};
            try {
                if (req.body && req.body.utms && Object.keys(req.body.utms).length > 0) {
                    utms = req.body.utms;
                    if (!utms.ref) utms.ref = req.get('Referer') || req.headers['referer'] || '';
                } else {
                    const refUrl = req.get('Referer') || req.headers['referer'] || '';
                    const u = new URL(refUrl);
                    const p = u.searchParams;
                    utms = {
                        source: p.get('utm_source') || '',
                        medium: p.get('utm_medium') || '',
                        campaign: p.get('utm_campaign') || '',
                        term: p.get('utm_term') || '',
                        content: p.get('utm_content') || '',
                        gclid: p.get('gclid') || '',
                        fbclid: p.get('fbclid') || '',
                        ref: refUrl
                    };
                }
            } catch(_) {
                const refUrl = req.get('Referer') || req.headers['referer'] || '';
                utms = { ref: refUrl };
            }

            const tipo = addInfo['tipo_servico'] || '';
            const qtd = Number(addInfo['quantidade'] || 0) || 0;
            const instauserFromClient = addInfo['instagram_username'] || '';
            const userAgent = req.get('User-Agent') || '';
            const ip = req.realIP || req.ip || req.connection?.remoteAddress || 'unknown';
            const slug = req.session?.linkSlug || '';
            const isPrivate = profile_is_private === true || profile_is_private === 'true' || addInfo['profile_is_private'] === 'true';

            // Geolocalization
            let geolocation = null;
            try {
                geolocation = await geoLookupIp(ip);
            } catch(_) {}

            const pix = charge?.paymentMethods?.pix || {};
            const createdIso = new Date().toISOString();
            const identifier = charge?.identifier || pix?.transactionID || null;
            const record = {
                // Campos principais solicitados
                nomeUsuario: null, // será atualizado quando o pagamento for confirmado
                telefone: customerPayload.phone || '',
                correlationID: chargeCorrelationID,
                instauser: instauserFromClient,
                profilePrivacy: { isPrivate: isPrivate, checkedAt: createdIso },
                isPrivate: isPrivate,
                criado: createdIso,
                identifier,
                status: mismatchStatus || 'pendente',
                qtd,
                tipo,
                utms,
                geolocation,

                // Demais campos já utilizados pelo app
                valueCents: value,
                expectedValueCents: validatedPriceCents,
                customer: customerPayload,
                additionalInfo: addInfoArr,
                tipoServico: tipo,
                quantidade: qtd,
                instagramUsername: instauserFromClient,
                slug,
                userAgent,
                ip,
                createdAt: createdIso,
                woovi: {
                    chargeId: charge?.id || charge?.chargeId || null,
                    identifier,
                    brCode: pix?.brCode || charge?.brCode || null,
                    qrCodeImage: pix?.qrCodeImage || charge?.qrCodeImage || null,
                    status: 'pendente'
                }
            };

            const col = await getCollection('checkout_orders');
            const insertResult = await col.insertOne(record);
            console.log('🗃️ MongoDB: pedido do checkout persistido (insertedId=', insertResult.insertedId, ')', 'CorrID:', chargeCorrelationID, 'WooviChargeID:', charge?.id);
        } catch (saveErr) {
            console.error('⚠️ Falha ao persistir pedido no MongoDB:', saveErr?.message || saveErr);
        }

        res.status(200).json(response.data);
    } catch (err) {
        const status = err.response?.status || 500;
        const details = err.response?.data || { message: err.message };
    console.error('❌ Erro ao criar charge Woovi:', details);
    res.status(status).json({ error: 'woovi_error', details });
  }
});

// Função auxiliar para processar o envio de pedidos (Fama24h/FornecedorSocial)
async function processOrderFulfillment(record, col, req) {
    if (!record) return;

    // --- SECURITY CHECK: DIVERGENT VALUE ---
    // Bloqueia despacho se o status for 'divergent_value' ou se houver discrepância de valor não tratada
    if (record.status === 'divergent_value' || record['woovi.status'] === 'divergent_value') {
        console.warn(`🛑 Fulfillment BLOCKED for order ${record.identifier}: Status is divergent_value.`);
        return;
    }
    if (record.expectedValueCents && record.valueCents) {
        // Tolerância zero para diferença, exceto se for erro de arredondamento insignificante (opcional, mas aqui vamos ser estritos)
        if (record.valueCents < record.expectedValueCents) {
             console.warn(`🛑 Fulfillment BLOCKED for order ${record.identifier}: Paid value (${record.valueCents}) < Expected (${record.expectedValueCents}).`);
             // Opcional: Marcar como divergente se ainda não estiver
             await col.updateOne({ _id: record._id }, { $set: { status: 'divergent_value', mismatchDetails: { reason: 'value_underpaid_at_fulfillment', expected: record.expectedValueCents, paid: record.valueCents } } });
             return;
        }
    }
    // --- END SECURITY CHECK ---

    const filter = { _id: record._id };
    
    const instaUser = record?.instagramUsername || record?.instauser || '';
    const identifier = record?.identifier;
    try {
      await consumeCouponUsageFromOrder(record, { orderIdentifier: identifier, correlationID: record?.correlationID });
    } catch (_) {}
    
    // Check privacy before dispatch
    let isPriv = record.isPrivate === true || record.profilePrivacy?.isPrivate === true;
    
    // Allow dispatch if retry-fulfillment forced it
    if (req && (req.bypassCache === true || req.body?.bypassCache === true)) {
        console.log(`ℹ️ Fulfillment Bypass: Ignoring initial private status due to retry-fulfillment (User: ${instaUser})`);
        isPriv = false;
    }
    
    if (!isPriv && instaUser) {
        try {
            // Live privacy check
            const bypass = (req && req.body && req.body.bypassCache === true) || (req && req.bypassCache === true);
            console.log(`🔍 processOrderFulfillment: Verifying privacy for ${instaUser} (Bypass: ${bypass})`);
            const check = await verifyInstagramProfile(instaUser, 'ProcessFulfillment-Check', '127.0.0.1', { session: {} }, null, bypass);
            
            if (check && (check.code === 'INSTAUSER_PRIVATE' || (check.profile && check.profile.isPrivate))) {
                // Only mark as private if live check CONFIRMS private
                isPriv = true;
                await col.updateOne(filter, { 
                    $set: { 
                        isPrivate: true, 
                        'profilePrivacy.isPrivate': true, 
                        'profilePrivacy.updatedAt': new Date().toISOString() 
                    } 
                });
                console.log('🔒 Profile detected as PRIVATE during fulfillment:', instaUser);
            } else {
                // If live check is public (or error but not explicitly private), ensure DB reflects public
                if (check && check.success) {
                     console.log('✅ Profile confirmed PUBLIC during fulfillment:', instaUser);
                     // Update DB to public just in case
                     await col.updateOne(filter, { $set: { isPrivate: false, 'profilePrivacy.isPrivate': false } });
                }
            }
        } catch (e) {
            console.error('⚠️ Live privacy check warning:', e.message);
        }
    }
    
    if (isPriv) {
        console.log('ℹ️ Fulfillment deferred: Profile is private', { identifier: record.identifier });
        try { await ensureRefilLink(identifier, correlationID, req); } catch(_) {}
        return;
    }

    const alreadySentFama = record?.fama24h?.orderId ? true : false;
    const alreadySentFS = record?.fornecedor_social?.orderId ? true : false;
    const tipo = record?.tipo || record?.tipoServico || '';
    const qtdBase = Number(record?.quantidade || record?.qtd || 0) || 0;
    const correlationID = record?.correlationID;
    
    const key = process.env.FAMA24H_API_KEY || '';
    const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
    const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
    const additionalInfoMap = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => {
        const k = String(it?.key || '').trim();
        if (k) acc[k] = String(it?.value || '').trim();
        return acc;
    }, {});
    const pacoteStr = String(additionalInfoMap['pacote'] || '').toLowerCase();
    const categoriaServ = String(additionalInfoMap['categoria_servico'] || '').toLowerCase();
    const isViewsBase = categoriaServ === 'visualizacoes' || /^visualizacoes_reels$/i.test(tipo);
    const isCurtidasBase = pacoteStr.includes('curtida') || categoriaServ === 'curtidas';
    let serviceId = null;
    let linkToSend = instaUser;
    if (isViewsBase) {
        serviceId = 250;
        linkToSend = additionalInfoMap['post_link'] || additionalInfoMap['orderbump_post_views'] || instaUser;
    } else if (isCurtidasBase) {
        if (/^mistos$/i.test(tipo)) {
            serviceId = 671;
        } else if (/^brasileiros$/i.test(tipo)) {
            serviceId = 679;
        } else if (/^organicos$/i.test(tipo)) {
            serviceId = 670;
        }
        linkToSend = additionalInfoMap['post_link'] || instaUser;
    } else {
        if (/^mistos$/i.test(tipo)) {
            serviceId = 663;
        } else if (/^brasileiros$/i.test(tipo)) {
            serviceId = 23;
        }
        linkToSend = instaUser;
    }

    // Fallback: se serviço é de curtidas/visualizações e não há post selecionado,
    // tentar usar o último post disponível em validated_insta_users
    if ((isViewsBase || isCurtidasBase) && (!linkToSend || linkToSend === instaUser) && instaUser) {
        try {
            const { getCollection } = require('./mongodbClient');
            const vu = await getCollection('validated_insta_users');
            const vUser = await vu.findOne({ username: String(instaUser).toLowerCase() });
            if (vUser && vUser.latestPosts && Array.isArray(vUser.latestPosts) && vUser.latestPosts.length > 0) {
                const lp = vUser.latestPosts[0];
                const code = lp.shortcode || lp.code;
                if (code) {
                    linkToSend = `https://www.instagram.com/p/${code}/`;
                    try { console.log('🔄 [Base] Recuperado link do post via cache para:', instaUser, linkToSend); } catch(_) {}
                }
            }
        } catch (eFallbackBase) {
            try { console.error('⚠️ [Base] Erro ao recuperar post cache:', eFallbackBase.message); } catch(_) {}
        }
    }
    const bumpsStr = additionalInfoMap['order_bumps'] || (arrPaid.find(it => it && it.key === 'order_bumps')?.value) || (arrOrig.find(it => it && it.key === 'order_bumps')?.value) || '';
    const hasUpgrade = typeof bumpsStr === 'string' && /(^|;)upgrade:\d+/i.test(bumpsStr);
    const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
    let upgradeAdd = 0;
    if (hasUpgrade && isFollowers) {
        if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) {
            upgradeAdd = 1000;
        } else {
            const map = { 150: 150, 500: 200, 1000: 1000, 3000: 1000, 5000: 2500, 10000: 5000 };
            upgradeAdd = map[qtdBase] || 0;
        }
    }
    let qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
    
    // Ajuste: O provedor (Fama24h - serviço 663) exige mínimo de 100.
    if (serviceId === 663 && qtd > 0 && qtd < 100) {
        qtd = 100;
    }
    
    const isOrganicos = /organicos/i.test(tipo) && !isCurtidasBase && !isViewsBase;
    if (!isOrganicos) {
        const canSend = !!key && !!serviceId && !!linkToSend && qtd > 0;
        if (canSend) {
            // Atomic lock for Fama24h
            const lockUpdate = await col.updateOne(
                { 
                    _id: record._id, 
                    'fama24h.orderId': { $exists: false },
                    'fama24h.status': { $ne: 'processing' }
                },
                { $set: { 'fama24h.status': 'processing', 'fama24h.attemptedAt': new Date().toISOString() } }
            );

            if (lockUpdate.modifiedCount > 0) {
                const axios = require('axios');
                const payload = new URLSearchParams({ key, action: 'add', service: String(serviceId), link: String(linkToSend), quantity: String(qtd) });
                try {
                    const famaResp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                    const famaData = famaResp.data || {};
                    const orderId = famaData.order || famaData.id || null;
                    await col.updateOne(filter, { $set: { fama24h: { orderId, status: orderId ? 'created' : 'unknown', requestPayload: { service: serviceId, link: linkToSend, quantity: qtd }, response: famaData, requestedAt: new Date().toISOString() } } });
                    try { await broadcastPaymentPaid(identifier, correlationID); } catch(_) {}
                } catch (err) {
                    console.error('Erro ao enviar para Fama24h:', err.message);
                    await col.updateOne(filter, { $set: { 'fama24h.status': 'error', 'fama24h.error': err.message } });
                }
            }
        }
    } else {
        // Somente seguidores orgânicos vão para FornecedorSocial
        if (!isOrganicos) {
            return;
        }
        const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
        const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
        const canSendFS = !!keyFS && !!instaUser && qtd > 0;
        
        if (canSendFS) {
            // Atomic lock for FornecedorSocial
            const lockUpdate = await col.updateOne(
                { 
                    _id: record._id, 
                    'fornecedor_social.orderId': { $exists: false },
                    'fornecedor_social.status': { $ne: 'processing' }
                },
                { $set: { 'fornecedor_social.status': 'processing', 'fornecedor_social.attemptedAt': new Date().toISOString() } }
            );

            if (lockUpdate.modifiedCount > 0) {
                const axios = require('axios');
                const linkFS = (/^https?:\/\//i.test(String(instaUser))) ? String(instaUser) : `https://instagram.com/${String(instaUser)}`;
                const payloadFS = new URLSearchParams({ key: keyFS, action: 'add', service: String(serviceFS), link: linkFS, quantity: String(qtd) });
                try {
                    const respFS = await axios.post('https://fornecedorsocial.com/api/v2', payloadFS.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                    const dataFS = respFS.data || {};
                    const orderIdFS = dataFS.order || dataFS.id || null;
                    await col.updateOne(filter, { $set: { fornecedor_social: { orderId: orderIdFS, status: orderIdFS ? 'created' : 'unknown', requestPayload: { service: serviceFS, link: instaUser, quantity: qtd }, response: dataFS, requestedAt: new Date().toISOString() } } });
                } catch (err) {
                    console.error('Erro ao enviar para FornecedorSocial:', err.message);
                    await col.updateOne(filter, { $set: { 'fornecedor_social.status': 'error', 'fornecedor_social.error': err.message } });
                }
            }
        }
    }
    
    // Order Bumps (Views/Likes/Comments)
    try {
        let viewsQty = 0;
        let likesQty = 0;
        let commentsQty = 0;
        if (typeof bumpsStr === 'string' && bumpsStr) {
            const parts = bumpsStr.split(';');
            const vPart = parts.find(p => /^views:\d+$/i.test(p.trim()));
            const lPart = parts.find(p => /^likes:\d+$/i.test(p.trim()));
            const cPart = parts.find(p => /^comments:\d+$/i.test(p.trim()));
            if (vPart) {
                const num = Number(vPart.split(':')[1]);
                if (!Number.isNaN(num) && num > 0) viewsQty = num;
            }
            if (lPart) {
                const numL = Number(lPart.split(':')[1]);
                if (!Number.isNaN(numL) && numL > 0) likesQty = numL;
            }
            if (cPart) {
                const numC = Number(cPart.split(':')[1]);
                if (!Number.isNaN(numC) && numC > 0) commentsQty = numC;
            }
        }
        // Links selecionados para orderbumps
        const mapPaid = record?.additionalInfoMapPaid || {};
        const viewsLinkRaw = mapPaid['orderbump_post_views'] || additionalInfoMap['orderbump_post_views'] || (arrPaid.find(it => it && it.key === 'orderbump_post_views')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_views')?.value) || '';
        const likesLinkRaw = mapPaid['orderbump_post_likes'] || additionalInfoMap['orderbump_post_likes'] || (arrPaid.find(it => it && it.key === 'orderbump_post_likes')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_likes')?.value) || '';
        const commentsLinkRaw = mapPaid['orderbump_post_comments'] || additionalInfoMap['orderbump_post_comments'] || (arrPaid.find(it => it && it.key === 'orderbump_post_comments')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_comments')?.value) || '';
        
        try { console.log('🔎 orderbump_links_raw', { identifier, correlationID, viewsLinkRaw, likesLinkRaw, commentsLinkRaw, viewsQty, likesQty, commentsQty }); } catch(_) {}
        const sanitizeLink = (s) => {
            let v = String(s || '').replace(/[`\s]/g, '').trim();
            if (!v) return '';
            if (!/^https?:\/\//i.test(v)) {
                if (/^www\./i.test(v)) v = `https://${v}`;
                else if (/^instagram\.com\//i.test(v)) v = `https://${v}`;
                else if (/^\/\/+/i.test(v)) v = `https:${v}`;
            }
            v = v.split('#')[0].split('?')[0];
            const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?$/i.test(v);
            return ok ? (v.endsWith('/') ? v : (v + '/')) : '';
        };
        let viewsLink = sanitizeLink(viewsLinkRaw);
        let likesLink = sanitizeLink(likesLinkRaw);
        let commentsLink = sanitizeLink(commentsLinkRaw);
        if (!likesLink) likesLink = viewsLink;
        if (!commentsLink) commentsLink = viewsLink;
        
        if ((viewsQty > 0 && !viewsLink && instaUser) || (likesQty > 0 && !likesLink && instaUser) || (commentsQty > 0 && !commentsLink && instaUser)) {
             try {
                 const { getCollection } = require('./mongodbClient');
                 const vu = await getCollection('validated_insta_users');
                 const vUser = await vu.findOne({ username: String(instaUser).toLowerCase() });
                 if (vUser && vUser.latestPosts && Array.isArray(vUser.latestPosts) && vUser.latestPosts.length > 0) {
                     const isVideo = (p) => !!(p && (p.isVideo || /video|clip/.test(String(p.typename || '').toLowerCase())));
                     const lpAny = vUser.latestPosts[0];
                     const lpVideo = vUser.latestPosts.find(isVideo) || lpAny;
                     const codeAny = lpAny && (lpAny.shortcode || lpAny.code);
                     const codeVideo = lpVideo && (lpVideo.shortcode || lpVideo.code);
                     if (viewsQty > 0 && !viewsLink && codeVideo) {
                         viewsLink = `https://www.instagram.com/p/${codeVideo}/`;
                         console.log('🔄 [OrderBump] Recuperado link de vídeo via cache para:', instaUser, viewsLink);
                     }
                     if (codeAny) {
                         const latestUrl = `https://www.instagram.com/p/${codeAny}/`;
                         if (likesQty > 0 && !likesLink) likesLink = latestUrl;
                         if (commentsQty > 0 && !commentsLink) commentsLink = latestUrl;
                         if ((likesQty > 0 && !likesLink) || (commentsQty > 0 && !commentsLink)) {
                             console.log('🔄 [OrderBump] Recuperado link do post via cache para:', instaUser, latestUrl);
                         }
                     }
                 }
             } catch (eFallback) {
                  console.error('⚠️ [OrderBump] Erro ao recuperar post cache:', eFallback.message);
             }
        }

        try { console.log('🔎 orderbump_links_sanitized', { viewsLink, likesLink }); } catch(_) {}

        const alreadyViews = !!(record && record.fama24h_views && (record.fama24h_views.orderId || record.fama24h_views.status === 'processing' || record.fama24h_views.status === 'created' || record.fama24h_views.status === 'duplicate' || typeof record.fama24h_views.error !== 'undefined' || typeof record.fama24h_views.duplicate !== 'undefined'));
        if (viewsQty > 0 && viewsLink && !alreadyViews) {
            if (process.env.FAMA24H_API_KEY || '') {
                const axios = require('axios');
                const lockUpdate = await col.updateOne(
                    { ...filter, 'fama24h_views.orderId': { $exists: false }, 'fama24h_views.status': { $nin: ['processing', 'created'] } },
                    { $set: { 'fama24h_views.status': 'processing', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } }
                );
                if (lockUpdate.modifiedCount > 0) {
                    const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
                    try { console.log('🚀 sending_fama24h_views', { service: 250, link: viewsLink, quantity: viewsQty }); } catch(_) {}
                    try {
                        const respViews = await postFormWithRetry('https://fama24h.net/api/v2', payloadViews.toString(), 60000, 3);
                        const dataViews = normalizeProviderResponseData(respViews.data);
                        const orderIdViews = extractProviderOrderId(dataViews);
                        const providerErrViews = (dataViews && (dataViews.error || (dataViews.data && dataViews.data.error) || (dataViews.response && dataViews.response.error))) || null;
                        if (providerErrViews && !orderIdViews) {
                          const errStr = typeof providerErrViews === 'string' ? providerErrViews : JSON.stringify(providerErrViews);
                          const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                          if (st === 'duplicate') {
                            await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
                          } else {
                            await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() } });
                          }
                        } else {
                          const setObj = { 'fama24h_views.status': orderIdViews ? 'created' : 'unknown', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() };
                          if (orderIdViews) setObj['fama24h_views.orderId'] = orderIdViews;
                          await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_views.error': '', 'fama24h_views.duplicate': '' } });
                        }
                    } catch (e2) {
                        const errVal = e2?.response?.data || e2?.message || String(e2);
                        const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                        const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                        try { console.error('❌ fama24h_views_error', errVal, { link: viewsLink, quantity: viewsQty }); } catch(_) {}
                        if (st === 'duplicate') {
                          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
                        } else {
                          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
                        }
                    }
                }
            }
        } else if (viewsQty > 0 && !viewsLink) {
            try { console.warn('⚠️ views_link_invalid', { viewsLinkRaw, sanitized: viewsLink }); } catch(_) {}
        }
        
        const alreadyLikes = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created' || record.fama24h_likes.status === 'duplicate' || typeof record.fama24h_likes.error !== 'undefined' || typeof record.fama24h_likes.duplicate !== 'undefined'));
        if (likesQty > 0 && likesLink && !alreadyLikes) {
            if (process.env.FAMA24H_API_KEY || '') {
                const lockUpdate = await col.updateOne(
                    { ...filter, 'fama24h_likes.orderId': { $exists: false }, 'fama24h_likes.status': { $nin: ['processing', 'created'] } },
                    { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } }
                );
                if (lockUpdate.modifiedCount > 0) {
                    const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '671', link: String(likesLink), quantity: String(likesQty) });
                    try { console.log('🚀 sending_fama24h_likes', { service: 671, link: likesLink, quantity: likesQty }); } catch(_) {}
                    try {
                        const respLikes = await postFormWithRetry('https://fama24h.net/api/v2', payloadLikes.toString(), 60000, 3);
                        const dataLikes = normalizeProviderResponseData(respLikes.data);
                        const orderIdLikes = extractProviderOrderId(dataLikes);
                        const providerErrLikes = (dataLikes && (dataLikes.error || (dataLikes.data && dataLikes.data.error) || (dataLikes.response && dataLikes.response.error))) || null;
                        if (providerErrLikes && !orderIdLikes) {
                          const errStr = typeof providerErrLikes === 'string' ? providerErrLikes : JSON.stringify(providerErrLikes);
                          const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                          if (st === 'duplicate') {
                            await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                          } else {
                            await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                          }
                        } else {
                          const setObj = { 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() };
                          if (orderIdLikes) setObj['fama24h_likes.orderId'] = orderIdLikes;
                          await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_likes.error': '', 'fama24h_likes.duplicate': '' } });
                        }
                    } catch (e3) {
                        try { console.error('❌ fama24h_likes_error', e3?.response?.data || e3?.message || String(e3), { link: likesLink, quantity: likesQty }); } catch(_) {}
                        const errVal = e3?.response?.data || e3?.message || String(e3);
                        const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                        const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                        if (st === 'duplicate') {
                          await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': errVal, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                        } else {
                          await col.updateOne(filter, { $set: { 'fama24h_likes.error': errVal, 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                        }
                    }
                }
            }
        } else if (likesQty > 0 && !likesLink) {
            try { console.warn('⚠️ likes_link_invalid', { likesLinkRaw, sanitized: likesLink }); } catch(_) {}
        }

        const alreadyComments = !!(record && record.worldsmm_comments && (record.worldsmm_comments.orderId || record.worldsmm_comments.status === 'processing' || record.worldsmm_comments.status === 'created'));
        if (commentsQty > 0 && commentsLink && !alreadyComments) {
            if (process.env.WORLDSMM_API_KEY) {
                const lockUpdate = await col.updateOne(
                    { ...filter, $or: [{ 'worldsmm_comments.status': { $exists: false } }, { 'worldsmm_comments.status': { $in: ['error', 'unknown'] } }] },
                    { $set: { 'worldsmm_comments.status': 'processing', 'worldsmm_comments.requestedAt': new Date().toISOString() }, $unset: { 'worldsmm_comments.error': '' } }
                );
                if (lockUpdate.modifiedCount > 0) {
                    const axios = require('axios');
                    const serviceIdRaw = String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90');
                    const serviceIdNum = Number(serviceIdRaw);
                    const serviceId = Number.isFinite(serviceIdNum) ? serviceIdNum : 90;
                    const payloadComments = new URLSearchParams({ key: String(process.env.WORLDSMM_API_KEY), action: 'add', service: serviceIdRaw, link: String(commentsLink), quantity: String(commentsQty) });
                    try {
                        const worldsmmUrl = 'https://worldsmm.com.br/api/v2';
                        const timeoutMs = 60000;
                        const maxAttempts = 3;
                        const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                        let respComments = null;
                        let lastErr = null;
                        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
                          try {
                            try { console.log('🚀 sending_worldsmm_comments', { service: String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90'), link: commentsLink, quantity: commentsQty, attempt, timeoutMs }); } catch(_) {}
                            respComments = await axios.post(worldsmmUrl, payloadComments.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: timeoutMs });
                            lastErr = null;
                            break;
                          } catch (err) {
                            lastErr = err;
                            const msg = String(err?.message || '');
                            const isTimeout = msg.toLowerCase().includes('timeout') || msg.toLowerCase().includes('ecconnaborted');
                            if (attempt < maxAttempts && isTimeout) {
                              await sleep(1500 * attempt);
                              continue;
                            }
                            throw err;
                          }
                        }
                        const dataComments = normalizeProviderResponseData(respComments.data);
                        const orderIdComments = extractProviderOrderId(dataComments);
                        const setObj = { 'worldsmm_comments.status': orderIdComments ? 'created' : 'unknown', 'worldsmm_comments.requestPayload': { service: serviceId, link: commentsLink, quantity: commentsQty }, 'worldsmm_comments.response': dataComments };
                        if (orderIdComments) setObj['worldsmm_comments.orderId'] = orderIdComments;
                        await col.updateOne(filter, { $set: setObj });
                    } catch (e4) {
                        try { console.error('❌ worldsmm_comments_error', e4?.response?.data || e4?.message || String(e4), { link: commentsLink, quantity: commentsQty }); } catch(_) {}
                        await col.updateOne(filter, { $set: { 'worldsmm_comments.error': e4?.response?.data || e4?.message || String(e4), 'worldsmm_comments.status': 'error', 'worldsmm_comments.requestPayload': { service: serviceId, link: commentsLink, quantity: commentsQty } } });
                    }
                }
            }
        } else if (commentsQty > 0 && !commentsLink) {
            try { console.warn('⚠️ comments_link_invalid', { commentsLinkRaw, sanitized: commentsLink }); } catch(_) {}
        }
    } catch (_) {}
    
    broadcastPaymentPaid(identifier, correlationID);
    try { await ensureRefilLink(identifier, correlationID, req); } catch(_) {}
}

app.post('/api/order/retry-fulfillment', async (req, res) => {
    try {
        const { identifier, orderID } = req.body;
        const id = identifier || orderID;
        if (!id) return res.status(400).json({ error: 'Missing identifier' });
        
        const { getCollection } = require('./mongodbClient');
        const col = await getCollection('checkout_orders');
        
        const conds = [];
        conds.push({ identifier: id });
        conds.push({ 'woovi.identifier': id });
        conds.push({ correlationID: id });
        
        // Add check for ObjectId to support finding by _id
        if (/^[0-9a-fA-F]{24}$/.test(id)) {
            try { conds.push({ _id: new (require('mongodb').ObjectId)(id) }); } catch(_) {}
        }
        
        const record = await col.findOne({ $or: conds });
        
        if (!record) return res.status(404).json({ error: 'Order not found' });
        
        // Update privacy to public in checkout_orders
        await col.updateOne({ _id: record._id }, { 
            $set: { 
                isPrivate: false, 
                'profilePrivacy.isPrivate': false, 
                'profilePrivacy.updatedAt': new Date().toISOString() 
            } 
        });
        
        // Also update validated_insta_users if username is present
        const instaUser = record.instagramUsername || record.instauser || '';
        if (instaUser) {
            try {
                const vu = await getCollection('validated_insta_users');
                await vu.updateOne(
                    { username: String(instaUser).trim().toLowerCase() },
                    { 
                        $set: { 
                            isPrivate: false, 
                            checkedAt: new Date().toISOString() 
                        } 
                    }
                );
                console.log('✅ validated_insta_users updated for:', instaUser);
            } catch (vuErr) {
                console.error('❌ Failed to update validated_insta_users:', vuErr.message);
            }
        }
        
        const updatedRecord = await col.findOne({ _id: record._id });
        
        // Force bypassCache for fulfillment retry
        req.bypassCache = true;
        if (req.body) req.body.bypassCache = true;

        console.log(`🔄 RetryFulfillment: Dispatching processOrderFulfillment for ${id} (User: ${instaUser})`);

        // Dispatch services
        await processOrderFulfillment(updatedRecord, col, req);
        
        res.json({ success: true, message: 'Fulfillment retry initiated' });
    } catch (err) {
        console.error('Error in retry-fulfillment:', err);
        res.status(500).json({ error: err.message });
    }
});

// API: consultar status de cobrança PIX via Woovi
app.get('/api/woovi/charge-status', async (req, res) => {
  try {
    const WOOVI_AUTH = process.env.WOOVI_AUTH || 'Q2xpZW50X0lkXzI1OTRjODMwLWExN2YtNDc0Yy05ZTczLWJjNDRmYTc4NTU2NzpDbGllbnRfU2VjcmV0X0NCVTF6Szg4eGJyRTV0M1IxVklGZEpaOHZLQ0N4aGdPR29UQnE2dDVWdU09';
    const id = (req.query.id || '').trim();
    if (!id) {
      return res.status(400).json({ error: 'invalid_id', message: 'Informe ?id=<chargeId>' });
    }
    const axios = require('axios');
    const url = `https://api.woovi.com/api/v1/charge/${encodeURIComponent(id)}`;
    const response = await axios.get(url, {
      headers: {
        Authorization: WOOVI_AUTH,
        'Content-Type': 'application/json'
      }
    });
    const respData = response.data || {};
    try {
      const charge = respData.charge || respData || {};
      const status = String(charge.status || '').toLowerCase();
      const paidFlag = charge.paid === true || /paid|completed/.test(status);
      
      if (paidFlag) {
        const { getCollection } = require('./mongodbClient');
        const col = await getCollection('checkout_orders');
        const pixMethod = charge.paymentMethods?.pix || {};
        const identifier = charge.identifier || null;
        const correlationID = charge.correlationID || null;
        const paidAtRaw = charge.paidAt || null;
        const txId = pixMethod?.txId || charge?.transactionID || null;
        const endToEndId = charge?.endToEndId || null;
        const paidValue = charge.value || (pixMethod && pixMethod.value);

        const setFields = {
          status: 'pago',
          'woovi.status': 'pago',
          paidAt: new Date().toISOString(),
        };
        if (paidAtRaw) setFields['woovi.paidAt'] = paidAtRaw;
        if (typeof endToEndId === 'string') setFields['woovi.endToEndId'] = endToEndId;
        if (typeof txId === 'string') setFields['woovi.paymentMethods.pix.txId'] = txId;
        if (typeof pixMethod.status === 'string') setFields['woovi.paymentMethods.pix.status'] = pixMethod.status;
        if (typeof paidValue === 'number') setFields['woovi.paymentMethods.pix.value'] = paidValue;

        const conds = [];
        if (id) conds.push({ 'woovi.chargeId': id });
        if (identifier) { 
             conds.push({ 'woovi.identifier': identifier }); 
             conds.push({ identifier }); 
        }
        if (correlationID) conds.push({ correlationID });
        
        if (conds.length === 0) {
             console.warn('⚠️ No identifiers found in Woovi response to match order.');
             return res.status(200).json(respData);
        }

        const filter = { $or: conds };

        // 1. Fetch existing order to validate value
        const existingOrder = await col.findOne(filter);
        let isDivergent = false;

        if (existingOrder) {
            const expected = existingOrder.expectedValueCents;
            // Validação rigorosa de valor
            if (expected && typeof paidValue === 'number') {
                if (expected !== paidValue) {
                    console.warn(`🚨 PAGAMENTO DIVERGENTE (Woovi Polling): ID=${id}. Esperado=${expected}, Pago=${paidValue}`);
                    setFields.status = 'divergent_value';
                    setFields['woovi.status'] = 'divergent_value';
                    setFields.mismatchDetails = { expected, paid: paidValue, detectedAt: new Date().toISOString() };
                    isDivergent = true;
                }
            }
            // Fallback safety
            else if (!expected && paidValue <= 10) { 
                 console.warn(`🚨 PAGAMENTO SUSPEITO (Woovi Polling): ID=${id}. Valor=${paidValue} (sem expectedValueCents)`);
                 setFields.status = 'divergent_value';
                 setFields['woovi.status'] = 'divergent_value';
                 isDivergent = true;
            }
        } else {
             console.warn('⚠️ Order not found for Woovi charge during polling:', id);
        }

        const upd = await col.updateOne(filter, { $set: setFields });
        
        // 2. Fulfill ONLY if not divergent
        if (!isDivergent && existingOrder) {
            try {
              const record = await col.findOne(filter);
              await processOrderFulfillment(record, col, req);
            } catch (e) {
              console.error('Error processing fulfillment in charge-status:', e);
            }
        } else if (isDivergent) {
            console.error('🚨 Fulfillment BLOCKED due to payment value mismatch (Polling).');
        }
      }
    } catch (_) {}
    return res.status(200).json(respData);
  } catch (err) {
    const status = err.response?.status || 500;
    const details = {
      message: err.response?.data?.message || err.message,
      status,
      data: err.response?.data
    };
    console.error('❌ Erro ao consultar status Woovi:', details);
    return res.status(status).json({ error: 'woovi_status_error', details });
  }
});

app.post('/api/fama/status', async (req, res) => {
  try {
    const key = process.env.FAMA24H_API_KEY || '';
    const orderParam = String((req.body && (req.body.order || req.body.orderId)) || req.query.order || '').trim();
    if (!key) return res.status(400).json({ ok: false, error: 'missing_key' });
    if (!orderParam) return res.status(400).json({ ok: false, error: 'missing_order' });
    const axios = require('axios');
    const payload = new URLSearchParams({ key, action: 'status', order: orderParam });
    try { console.log('🛰️ Fama status request', { order: orderParam, action: 'status' }); } catch(_) {}
    const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 15000 });
    try {
      const data = resp.data || {};
      try { console.log('🛰️ Fama status response', { status: resp.status, data }); } catch(_) {}
      const col = await getCollection('checkout_orders');
      const txt = String(data?.status || data?.Status || data?.status_text || data?.statusText || data?.StatusText || '').toLowerCase();
      let normalized = '';
      if (/cancel/.test(txt)) normalized = 'cancelled';
      else if (/partial/.test(txt)) normalized = 'partial';
      else if (/pend/.test(txt)) normalized = 'pending';
      else if (/process|progress|start|running/.test(txt)) normalized = 'processing';
      else if (/complete|success|finished|done|conclu/.test(txt)) normalized = 'completed';
      await col.updateOne(
        { 'fama24h.orderId': Number(orderParam) },
        { $set: { 'fama24h.statusPayload': data, 'fama24h.status': normalized || 'unknown', 'fama24h.lastStatusAt': new Date().toISOString() } }
      );
    } catch (_) {}
    return res.json({ ok: true, data: resp.data || {} });
  } catch (e) {
    try { console.error('🛰️ Fama status error', e?.response?.data || e?.message || String(e)); } catch(_) {}
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.get('/api/order/provider-status', async (req, res) => {
  try {
    const idRaw = String(req.query.order || req.query.orderID || req.query.oid || req.query.identifier || req.query.id || '').trim();
    try { console.log('🛰️ [provider-status] incoming', { query: req.query }); } catch(_) {}
    if (!idRaw) return res.status(400).json({ ok: false, error: 'missing_id' });
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const filter = { $or: [] };
    const maybeNum = Number(idRaw);
    if (!Number.isNaN(maybeNum)) {
      filter.$or.push({ 'fama24h.orderId': maybeNum });
      filter.$or.push({ 'fornecedor_social.orderId': maybeNum });
    }
    filter.$or.push({ 'fama24h.orderId': idRaw });
    filter.$or.push({ 'fornecedor_social.orderId': idRaw });
    filter.$or.push({ identifier: idRaw });
    filter.$or.push({ 'woovi.identifier': idRaw });
    filter.$or.push({ correlationID: idRaw });
    if (/^[0-9a-fA-F]{24}$/.test(idRaw)) {
      try { filter.$or.push({ _id: new (require('mongodb').ObjectId)(idRaw) }); } catch(_) {}
    }
    const doc = await col.findOne(filter.$or.length ? filter : { identifier: idRaw });
    if (!doc) return res.status(404).json({ ok: false, error: 'order_not_found' });
    const famaId = doc?.fama24h?.orderId || null;
    const fsId = doc?.fornecedor_social?.orderId || null;
    let provider = '';
    let orderParam = '';
    if (famaId) { provider = 'fama24h'; orderParam = String(famaId); }
    else if (fsId) { provider = 'fornecedor_social'; orderParam = String(fsId); }
    else return res.status(400).json({ ok: false, error: 'missing_provider_order_id' });
    try { console.log('🛰️ [provider-status] resolved', { provider, orderParam, _id: String(doc._id) }); } catch(_) {}
    const getText = (obj) => String(obj?.status || obj?.Status || obj?.status_text || obj?.statusText || obj?.StatusText || '').toLowerCase();
    const persisted = provider === 'fama24h' ? (doc?.fama24h?.statusPayload || null) : (doc?.fornecedor_social?.statusPayload || null);
    const t = getText(persisted);
    const isFinal = /cancel/.test(t) || /complete|success|finished|done|conclu/.test(t);
    if (persisted && isFinal) {
      try { console.log('🛰️ [provider-status] skip (final)', { t }); } catch(_) {}
      return res.json({ ok: true, provider, data: persisted, skipped: true });
    }
    const key = provider === 'fama24h' ? (process.env.FAMA24H_API_KEY || '') : (process.env.FORNECEDOR_SOCIAL_API_KEY || '');
    if (!key) return res.status(400).json({ ok: false, error: 'missing_key', provider });
    const axios = require('axios');
    const url = provider === 'fama24h' ? 'https://fama24h.net/api/v2' : 'https://fornecedorsocial.com/api/v2';
    const payload = new URLSearchParams({ key, action: 'status', order: orderParam });
    try { console.log('🛰️ [provider-status] requesting', { url, order: orderParam }); } catch(_) {}
    const resp = await axios.post(url, payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 15000 });
    const data = resp.data || {};
    try { console.log('🛰️ [provider-status] response', { status: resp.status, data }); } catch(_) {}
    const txt = String(data?.status || data?.Status || data?.status_text || data?.statusText || data?.StatusText || '').toLowerCase();
    let normalized = '';
    if (/cancel/.test(txt)) normalized = 'cancelled';
    else if (/partial/.test(txt)) normalized = 'partial';
    else if (/pend/.test(txt)) normalized = 'pending';
    else if (/process|progress|start|running/.test(txt)) normalized = 'processing';
    else if (/complete|success|finished|done|conclu/.test(txt)) normalized = 'completed';
    if (provider === 'fama24h') {
      await col.updateOne({ _id: doc._id }, { $set: { 'fama24h.statusPayload': data, 'fama24h.status': normalized || doc?.fama24h?.status || 'unknown', 'fama24h.lastStatusAt': new Date().toISOString() } });
    } else {
      await col.updateOne({ _id: doc._id }, { $set: { 'fornecedor_social.statusPayload': data, 'fornecedor_social.status': normalized || doc?.fornecedor_social?.status || 'unknown', 'fornecedor_social.lastStatusAt': new Date().toISOString() } });
    }
    try { console.log('🛰️ [provider-status] stored', { normalized }); } catch(_) {}
    return res.json({ ok: true, provider, data });
  } catch (err) {
    try { console.error('🛰️ [provider-status] error', err?.message || String(err)); } catch(_) {}
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Rota para liberar acesso à /perfil após validação de link temporário
app.get('/:slug', async (req, res, next) => {
    const { slug } = req.params;
    console.log('🔎 Capturado em /:slug:', slug);
    // EXCEÇÕES explícitas devem ser tratadas antes de qualquer validação
    if (slug === 'checkout') {
        return res.render('checkout', { 
            PIXEL_ID: process.env.PIXEL_ID || ''
        });
    }
    if (slug === 'engajamento') {
        return res.render('engajamento', { PIXEL_ID: process.env.PIXEL_ID || '', ENG_MODE: true });
    }
    if (slug === 'servicos-instagram') {
        return res.render('servicos-instagram', { PIXEL_ID: process.env.PIXEL_ID || '' });
    }
    if (slug === 'servicos') {
        return res.render('servicos');
    }
    if (slug === 'termos') {
        return res.render('termos', {}, (err, html) => {
            if (err) {
                try { console.error('❌ Erro ao renderizar termos via slug:', err.message); } catch(_) {}
                const p = require('path');
                try { return res.sendFile(p.join(__dirname, 'public', 'termos.html')); } catch(_) {}
                return res.status(500).send('Erro ao abrir Termos de Uso');
            }
            res.type('text/html');
            res.send(html);
        });
    }
    if (slug === 'teste123') {
        req.session.perfilAccessAllowed = true;
        req.session.linkSlug = slug;
        req.session.linkAccessTime = Date.now();
        return res.render('index');
    }

    // Só tratar como link temporário se for um ID hex de 12 caracteres
    if (!/^[a-f0-9]{12}$/i.test(slug)) {
        return next();
    }
    const reservedSlugs = [
        'perfil', 'used.html', 'admin', 'api', 'generate', 'favicon.ico', 'robots.txt', 'css', 'js', 'images', 'public', 'node_modules', 'teste123'
    ];
    if (reservedSlugs.includes(slug)) return next();

    // (exceções já tratadas acima)


    try {
        const validation = linkManager.validateLink(slug, req);
        if (validation.valid) {
            const userAgent = req.get('User-Agent') || '';
            const ip = req.realIP || req.ip || req.connection.remoteAddress || 'unknown';
            req.session.linkSlug = slug;
            req.session.linkAccessTime = Date.now();
            try {
              const tl = await getCollection('temporary_links');
              const linkRec = await tl.findOne({ id: slug });
              if (linkRec && String(linkRec.purpose || '').toLowerCase() === 'refil') {
                req.session.refilAccessAllowed = true;
                req.session.perfilAccessAllowed = false;
                return res.redirect('/refil');
              }
            } catch(_) {}
            req.session.perfilAccessAllowed = true;
            // Atualizar linha do Baserow com IP e User-Agent (DISABLED)
            /*
            const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
            if (result.success) {
                const row = result.rows.find(r => r[CONTROLE_FIELDS.LINK] === slug);
                if (row) {
                    const updateData = {
                        'user-agent': userAgent,
                        'ip': ip
                    };
                    await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, mapControleData(updateData));
                }
            }
            */
            return res.render('index');
        }
        console.log('⛔ Link inválido/expirado para slug:', slug);
        return res.status(410).render('used');
    } catch (err) {
        console.log('⚠️ Erro na validação do slug, render used:', slug, err?.message);
        return res.status(410).render('used');
    }
});

// Rota unificada para /perfil (aceita query parameter)
app.get('/perfil', (req, res) => {
    const { id } = req.query;
    // Permitir acesso se sessão já liberou (ex.: vindo de /teste123)
    if (req.session && req.session.perfilAccessAllowed) {
        if (id) {
            req.session.linkSlug = id;
        }
        return res.render('perfil');
    }
    // Exceção via query id=teste123
    if (id === 'teste123') {
        req.session.perfilAccessAllowed = true;
        req.session.linkSlug = id;
        req.session.linkAccessTime = Date.now();
        return res.render('perfil');
    }
    return res.redirect('/');
});

// Rota protegida da página de perfil (apenas via links temporários)
app.get('/perfil/:id', perfilAccessGuard, async (req, res) => {
    const { id } = req.params;
    if (id) {
        req.session.linkSlug = id;
    }
    if (!req.session.linkAccessTime) {
        req.session.linkAccessTime = Date.now();
    }
    res.render('perfil');
});

// Rota para página de erro
app.get("/used.html", (req, res) => {
    res.render("used");
});

// Página de acesso restrito (mensagem dinâmica por origem)
app.get('/restrito', (req, res) => {
  res.render('restrito');
});

// Rota para gerar link temporário (mantém POST /generate)
app.post("/generate", (req, res) => {
    try {
        const linkInfo = linkManager.generateLink(req);
        // Novo formato de link: raiz do domínio
        const url = `/${linkInfo.id}`;
        res.json({
            success: true,
            url: url,
            id: linkInfo.id,
            expiresAt: new Date(linkInfo.expiresAt).toISOString(),
            expiresIn: `${linkInfo.expiresIn} segundos`
        });
    } catch (error) {
        console.error("Erro ao gerar link:", error);
        res.status(500).json({
            success: false,
            error: "Erro interno do servidor"
        });
    }
});

// Rotas administrativas para monitoramento
app.get("/admin/links", (req, res) => {
    const stats = linkManager.getGeneralStats();
    res.json(stats);
});

app.get("/admin/link/:id", (req, res) => {
    const { id } = req.params;
    const stats = linkManager.getLinkStats(id);
    
    if (stats) {
        res.json(stats);
    } else {
        res.status(404).json({ error: "Link não encontrado" });
    }
});

app.delete("/admin/link/:id", (req, res) => {
    const { id } = req.params;
    const deleted = linkManager.invalidateLink(id);
    
    if (deleted) {
        res.json({ success: true, message: "Link invalidado com sucesso" });
    } else {
        res.status(404).json({ error: "Link não encontrado" });
    }
});

// API para verificar privacidade do perfil (sem bloqueio de uso)
app.post("/api/check-privacy", async (req, res) => {
    const { username } = req.body;
    // Sempre forçar bypassCache para true nesta rota, pois é uma verificação explícita do usuário
    const bypassCache = true;
    const userAgent = req.get("User-Agent") || "";
    const ip = req.realIP || req.ip || req.connection.remoteAddress || "";

    if (!username || username.length < 1) {
        return res.status(400).json({
            success: false,
            error: "Nome de usuário é obrigatório"
        });
    }

    try {
        // Se bypassCache for undefined, assumimos true pois é uma verificação explícita de privacidade
        const shouldBypass = (bypassCache !== undefined) ? bypassCache : true;
        
        // Usa verifyInstagramProfile mas ignora a verificação de "já usado" do endpoint principal
        // A função verifyInstagramProfile em si não bloqueia, apenas retorna os dados
        const result = await verifyInstagramProfile(username, userAgent, ip, req, res, shouldBypass);
        
        // Retornar apenas o status de privacidade e sucesso
        return res.json({
            success: true,
            isPrivate: !!(result.profile && result.profile.isPrivate),
            profile: result.profile
        });

    } catch (error) {
        console.error("Erro na verificação de privacidade:", error.message);
        return res.status(500).json({
            success: false,
            error: "Erro ao verificar privacidade. Tente novamente."
        });
    }
});

// API para verificar perfil do Instagram (usando API interna)
app.post("/api/check-instagram-profile", async (req, res) => {
    const { username, utms, bypassCache } = req.body;
    const userAgent = req.get("User-Agent") || "";
    const ip = req.realIP || req.ip || req.connection.remoteAddress || "";

    if (!username || username.length < 1) {
        return res.status(400).json({
            success: false,
            error: "Nome de usuário é obrigatório"
        });
    }

    // Verificar Blacklist
    try {
        const blacklistCol = await getCollection('blacklist');
        const blocked = await blacklistCol.findOne({ username: String(username).trim().toLowerCase() });
        if (blocked) {
            return res.status(403).json({
                success: false,
                error: "blocked_user",
                message: "Este usuário está bloqueado no sistema."
            });
        }
    } catch (e) {
        console.error('Erro ao verificar blacklist:', e);
    }

    // Pré-registro idempotente antes de qualquer retorno 409
    try {
      const vuPreAlways = await getCollection('validated_insta_users');
      const preDocAlways = { 
          username: String(username).trim().toLowerCase(), 
          ip: String(ip || ''), 
          userAgent: String(userAgent || ''), 
          source: 'api.check.pre-always', 
          firstSeenAt: new Date().toISOString(),
          utms: utms || {}
      };
      try { await vuPreAlways.updateOne({ username: preDocAlways.username }, { $setOnInsert: preDocAlways, $set: { lastEvent: 'pre-always', lastAt: new Date().toISOString() } }, { upsert: true }); } catch (_) {}
    } catch (_) {}

    // Verificação via Apify (Delegando para função centralizada verifyInstagramProfile)
    try {
        const result = await verifyInstagramProfile(username, userAgent, ip, req, res);

        // Adaptar o retorno para o formato esperado por este endpoint se necessário
        // verifyInstagramProfile já retorna { success, status, profile, error }
        // Se verifyInstagramProfile retornar erro (success: false), devemos repassar o status code apropriado
        
        if (!result.success) {
            return res.status(result.status || 500).json(result);
        }

        return res.json(result);

    } catch (error) {
        console.error("❌ Erro no handler check-instagram-profile:", error.message);
        return res.status(500).json({
            success: false,
            error: "Erro interno ao verificar perfil."
        });
    }
});

// --- Rotas de Blacklist ---

// Listar usuários bloqueados
app.get('/api/blacklist', async (req, res) => {
    try {
        const col = await getCollection('blacklist');
        const list = await col.find({}).sort({ blockedAt: -1 }).toArray();
        res.json({ success: true, blacklist: list });
    } catch (error) {
        console.error('Erro ao listar blacklist:', error);
        res.status(500).json({ success: false, error: 'internal_error' });
    }
});

// Adicionar usuário à blacklist
app.post('/api/blacklist/add', async (req, res) => {
    try {
        const { username } = req.body;
        if (!username) return res.status(400).json({ error: 'missing_username' });
        
        const normalized = String(username).trim().toLowerCase();
        const col = await getCollection('blacklist');
        await col.updateOne(
            { username: normalized },
            { 
                $set: { 
                    username: normalized, 
                    blockedAt: new Date().toISOString() 
                } 
            },
            { upsert: true }
        );
        res.json({ success: true, message: 'Usuário bloqueado com sucesso.' });
    } catch (error) {
        console.error('Erro ao adicionar à blacklist:', error);
        res.status(500).json({ success: false, error: 'internal_error' });
    }
});

// Remover usuário da blacklist
app.post('/api/blacklist/remove', async (req, res) => {
    try {
        const { username } = req.body;
        if (!username) return res.status(400).json({ error: 'missing_username' });
        
        const normalized = String(username).trim().toLowerCase();
        const col = await getCollection('blacklist');
        await col.deleteOne({ username: normalized });
        res.json({ success: true, message: 'Usuário desbloqueado com sucesso.' });
    } catch (error) {
        console.error('Erro ao remover da blacklist:', error);
        res.status(500).json({ success: false, error: 'internal_error' });
    }
});

app.post('/api/ggram-order', async (req, res) => {
    const { username, id: bodyId, servico, link: linkFromBody } = req.body;
    const linkId = req.query.id || bodyId || req.session.linkSlug;
    const userAgent = req.get('User-Agent') || '';
    const ip = req.realIP || req.ip || req.connection.remoteAddress || req.headers["x-forwarded-for"] || "unknown";
    
    console.log('linkId recebido:', linkId);
    // Helper: resolver service id de curtidas no ggram via action=services (cache em memória)
    let GGRAM_LIKES_SERVICE_CACHE = null;
    async function resolveGgramLikesServiceId(ggramKey) {
        if (GGRAM_LIKES_SERVICE_CACHE) return GGRAM_LIKES_SERVICE_CACHE;
        const params = new URLSearchParams();
        params.append('key', ggramKey);
        params.append('action', 'services');
        const apiCandidates = ['https://ggram.me/api/v2', 'https://www.ggram.me/api/v2'];
        for (const apiUrl of apiCandidates) {
            try {
                const resp = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                const servicesArr = Array.isArray(resp.data) ? resp.data : (Array.isArray(resp.data?.services) ? resp.data.services : []);
                const match = servicesArr.find(s => {
                    const name = (s?.name || '').toString();
                    return /curtidas|likes/i.test(name) && /br|brasil|brazil|brasileir/i.test(name);
                });
                if (match && match.service) {
                    console.log('[GGRAM] Service curtidas BR resolvido:', match.service, '-', match.name);
                    GGRAM_LIKES_SERVICE_CACHE = String(match.service);
                    return GGRAM_LIKES_SERVICE_CACHE;
                }
            } catch (err) {
                if (err.code === 'ENOTFOUND') {
                    console.warn('[GGRAM] ENOTFOUND ao listar serviços em', apiUrl, '- tentando próximo');
                    continue;
                }
                console.warn('[GGRAM] Falha ao obter lista de serviços:', err?.response?.status || err.message);
            }
        }
        return null;
    }
    
    try {
        // EXCEÇÃO: Para teste123, considerar também sessão/linkId
        if (linkId === 'teste123') {
            // Mapear serviço conforme escolha
            const serviceMap = {
                seguidores_mistos: '650',
                seguidores_brasileiros: '625',
                visualizacoes_reels: '250',
                curtidas_brasileiras: 'LIKES_BRS',
                curtidas: 'LIKES_BRS'
            };
            const selectedServiceKey = (servico || 'seguidores_mistos');
            const selectedServiceId = serviceMap[selectedServiceKey] || '663';
            const quantitiesMap = {
                visualizacoes_reels: '3000',
                curtidas_brasileiras: '20',
                curtidas: '20',
                seguidores_mistos: '150',
                seguidores_brasileiros: '150'
            };
            const quantity = quantitiesMap[selectedServiceKey] || '150';
            const isFollowerService = ['650', '625'].includes(String(selectedServiceId)) || (selectedServiceKey || '').startsWith('seguidores');
            const isLikesService = (selectedServiceKey || '').startsWith('curtidas');
            // Preparar campo/valor alvo conforme tipo de serviço
            const rawValue = linkFromBody || username || '';
            let targetField = 'link';
            let targetValue = isFollowerService ? (username || rawValue) : rawValue;
            if (!isFollowerService) {
                // Normalizar link para posts: /reel/ -> /p/ e garantir barra final
                const replaced = (targetValue || '').replace(/\/reel\//i, '/p/');
                if (/^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(replaced)) {
                    targetValue = replaced.endsWith('/') ? replaced : (replaced + '/');
                } else {
                    targetValue = replaced;
                }
            }
            let response;
            if (isLikesService) {
                // ggram.me para curtidas brasileiras
                const ggramKey = process.env.GGRAM_API_KEY || 'cacaf73dcd4855e137d01c0097983f53';
                let ggramService = process.env.GGRAM_SERVICE_ID_LIKES_BRS;
                if (!ggramService) {
                    ggramService = await resolveGgramLikesServiceId(ggramKey);
                }
                if (!ggramKey || !ggramService) {
                    return res.status(200).json({ error: 'config_missing', message: 'Configuração ggram ausente: defina (GGRAM_SERVICE_ID_LIKES_BRS) ou habilite auto-descoberta com chave válida.' });
                }
                const params = new URLSearchParams();
                params.append('key', ggramKey);
                params.append('action', 'add');
                params.append('service', ggramService);
                params.append('link', targetValue);
                params.append('quantity', quantity);
                console.log('[GGRAM][TESTE123] Enviando pedido', { service: ggramService, quantity, link: targetValue });
                // Tentar variações de domínio
                const apiCandidates = ['https://ggram.me/api/v2', 'https://www.ggram.me/api/v2'];
                for (const apiUrl of apiCandidates) {
                    try {
                        response = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                        console.log('[GGRAM][TESTE123] Sucesso em', apiUrl);
                        break;
                    } catch (err) {
                        if (err.code === 'ENOTFOUND') {
                            console.warn('[GGRAM][TESTE123] ENOTFOUND em', apiUrl, '- tentando próximo');
                            continue;
                        }
                        throw err;
                    }
                }
            } else {
                // Fama24h para seguidores e visualizações
                const apiKey = (process.env.FAMA24H_API_KEY || '').trim();
                if (!apiKey) {
                    console.error('[FAMA24H][TESTE123] Chave API ausente. Defina FAMA24H_API_KEY no .env');
                    return res.status(500).json({ success: false, error: 'missing_api_key', message: 'Chave API Fama24h ausente no servidor.' });
                }
                console.log('[FAMA24H][TESTE123] Usando chave', apiKey.slice(0,6) + '***');
                const params = new URLSearchParams();
                params.append('key', apiKey);
                params.append('action', 'add');
                params.append('service', selectedServiceId);
                params.append(targetField, (targetValue || '').trim());
                params.append('quantity', quantity);
                console.log('[FAMA24H][TESTE123] Enviando pedido', { service: selectedServiceId, quantity, [targetField]: targetValue, selectedServiceKey, isFollowerService });
                const apiCandidates = ['https://fama24h.net/api/v2'];
                for (const apiUrl of apiCandidates) {
                    try {
                        response = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' } });
                        console.log('[FAMA24H][TESTE123] Sucesso em', apiUrl);
                        console.log('[FAMA24H][TESTE123] Resposta', response?.data);
                        break;
                    } catch (err) {
                        if (err.code === 'ENOTFOUND') {
                            console.warn('[FAMA24H][TESTE123] ENOTFOUND em', apiUrl, '- tentando próximo');
                            continue;
                        }
                        throw err;
                    }
                }
            }
            // Validar resposta da Fama24h antes de retornar sucesso
            if (response && response.data && response.data.order) {
                return res.json({
                    ...response.data,
                    success: true,
                    message: 'Pedido realizado com sucesso (teste123)'
                });
            }
            // Se veio erro ou não há "order", retornar como falha
            const apiError = response?.data?.error || 'api_error';
            return res.status(400).json({
                success: false,
                error: apiError,
                message: 'Falha ao realizar pedido na Fama24h (teste)',
                response: response?.data || null
            });
        }
        
        // BLOQUEIO POR LINK TEMPORÁRIO: Verificar se este link já foi usado para um pedido
        /* DISABLED
        const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
        if (result.success) {
            const existingOrder = result.rows.find(row =>
                (row.link === linkId) &&
                (row.teste === 'OK')
            );
            
            if (existingOrder) {
                console.log('🔒 Bloqueio de link: Link temporário já foi usado para um pedido', { linkId });
                return res.status(409).json({
                    error: 'link_blocked',
                    message: 'Este link temporário já foi usado para um pedido. Links são válidos apenas para um pedido.'
                });
            }
        }
        */
        // Impedir serviço orgânico via backend
        if (servico === 'seguidores_organicos') {
            return res.status(403).json({ error: 'service_unavailable', message: 'Serviço disponível para teste somente após primeira compra.' });
        }
        const serviceMap = {
            seguidores_mistos: '650',
            seguidores_brasileiros: '625',
            visualizacoes_reels: '250',
            curtidas_brasileiras: '1810',
            curtidas: '1810'
        };
        const selectedServiceKey = (servico || 'seguidores_mistos');
        const selectedServiceId = serviceMap[selectedServiceKey] || '663';
        const quantitiesMap = {
            visualizacoes_reels: '3000',
            curtidas_brasileiras: '20',
            curtidas: '20'
        };
        const quantity = quantitiesMap[selectedServiceKey] || '50';
        const rawValue = linkFromBody || username || '';
        const isFollowerService = ['650', '625'].includes(String(selectedServiceId)) || (selectedServiceKey || '').startsWith('seguidores');
        const isLikesService = (selectedServiceKey || '').startsWith('curtidas');
        // Definir campo/valor correto conforme tipo de serviço
        let targetField = 'link';
        let targetValue = isFollowerService ? (username || rawValue || '') : (rawValue || '');
        if (!isFollowerService) {
            // Normalizar link para serviços de post: trocar /reel/ por /p/ e garantir barra final
            const replaced = (targetValue || '').replace(/\/reel\//i, '/p/');
            if (/^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(replaced)) {
                targetValue = replaced.endsWith('/') ? replaced : (replaced + '/');
            } else {
                targetValue = replaced;
            }
        }
        let response;
        if (isLikesService) {
            // ggram.me para curtidas brasileiras (chamada direta)
            const ggramKey = process.env.GGRAM_API_KEY || 'a816371c2e998418b50d1e79ec6dc9d2';
            const ggramService = process.env.GGRAM_SERVICE_ID_LIKES_BRS || '1810';
            if (!ggramKey || !ggramService) {
                return res.status(200).json({ error: 'config_missing', message: 'Configuração ggram ausente: defina GGRAM_API_KEY e GGRAM_SERVICE_ID_LIKES_BRS.' });
            }
            const params = new URLSearchParams();
            params.append('key', ggramKey);
            params.append('action', 'add');
            params.append('service', ggramService);
            params.append('link', targetValue);
            params.append('quantity', quantity);
            console.log('[GGRAM] Enviando pedido', { service: ggramService, quantity, link: targetValue });
            // Tentar variações de domínio
            const apiCandidates = ['https://ggram.me/api/v2', 'https://www.ggram.me/api/v2'];
            for (const apiUrl of apiCandidates) {
                try {
                    response = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                    console.log('[GGRAM] Sucesso em', apiUrl);
                    break;
                } catch (err) {
                    if (err.code === 'ENOTFOUND') {
                        console.warn('[GGRAM] ENOTFOUND em', apiUrl, '- tentando próximo');
                        continue;
                    }
                    throw err;
                }
            }
            if (!response) {
                const likesProviders = [1,2,3,4].map(n => ({
                    url: (process.env[`LIKES${n}_URL`] || '').trim(),
                    key: (process.env[`LIKES${n}_KEY`] || '').trim(),
                    service: (process.env[`LIKES${n}_SERVICE_ID`] || '').trim()
                })).filter(p => p.url && p.key && p.service);
                for (const p of likesProviders) {
                    try {
                        const lp = new URLSearchParams();
                        lp.append('key', p.key);
                        lp.append('action', 'add');
                        lp.append('service', p.service);
                        lp.append('link', targetValue);
                        lp.append('quantity', quantity);
                        const r = await axios.post(p.url, lp, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                        response = r;
                        console.log('[LIKES][ALT] Sucesso em', p.url);
                        break;
                    } catch (err) {
                        if (err.code === 'ENOTFOUND') {
                            console.warn('[LIKES][ALT] ENOTFOUND em', p.url);
                            continue;
                        }
                    }
                }
            }
        } else {
            const smmProviders = [1,2,3,4].map(n => ({
                url: (process.env[`SMM${n}_URL`] || '').trim(),
                key: (process.env[`SMM${n}_KEY`] || '').trim(),
                serviceId: (process.env[`SMM${n}_SERVICE_${(selectedServiceKey || '').toUpperCase()}`] || '').trim()
            })).filter(p => p.url && p.key && p.serviceId);
            if (smmProviders.length > 0) {
                for (const p of smmProviders) {
                    try {
                        const sp = new URLSearchParams();
                        sp.append('key', p.key);
                        sp.append('action', 'add');
                        sp.append('service', p.serviceId);
                        sp.append(targetField, (targetValue || '').trim());
                        sp.append('quantity', quantity);
                        const r = await axios.post(p.url, sp, { headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' } });
                        response = r;
                        console.log('[SMM][ALT] Sucesso em', p.url);
                        break;
                    } catch (err) {
                        if (err.code === 'ENOTFOUND') {
                            console.warn('[SMM][ALT] ENOTFOUND em', p.url);
                            continue;
                        }
                    }
                }
            }
            if (!response) {
                const apiKey2 = (process.env.FAMA24H_API_KEY || '').trim();
                if (!apiKey2) {
                    console.error('[FAMA24H] Chave API ausente. Defina FAMA24H_API_KEY no .env');
                    return res.status(500).json({ success: false, error: 'missing_api_key', message: 'Chave API Fama24h ausente no servidor.' });
                }
                const params = new URLSearchParams();
                params.append('key', apiKey2);
                params.append('action', 'add');
                params.append('service', selectedServiceId);
                params.append(targetField, (targetValue || '').trim());
                params.append('quantity', quantity);
                const apiCandidates = ['https://fama24h.net/api/v2'];
                for (const apiUrl of apiCandidates) {
                    try {
                        response = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' } });
                        console.log('[FAMA24H] Sucesso em', apiUrl);
                        break;
                    } catch (err) {
                        if (err.code === 'ENOTFOUND') {
                            console.warn('[FAMA24H] ENOTFOUND em', apiUrl);
                            continue;
                        }
                    }
                }
            }
        }
        if (response?.data?.order) {
            // Buscar a linha correta no Baserow pelo campo 'link' igual ao linkId
            /* DISABLED
            const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
            if (result.success) {
                const row = result.rows.find(r => r[CONTROLE_FIELDS.LINK] === linkId);
                if (row) {
                    const updateData = {
                        statushttp: 'OK',
                        teste: 'OK'
                    };
                    // Para serviços de seguidores, salva instauser; para post (curtidas/visualizações), salva linkpost
                    if (isFollowerService) {
                        updateData.instauser = targetValue;
                    } else {
                        updateData.linkpost = targetValue;
                    }
                    await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, mapControleData(updateData));
                }
            }
            */
            
            // INVALIDAR O LINK TEMPORÁRIO após pedido bem-sucedido
            if (linkId && linkId !== 'teste123') {
                linkManager.invalidateLink(linkId);
                console.log(`🔒 Link temporário invalidado após pedido: ${linkId}`);
            }
            
            return res.json({
                ...response.data,
                success: true,
                message: 'Pedido realizado com sucesso'
            });
        }
        if (isFollowerService && response.data.error === 'link_duplicate') {
            return res.status(409).json({ 
                error: 'link_duplicate',
                message: 'Você acabou de realizar um pedido para este perfil. Aguarde alguns minutos antes de tentar novamente.'
            });
        }
        // Se falhou, retornar erro detalhado
        return res.status(400).json({
            success: false,
            error: response?.data?.error || 'api_error',
            message: 'Falha ao realizar pedido na Fama24h',
            response: response?.data || null
        });
    } catch (error) {
        res.status(500).json({ error: error.message || 'Erro ao enviar pedido' });
    }
});

app.post('/api/check-usage', async (req, res) => {
  const userAgent = req.get('User-Agent') || '';
  const ip = req.realIP || req.ip || req.connection.remoteAddress || req.headers["x-forwarded-for"] || "unknown";
  // Lista de exceção
  const ipExcecao = ['45.190.117.46', '127.0.0.1', '::1', 'localhost'];
  if (ipExcecao.includes(ip)) {
    return res.json({ used: false });
  }
  try {
    return res.json({ used: false }); // DISABLED
    /*
    const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
    if (!result.success) {
      return res.json({ used: false });
    }
    const found = result.rows.find(row =>
      (row[CONTROLE_FIELDS.USER_AGENT] === userAgent) &&
      (row[CONTROLE_FIELDS.IP] === ip)
    );
    if (found) {
      // Verificar se o teste está como 'OK' - só bloquear se teste for OK
      const testeValue = found[CONTROLE_FIELDS.TESTE];
      if (testeValue === 'OK') {
        return res.json({ used: true, message: 'Já há registro de utilização para este IP e navegador.' });
      }
    }
    return res.json({ used: false });
    */
  } catch (err) {
    return res.json({ used: false });
  }
});

// API para verificar se um link temporário já foi usado
app.post('/api/check-link-status', async (req, res) => {
  const { id } = req.query;
  
  if (!id || id === 'teste123') {
    return res.json({ blocked: false });
  }
  
  return res.json({ blocked: false }); // DISABLED: Baserow validation removed
  /*
  try {
    const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
    if (!result.success) {
      return res.json({ blocked: false });
    }
    
    const found = result.rows.find(row =>
      (row[CONTROLE_FIELDS.LINK] === id) &&
      (row[CONTROLE_FIELDS.TESTE] === 'OK')
    );
    
    if (found) {
      console.log(`🔒 Link ${id} já foi usado para um pedido`);
      return res.json({ blocked: true, message: 'Este link temporário já foi usado para um pedido.' });
    }
    
    return res.json({ blocked: false });
  } catch (err) {
    console.error('Erro ao verificar status do link:', err);
    return res.json({ blocked: false });
  }
  */
});

app.post('/api/webhook-phone', async (req, res) => {
  const phone = req.body.tel || req.body.phone;
  console.log('Webhook recebido:', req.body);
  if (!phone) return res.status(400).json({ error: 'Telefone não informado' });

  // Gerar link temporário
  const fakeReq = { ip: req.realIP || req.ip, get: () => req.get('User-Agent') };
  const linkInfo = linkManager.generateLink(fakeReq);

  // Montar dados para o Baserow usando os nomes dos campos
  const data = {
    "tel": phone,                           // tel - telefone
    "link": linkInfo.id,                    // link - link temporário
    "criado": new Date().toISOString()     // criado - data de criação
  };

  console.log('📊 Dados para Baserow (webhook):', data);

  // Criar linha no Baserow
  try {
    // DISABLED: Baserow validation removed
    const fakeRowId = 'disabled_' + Date.now();
    console.log("✅ Webhook processado (Baserow desativado):", fakeRowId);
    
    res.json({ 
      success: true, 
      link: `https://agenciaoppus.site/${linkInfo.id}`,
      rowId: fakeRowId,
      confirmed: true
    });
    
    /*
    const result = await baserowManager.createRow(BASEROW_TABLES.CONTROLE, mapControleData(data));
    if (result.success) {
      console.log("✅ Webhook registrado na tabela controle:", result.row.id);
      // Verificar leitura imediata para confirmar persistência
      const readBack = await baserowManager.getRow(BASEROW_TABLES.CONTROLE, result.row.id);
      if (!readBack.success) {
        console.error("⚠️ Criado mas não foi possível ler a linha imediatamente:", readBack.error);
      } else {
        console.log("🔎 Linha confirmada no Baserow:", readBack.row?.id, readBack.row);
      }
      res.json({ 
        success: true, 
        link: `https://agenciaoppus.site/${linkInfo.id}`,
        rowId: result.row.id,
        confirmed: !!readBack.success
      });
    } else {
      console.error("❌ Erro ao registrar webhook na tabela controle:", result.error);
      res.status(500).json({ error: 'Erro ao criar linha no Baserow', details: result.error });
    }
    */
  } catch (err) {
    console.error("❌ Erro ao registrar webhook:", err);
    res.status(500).json({ error: 'Erro ao criar linha no Baserow', details: err.message });
  }
});

// Importação em massa de telefones
app.post('/api/webhook-phone-bulk', async (req, res) => {
  try {
    const { tels, link } = req.body || {};
    if (!Array.isArray(tels) || tels.length === 0) {
      return res.status(400).json({ error: 'no_tels', message: 'Envie um array "tels" com um ou mais números.' });
    }
    // Normalizar: somente dígitos, remover vazios, deduplicar
    const normalized = Array.from(new Set(
      tels
        .map(t => String(t).replace(/\D/g, ''))
        .filter(t => t && t.length >= 8)
    ));
    const createdIds = [];
    const errors = [];
    for (const tel of normalized) {
      const data = { tel, criado: new Date().toISOString() };
      if (link) data.link = link;
      
      // DISABLED: Baserow validation removed
      createdIds.push('disabled_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9));
      
      /*
      const result = await baserowManager.createRow(BASEROW_TABLES.CONTROLE, mapControleData(data));
      if (result.success) {
        createdIds.push(result.row.id);
      } else {
        errors.push({ tel, error: result.error });
      }
      */
    }
    console.log(`📦 Importação bulk de telefones concluída: ${createdIds.length} criados, ${errors.length} erros.`);
    return res.json({ success: true, total: normalized.length, createdCount: createdIds.length, errorCount: errors.length, createdIds, errors });
  } catch (err) {
    console.error('❌ Erro em webhook-phone-bulk:', err);
    return res.status(500).json({ error: 'bulk_error', message: err.message || 'Erro ao importar telefones' });
  }
});

// Endpoint de diagnóstico: ler linha do Baserow por ID
app.get('/api/debug-baserow-row', async (req, res) => {
  return res.json({ success: false, error: 'Baserow integration disabled' });
  /*
  const id = Number(req.query.id);
  if (!id) return res.status(400).json({ error: 'Informe ?id=<rowId>' });
  try {
    const result = await baserowManager.getRow(BASEROW_TABLES.CONTROLE, id);
    if (!result.success) {
      return res.status(500).json({ error: 'Falha ao ler linha', details: result.error });
    }
    return res.json({ success: true, row: result.row });
  } catch (err) {
    return res.status(500).json({ error: 'Exceção ao ler linha', details: err.message });
  }
  */
});

// Audio Tracking Routes - Progressive
app.post('/api/track-audio-progress', async (req, res) => {
    try {
        const { getCollection } = require('./mongodbClient');
        const col = await getCollection('audio_logs');
        
        // Capture IP and force IPv4 format if mapped
        let ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
        if (ip.includes(',')) ip = ip.split(',')[0].trim();
        if (ip.startsWith('::ffff:')) ip = ip.substring(7); // Normalize to IPv4
        
        const { username, seconds, percentage, milestone, browserId } = req.body;
        const now = new Date();

        // Check for existing recent log (24h window)
        // Match by browserId (stronger) OR ip (fallback)
        let filter = {
            createdAt: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) }
        };
        
        if (browserId) {
            filter.$or = [ { browserId }, { ip } ];
        } else {
            filter.ip = ip;
        }

        const recentLog = await col.findOne(filter, { sort: { createdAt: -1 } });

        if (recentLog) {
            // Update existing record
            const updateFields = { 
                updatedAt: now,
                // Only update max values if current is higher
                max_seconds: Math.max(recentLog.max_seconds || 0, Number(seconds) || 0),
                max_percentage: Math.max(recentLog.max_percentage || 0, Number(percentage) || 0),
                current_milestone: milestone || recentLog.current_milestone
            };
            
            if (browserId && !recentLog.browserId) {
                updateFields.browserId = browserId;
            }
            if (username && !recentLog.username) {
                updateFields.username = username;
            }

            await col.updateOne({ _id: recentLog._id }, { $set: updateFields });
            return res.json({ success: true, updated: true });
        }
        
        // Create new record
        await col.insertOne({
            ip: ip,
            browserId: browserId || null,
            userAgent: req.get('User-Agent') || '',
            username: username || null,
            max_seconds: Number(seconds) || 0,
            max_percentage: Number(percentage) || 0,
            current_milestone: milestone || 'started',
            createdAt: now,
            updatedAt: now
        });
        res.json({ success: true, inserted: true });
    } catch (e) {
        console.error('Audio progress track error:', e);
        res.status(500).json({ error: 'Internal error' });
    }
});

// Legacy/Specific 10% route for User Profile validation logic (keeps existing behavior for validated_insta_users)
app.post('/api/track-audio-10p', async (req, res) => {
    try {
        const { username } = req.body;
        if (!username) return res.status(400).json({ error: 'Username required' });
        
        const { getCollection } = require('./mongodbClient');
        const vu = await getCollection('validated_insta_users');
        
        await vu.updateOne(
            { username: username.toLowerCase() },
            { $set: { audio_listened_10_percent: true } }
        );
        res.json({ success: true });
    } catch (e) {
        console.error('Audio 10% track error:', e);
        res.status(500).json({ error: 'Internal error' });
    }
});

/*
// Meta CAPI: Track InitiateCheckout
app.post('/api/meta/track', async (req, res) => {
  try {
    const PIXEL_ID = process.env.PIXEL_ID || '1019661457030791';
    const ACCESS_TOKEN = process.env.META_CAPI_TOKEN || '';
    const {
      eventName = 'InitiateCheckout',
      value = 0,
      currency = 'BRL',
      contentName = '',
      contents = [],
      phone = '',
      fbp = '',
      correlationID = '',
      eventSourceUrl = ''
    } = req.body || {};
    const normalizePhone = (p) => (String(p || '').replace(/[^0-9]/g, ''));
    const phoneNorm = normalizePhone(phone);
    const phoneHash = phoneNorm ? crypto.createHash('sha256').update(phoneNorm, 'utf8').digest('hex') : undefined;
    const event_time = Math.floor(Date.now() / 1000);
    const userAgent = req.headers['user-agent'] || '';
    const clientIp = (req.headers['x-forwarded-for']?.split(',')[0] || req.socket?.remoteAddress || '').toString();
    const testCode = process.env.META_TEST_EVENT_CODE;
    const payload = {};
    const url = `https://graph.facebook.com/v18.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;
    const resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await resp.json();
    if (!resp.ok) { return res.status(resp.status).json({ success: false, error: data?.error || 'meta_error', details: data }); }
    return res.json({ success: true, result: data });
  } catch (err) {
    return res.status(500).json({ success: false, error: 'exception', details: err?.message || String(err) });
  }
});
*/

  // Webhook Woovi/OpenPix: CHARGE_CREATED -> enviar InitiateCheckout (CAPI)
app.post('/api/openpix/webhook', async (req, res) => {
  try {
    let body = req.body || {};
    if (typeof body === 'string') {
      try { body = JSON.parse(body); } catch(_) { body = {}; }
    }
    const event = String(body.event || '').toUpperCase();

      // Atualiza status para 'pago' quando a cobrança for concluída
    if (/CHARGE_COMPLETED/.test(event)) {
      const charge = body.charge || {};
      const customerName = charge?.customer?.name || null;
      const customerObj = charge?.customer || (body.pix && body.pix.customer) || null;
      const payerObj = charge?.payer || (body.pix && body.pix.payer) || null;
      const additionalInfoArr = Array.isArray(charge.additionalInfo)
        ? charge.additionalInfo
            .filter((it) => it && typeof it.key === 'string' && typeof it.value === 'string' && it.key.trim() && it.value.trim())
            .map((it) => ({ key: String(it.key).trim(), value: String(it.value).trim() }))
        : [];
      const additionalInfoMap = additionalInfoArr.reduce((acc, item) => {
        acc[item.key] = item.value;
        return acc;
      }, {});
      const pixMethod = charge?.paymentMethods?.pix || {};
      const paidAtRaw = charge?.paidAt || (body.pix && body.pix.time) || null;
      const endToEndId = (body.pix && body.pix.endToEndId) || null;
      const txId = pixMethod?.txId || charge?.transactionID || (body.pix && body.pix.transactionID) || null;

      try {
        const col = await getCollection('checkout_orders');
        const conds = [];
        if (charge?.id) conds.push({ 'woovi.chargeId': charge.id });
        if (charge?.correlationID) conds.push({ correlationID: charge.correlationID });
        if (charge?.identifier) {
          conds.push({ 'woovi.identifier': charge.identifier });
          conds.push({ identifier: charge.identifier });
        }
        const filter = conds.length ? { $or: conds } : { correlationID: charge?.correlationID || '' };

        const setFields = {
          status: 'pago',
          'woovi.status': 'pago',
          paidAt: new Date().toISOString(),
        };
        if (customerName) setFields.nomeUsuario = customerName;
        if (paidAtRaw) setFields['woovi.paidAt'] = paidAtRaw;
        if (typeof endToEndId === 'string') setFields['woovi.endToEndId'] = endToEndId;
        if (typeof txId === 'string') setFields['woovi.paymentMethods.pix.txId'] = txId;
        if (typeof pixMethod.status === 'string') setFields['woovi.paymentMethods.pix.status'] = pixMethod.status;
        if (typeof pixMethod.value === 'number') setFields['woovi.paymentMethods.pix.value'] = pixMethod.value;
        if (customerObj && typeof customerObj.name === 'string') setFields['customer.name'] = customerObj.name;
        if (customerObj && customerObj.taxID && typeof customerObj.taxID.taxID === 'string') setFields['customer.taxID'] = customerObj.taxID.taxID;
        if (customerObj && customerObj.taxID && typeof customerObj.taxID.type === 'string') setFields['customer.taxType'] = customerObj.taxID.type;
        if (payerObj && typeof payerObj.name === 'string') setFields['payer.name'] = payerObj.name;
        if (payerObj && payerObj.taxID && typeof payerObj.taxID.taxID === 'string') setFields['payer.taxID'] = payerObj.taxID.taxID;
        if (payerObj && payerObj.taxID && typeof payerObj.taxID.type === 'string') setFields['payer.taxType'] = payerObj.taxID.type;
        if (additionalInfoArr.length) setFields['additionalInfoPaid'] = additionalInfoArr;
        if (Object.keys(additionalInfoMap).length) setFields['additionalInfoMapPaid'] = additionalInfoMap;

        const update = { $set: setFields };

        const result = await col.updateOne(filter, update);
        if (!result.matchedCount) {
          const altFilter = charge?.identifier ? { identifier: charge.identifier } : filter;
          const altResult = await col.updateOne(altFilter, update);
          if (!altResult.matchedCount) {
            const phone = (customerObj && customerObj.phone) || additionalInfoMap['phone'] || null;
            const phoneNorm = phone ? String(phone).replace(/\D/g, '') : null;
            const phoneFilter = phoneNorm ? { $or: [ { 'customer.phone': `+55${phoneNorm}` }, { 'additionalInfo': { $elemMatch: { key: 'phone', value: phoneNorm } } } ] } : null;
            if (phoneFilter) {
              await col.updateOne(phoneFilter, update);
            }
          }
        }
        try {
          const record = await col.findOne(filter);
        
          // ---------------------------------------------------------
          // VALIDAÇÃO RIGOROSA DE VALOR (Solicitado pelo usuário)
          // ---------------------------------------------------------
          // "validar se a resposta de pagamento... o valor é o mesmo do pedido que foi gerado aqui"
          // Se houver discrepância entre o valor pago e o valor esperado (calculado no backend),
          // bloqueamos a entrega do serviço e marcamos como 'divergent_value'.
          
          const paidValue = charge?.value || (pixMethod && pixMethod.value) || record?.valueCents;
          const expectedValue = record?.expectedValueCents;

          if (expectedValue && paidValue && Number(paidValue) < Number(expectedValue)) {
              console.error(`🚨 FRAUD PREVENTED: Payment value ${paidValue} is less than expected ${expectedValue}. Blocking dispatch. Identifier: ${charge?.identifier}`);
              await col.updateOne(filter, { 
                  $set: { 
                      status: 'divergent_value', 
                      'woovi.status': 'divergent_value',
                      mismatchDetails: { expected: expectedValue, paid: paidValue, detectedAt: new Date().toISOString() }
                  } 
              });
              // Não entregamos o serviço
              return res.status(200).json({ ok: true, status: 'divergent_value', message: 'Payment value mismatch detected. Service not dispatched.' });
          }
          // ---------------------------------------------------------

        // Check LIVE privacy before dispatch if not already marked as private
        let isPriv = record && (
            record.isPrivate === true || 
            String(record.isPrivate) === 'true' || 
            record.profilePrivacy?.isPrivate === true || 
            String(record.profilePrivacy?.isPrivate) === 'true'
        );

        // If not marked private in DB, check live to be safe (prevent API dispatch for private)
        if (!isPriv) {
            const instaUser = additionalInfoMap['instagram_username'] || record?.instagramUsername || record?.instauser || '';
            if (instaUser) {
                try {
                    // Check privacy (timeout 5s)
                    const mockReq = { session: {}, query: {}, body: {} };
                    const check = await verifyInstagramProfile(instaUser, 'Webhook-LiveCheck', '127.0.0.1', mockReq, null);
                    if (check && !check.success && /privado/i.test(check.error || '')) {
                         isPriv = true;
                         // Update DB immediately
                         await col.updateOne(filter, { 
                             $set: { 
                                 isPrivate: true, 
                                 'profilePrivacy.isPrivate': true, 
                                 'profilePrivacy.updatedAt': new Date().toISOString() 
                             } 
                         });
                         console.log('🔒 Profile detected as PRIVATE during payment webhook (Live Check):', instaUser);
                    }
                } catch (e) {
                    console.error('⚠️ Live privacy check failed in webhook:', e.message);
                }
            }
        }
        
        if (isPriv) {
            console.log('ℹ️ Service dispatch blocked: Profile is private', { identifier: charge?.identifier });
            try { await broadcastPaymentPaid(charge?.identifier, charge?.correlationID); } catch(_) {}
            
            return res.status(200).json({ ok: true, status: 'paid_private_deferred', message: 'Service dispatch blocked because profile is private' });
        }

        const alreadySentFama = record?.fama24h?.orderId ? true : false;
          const alreadySentFS = record?.fornecedor_social?.orderId ? true : false;
          const tipo = additionalInfoMap['tipo_servico'] || record?.tipo || record?.tipoServico || '';
          const qtdBase = Number(additionalInfoMap['quantidade'] || record?.quantidade || record?.qtd || 0) || 0;
          const instaUser = additionalInfoMap['instagram_username'] || record?.instagramUsername || record?.instauser || '';
          const key = process.env.FAMA24H_API_KEY || '';
          
          const pacoteStr = String(additionalInfoMap['pacote'] || record?.additionalInfoMapPaid?.['pacote'] || '').toLowerCase();
          const categoriaServ = String(additionalInfoMap['categoria_servico'] || '').toLowerCase();
          const isViewsBase = categoriaServ === 'visualizacoes' || /^visualizacoes_reels$/i.test(tipo);
          const isCurtidasBase = pacoteStr.includes('curtida') || categoriaServ === 'curtidas';
          let serviceId = null;
          let linkToSend = instaUser;
          if (isViewsBase) {
              serviceId = 250;
              linkToSend = additionalInfoMap['post_link'] || additionalInfoMap['orderbump_post_views'] || instaUser;
          } else if (isCurtidasBase) {
              if (/^mistos$/i.test(tipo)) {
                  serviceId = 671;
              } else if (/^brasileiros$/i.test(tipo)) {
                  serviceId = 679;
              } else if (/^organicos$/i.test(tipo)) {
                  serviceId = 670;
              }
              linkToSend = additionalInfoMap['post_link'] || instaUser;
          } else {
              if (/^mistos$/i.test(tipo)) {
                  serviceId = 663;
              } else if (/^brasileiros$/i.test(tipo)) {
                  serviceId = 23;
              }
              linkToSend = instaUser;
          }

          // Fallback: se serviço é de curtidas/visualizações e não há post selecionado,
          // tentar usar o último post disponível em validated_insta_users
          if ((isViewsBase || isCurtidasBase) && (!linkToSend || linkToSend === instaUser) && instaUser) {
              try {
                  const { getCollection } = require('./mongodbClient');
                  const vu = await getCollection('validated_insta_users');
                  const vUser = await vu.findOne({ username: String(instaUser).toLowerCase() });
                  if (vUser && vUser.latestPosts && Array.isArray(vUser.latestPosts) && vUser.latestPosts.length > 0) {
                      const lp = vUser.latestPosts[0];
                      const code = lp.shortcode || lp.code;
                      if (code) {
                          linkToSend = `https://www.instagram.com/p/${code}/`;
                          try { console.log('🔄 [Webhook Base] Recuperado link do post via cache para:', instaUser, linkToSend); } catch(_) {}
                      }
                  }
              } catch (eFallbackWebhook) {
                  try { console.error('⚠️ [Webhook Base] Erro ao recuperar post cache:', eFallbackWebhook.message); } catch(_) {}
              }
          }

          const bumpsStr0 = additionalInfoMap['order_bumps'] || '';
          const hasUpgrade = typeof bumpsStr0 === 'string' && /(^|;)upgrade:\d+/i.test(bumpsStr0);
          const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
          const isVisualizacoes = /(visualizacoes|views|reels)/i.test(tipo);
          let upgradeAdd = 0;
          if (hasUpgrade) {
            if (isFollowers) {
              if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) {
                upgradeAdd = 1000;
              } else {
                const map = { 
                  50: 50, 150: 150, 300: 200, 500: 200, 700: 300, 
                  1000: 1000, 1200: 800, 2000: 1000, 3000: 1000, 4000: 1000, 
                  5000: 2500, 7500: 2500, 10000: 5000 
                };
                upgradeAdd = map[qtdBase] || 0;
              }
            } else if (isVisualizacoes) {
               const map = {
                  1000: 1500,
                  5000: 5000,
                  25000: 25000,
                  100000: 50000,
                  200000: 50000,
                  500000: 500000
               };
               upgradeAdd = map[qtdBase] || 0;
            }
          }
          const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
          const isOrganicos = /organicos/i.test(tipo) && !isCurtidasBase && !isViewsBase;
          if (!isOrganicos) {
            const canSend = !!key && !!serviceId && !!linkToSend && qtd > 0 && !alreadySentFama;
            if (canSend) {
              const axios = require('axios');
              const payload = new URLSearchParams({ key, action: 'add', service: String(serviceId), link: String(linkToSend), quantity: String(qtd) });
              console.log('➡️ Enviando pedido Fama24h', { service: serviceId, link: linkToSend, quantity: qtd });
              try {
                const famaResp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const famaData = famaResp.data || {};
                console.log('✅ Fama24h resposta', { status: famaResp.status, data: famaData });
                const orderId = famaData.order || famaData.id || null;
                await col.updateOne(filter, { $set: { fama24h: { orderId, status: orderId ? 'created' : 'unknown', requestPayload: { service: serviceId, link: linkToSend, quantity: qtd }, response: famaData, requestedAt: new Date().toISOString() } } });
                try { await broadcastPaymentPaid(charge?.identifier, charge?.correlationID); } catch(_) {}
              } catch (fErr) {
                console.error('❌ Fama24h erro', fErr?.response?.data || fErr?.message || String(fErr));
                await col.updateOne(filter, { $set: { fama24h: { error: fErr?.response?.data || fErr?.message || String(fErr), requestPayload: { service: serviceId, link: linkToSend, quantity: qtd }, requestedAt: new Date().toISOString() } } });
              }
            } else {
              console.log('ℹ️ Fama24h não enviado', { hasKey: !!key, tipo, qtd: qtdBase, instaUser, alreadySentFama, hasUpgrade });
            }
          } else {
            const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
            const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
            const canSendFS = !!keyFS && !!instaUser && qtd > 0 && !alreadySentFS;
            if (canSendFS) {
              const lockUpdate = await col.updateOne(
                { _id: record._id, 'fornecedor_social.orderId': { $exists: false }, 'fornecedor_social.status': { $ne: 'processing' } },
                { $set: { 'fornecedor_social.status': 'processing', 'fornecedor_social.attemptedAt': new Date().toISOString() } }
              );
              if (lockUpdate.modifiedCount > 0) {
                const axios = require('axios');
                const linkFS = (/^https?:\/\//i.test(String(instaUser))) ? String(instaUser) : `https://instagram.com/${String(instaUser)}`;
                const payloadFS = new URLSearchParams({ key: keyFS, action: 'add', service: String(serviceFS), link: linkFS, quantity: String(qtd) });
                console.log('➡️ Enviando pedido FornecedorSocial', { url: 'https://fornecedorsocial.com/api/v2', payload: Object.fromEntries(payloadFS.entries()) });
                try {
                  const respFS = await axios.post('https://fornecedorsocial.com/api/v2', payloadFS.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                  const dataFS = respFS.data || {};
                  console.log('✅ FornecedorSocial resposta', { status: respFS.status, data: dataFS });
                  const orderIdFS = dataFS.order || dataFS.id || null;
                  await col.updateOne(filter, { $set: { fornecedor_social: { orderId: orderIdFS, status: orderIdFS ? 'created' : 'unknown', requestPayload: { service: serviceFS, link: instaUser, quantity: qtd }, response: dataFS, requestedAt: new Date().toISOString() } } });
                  try { await broadcastPaymentPaid(charge?.identifier, charge?.correlationID); } catch(_) {}
                } catch (fsErr) {
                  console.error('❌ FornecedorSocial erro', { message: fsErr?.message || String(fsErr), data: fsErr?.response?.data, status: fsErr?.response?.status });
                  await col.updateOne(filter, { $set: { fornecedor_social: { error: fsErr?.response?.data || fsErr?.message || String(fsErr), requestPayload: { service: serviceFS, link: instaUser, quantity: qtd }, requestedAt: new Date().toISOString() } } });
                }
              }
            } else {
              console.log('ℹ️ FornecedorSocial não enviado', { hasKeyFS: !!keyFS, tipo, qtd: qtdBase, instaUser, alreadySentFS, hasUpgrade, reason: (!keyFS ? 'missing_key' : (!instaUser ? 'missing_link' : (!qtd ? 'missing_qty' : (alreadySentFS ? 'already_sent' : 'unknown')))) });
            }
          }

          try {
            const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
            const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
            const additionalInfoMap = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
            const bumpsStr = additionalInfoMap['order_bumps'] || (arrPaid.find(it => it && it.key === 'order_bumps')?.value) || (arrOrig.find(it => it && it.key === 'order_bumps')?.value) || '';
            let viewsQty = 0;
            let likesQtyForStatus = 0;
            let commentsQty = 0;
            if (typeof bumpsStr === 'string' && bumpsStr) {
              const parts = bumpsStr.split(';');
            const vPart = parts.find(p => /^views:\d+$/i.test(p.trim()));
            const lPartStatus = parts.find(p => /^likes:\d+$/i.test(p.trim()));
            const cPart = parts.find(p => /^comments:\d+$/i.test(p.trim()));
            if (vPart) {
              const num = Number(vPart.split(':')[1]);
              if (!Number.isNaN(num) && num > 0) viewsQty = num;
            }
            if (lPartStatus) {
              const numL = Number(lPartStatus.split(':')[1]);
              if (!Number.isNaN(numL) && numL > 0) likesQtyForStatus = numL;
            }
            if (cPart) {
              const numC = Number(cPart.split(':')[1]);
              if (!Number.isNaN(numC) && numC > 0) commentsQty = numC;
            }
            }
            const hasFamaKey = !!(process.env.FAMA24H_API_KEY || '');
            if (hasFamaKey) {
              const axios = require('axios');
              const sanitizeLink = (s) => {
                let v = String(s || '').replace(/[`\s]/g, '').trim();
                if (!v) return '';
                if (!/^https?:\/\//i.test(v)) {
                  if (/^www\./i.test(v)) v = `https://${v}`;
                  else if (/^instagram\.com\//i.test(v)) v = `https://${v}`;
                  else if (/^\/\/+/i.test(v)) v = `https:${v}`;
                }
                v = v.split('#')[0].split('?')[0];
                const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?$/i.test(v);
                return ok ? (v.endsWith('/') ? v : (v + '/')) : '';
              };
              const mapPaid2 = record?.additionalInfoMapPaid || {};
              const selectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};
              const selViews = selectedFor && selectedFor.views && selectedFor.views.link ? String(selectedFor.views.link) : '';
              const selLikes = selectedFor && selectedFor.likes && selectedFor.likes.link ? String(selectedFor.likes.link) : '';
              const viewsLinkRaw = mapPaid2['orderbump_post_views'] || selViews || additionalInfoMap['orderbump_post_views'] || (arrPaid.find(it => it && it.key === 'orderbump_post_views')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_views')?.value) || '';
              const likesLinkRaw0 = mapPaid2['orderbump_post_likes'] || selLikes || additionalInfoMap['orderbump_post_likes'] || (arrPaid.find(it => it && it.key === 'orderbump_post_likes')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_likes')?.value) || '';
              const viewsLink = sanitizeLink(viewsLinkRaw);
              const likesLinkSel = sanitizeLink(likesLinkRaw0 || viewsLinkRaw) || viewsLink;

              if (viewsQty > 0) {
                const alreadyViews2 = !!(record && record.fama24h_views && (record.fama24h_views.orderId || record.fama24h_views.status === 'processing' || record.fama24h_views.status === 'created' || record.fama24h_views.status === 'duplicate' || typeof record.fama24h_views.error !== 'undefined' || typeof record.fama24h_views.duplicate !== 'undefined'));
                if (!viewsLink) {
                  await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': 'invalid_link', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
                } else if (!alreadyViews2) {
                  const lockUpdate = await col.updateOne(
                    { ...filter, 'fama24h_views.orderId': { $exists: false }, 'fama24h_views.status': { $nin: ['processing', 'created'] } },
                    { $set: { 'fama24h_views.status': 'processing', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } }
                  );
                  if (lockUpdate.modifiedCount > 0) {
                    const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
                    try {
                      const respViews = await axios.post('https://fama24h.net/api/v2', payloadViews.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                      const dataViews = normalizeProviderResponseData(respViews.data);
                      const orderIdViews = extractProviderOrderId(dataViews);
                      const providerErrViews = (dataViews && (dataViews.error || (dataViews.data && dataViews.data.error) || (dataViews.response && dataViews.response.error))) || null;
                      if (providerErrViews && !orderIdViews) {
                        const errStr = typeof providerErrViews === 'string' ? providerErrViews : JSON.stringify(providerErrViews);
                        const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                        if (st === 'duplicate') {
                          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
                        } else {
                          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() } });
                        }
                      } else {
                        const setObj = { 'fama24h_views.status': orderIdViews ? 'created' : 'unknown', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() };
                        if (orderIdViews) setObj['fama24h_views.orderId'] = orderIdViews;
                        await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_views.error': '', 'fama24h_views.duplicate': '' } });
                      }
                    } catch (e2) {
                      const errVal = e2?.response?.data || e2?.message || String(e2);
                      const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                      const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                      if (st === 'duplicate') {
                        await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
                      } else {
                        await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
                      }
                    }
                  }
                }
              }

              if (likesQtyForStatus > 0) {
                const alreadyLikes2 = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created' || record.fama24h_likes.status === 'duplicate' || typeof record.fama24h_likes.error !== 'undefined' || typeof record.fama24h_likes.duplicate !== 'undefined'));
                if (!likesLinkSel) {
                  await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': 'invalid_link', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                } else if (!alreadyLikes2) {
                  const lockUpdate = await col.updateOne(
                    { ...filter, 'fama24h_likes.orderId': { $exists: false }, 'fama24h_likes.status': { $nin: ['processing', 'created'] } },
                    { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } }
                  );
                  if (lockUpdate.modifiedCount > 0) {
                    const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '671', link: String(likesLinkSel), quantity: String(likesQtyForStatus) });
                    try {
                      const respLikes = await axios.post('https://fama24h.net/api/v2', payloadLikes.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                      const dataLikes = normalizeProviderResponseData(respLikes.data);
                      const orderIdLikes = extractProviderOrderId(dataLikes);
                      const providerErrLikes = (dataLikes && (dataLikes.error || (dataLikes.data && dataLikes.data.error) || (dataLikes.response && dataLikes.response.error))) || null;
                      if (providerErrLikes && !orderIdLikes) {
                        const errStr = typeof providerErrLikes === 'string' ? providerErrLikes : JSON.stringify(providerErrLikes);
                        const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                        if (st === 'duplicate') {
                          await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                        } else {
                          await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                        }
                      } else {
                        const setObj = { 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() };
                        if (orderIdLikes) setObj['fama24h_likes.orderId'] = orderIdLikes;
                        await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_likes.error': '', 'fama24h_likes.duplicate': '' } });
                      }
                    } catch (e3) {
                      const errVal = e3?.response?.data || e3?.message || String(e3);
                      const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                      const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                      if (st === 'duplicate') {
                        await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': errVal, 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                      } else {
                        await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': errVal, 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                      }
                    }
                  }
                }
              }
            }
            if (commentsQty > 0) {
              if (!process.env.WORLDSMM_API_KEY) {
                 console.error('❌ WORLDSMM_API_KEY não definida para orderbump de comentários');
              } else {
                const axios = require('axios');
                const sanitizeLinkC = (s) => {
                  let v = String(s || '').replace(/[`\s]/g, '').trim();
                  if (!v) return '';
                  if (!/^https?:\/\//i.test(v)) {
                    if (/^www\./i.test(v)) v = `https://${v}`;
                    else if (/^instagram\.com\//i.test(v)) v = `https://${v}`;
                    else if (/^\/\/+/i.test(v)) v = `https:${v}`;
                  }
                  v = v.split('#')[0].split('?')[0];
                  const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?$/i.test(v);
                  return ok ? (v.endsWith('/') ? v : (v + '/')) : '';
                };
                const mapPaid3 = record?.additionalInfoMapPaid || {};
                const commentsLinkRaw = mapPaid3['orderbump_post_comments'] || additionalInfoMap['orderbump_post_comments'] || (arrPaid.find(it => it && it.key === 'orderbump_post_comments')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_comments')?.value) || '';
                const commentsLinkSel = sanitizeLinkC(commentsLinkRaw);
                try { console.log('🔎 orderbump_comments_raw', { identifier: charge?.identifier, correlationID: charge?.correlationID, commentsLinkRaw, commentsQty }); } catch(_) {}
                
                const alreadyComments = !!(record && record.worldsmm_comments && (record.worldsmm_comments.orderId || record.worldsmm_comments.status === 'processing' || record.worldsmm_comments.status === 'created'));
                
                if (!commentsLinkSel) {
                  const serviceIdRaw = String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90');
                  const serviceIdNum = Number(serviceIdRaw);
                  const serviceId = Number.isFinite(serviceIdNum) ? serviceIdNum : 90;
                  await col.updateOne(filter, { $set: { worldsmm_comments: { error: 'invalid_link', requestPayload: { service: serviceId, link: commentsLinkSel, quantity: commentsQty }, requestedAt: new Date().toISOString() } } });
                } else if (!alreadyComments) {
                  const lockUpdate = await col.updateOne(
                    { ...filter, $or: [{ 'worldsmm_comments.status': { $exists: false } }, { 'worldsmm_comments.status': { $in: ['error', 'unknown'] } }] },
                    { $set: { 'worldsmm_comments.status': 'processing', 'worldsmm_comments.requestedAt': new Date().toISOString() }, $unset: { 'worldsmm_comments.error': '' } }
                  );
                  if (lockUpdate.modifiedCount > 0) {
                      const serviceIdRaw = String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90');
                      const serviceIdNum = Number(serviceIdRaw);
                      const serviceId = Number.isFinite(serviceIdNum) ? serviceIdNum : 90;
                      const payloadComments = new URLSearchParams({ key: String(process.env.WORLDSMM_API_KEY), action: 'add', service: serviceIdRaw, link: String(commentsLinkSel), quantity: String(commentsQty) });
                      try {
                        const worldsmmUrl = 'https://worldsmm.com.br/api/v2';
                        const timeoutMs = 60000;
                        const maxAttempts = 3;
                        const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                        let respComments = null;
                        let lastErr = null;
                        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
                          try {
                            try { console.log('🚀 sending_worldsmm_comments', { service: '90', link: commentsLinkSel, quantity: commentsQty, attempt, timeoutMs }); } catch(_) {}
                            respComments = await axios.post(worldsmmUrl, payloadComments.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: timeoutMs });
                            lastErr = null;
                            break;
                          } catch (err) {
                            lastErr = err;
                            const msg = String(err?.message || '');
                            const isTimeout = msg.toLowerCase().includes('timeout') || msg.toLowerCase().includes('ecconnaborted');
                            if (attempt < maxAttempts && isTimeout) {
                              await sleep(1500 * attempt);
                              continue;
                            }
                            throw err;
                          }
                        }
                        const dataComments = normalizeProviderResponseData(respComments.data);
                        const orderIdComments = extractProviderOrderId(dataComments);
                        const setObj = { 'worldsmm_comments.status': orderIdComments ? 'created' : 'unknown', 'worldsmm_comments.requestPayload': { service: serviceId, link: commentsLinkSel, quantity: commentsQty }, 'worldsmm_comments.response': dataComments };
                        if (orderIdComments) setObj['worldsmm_comments.orderId'] = orderIdComments;
                        await col.updateOne(filter, { $set: setObj });
                      } catch (e4) {
                        try { console.error('❌ worldsmm_comments_error', e4?.response?.data || e4?.message || String(e4), { link: commentsLinkSel, quantity: commentsQty }); } catch(_) {}
                        await col.updateOne(filter, { $set: { 'worldsmm_comments.error': e4?.response?.data || e4?.message || String(e4), 'worldsmm_comments.status': 'error', 'worldsmm_comments.requestPayload': { service: serviceId, link: commentsLinkSel, quantity: commentsQty } } });
                      }
                  }
                }
              }
            }
          } catch (_) {}

          try {
            const trackUrl = 'https://track.agenciaoppus.site/webhook/validar-confirmado';
            const trackPayload = {
              event: 'CHECKOUT_PIX_PAID',
              identifier: charge?.identifier || null,
              correlationID: charge?.correlationID || null,
              value: Number(charge?.value || 0) || null,
              paidAt: paidAtRaw || null,
              endToEndId: endToEndId || null,
              tipo_servico: tipo || null,
              quantidade: qtd || null,
              instagram_username: instaUser || null,
              phone: (customerObj && customerObj.phone) || additionalInfoMap['phone'] || null
            };
            const resp = await fetch(trackUrl, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(trackPayload)
            });
            const data = await resp.text();
            console.log('🔗 Track validar-confirmado', { status: resp.status, body: data });
          } catch (tErr) {
            console.error('⚠️ Falha ao notificar validar-confirmado', tErr?.message || String(tErr));
          }
          broadcastPaymentPaid(charge?.identifier, charge?.correlationID);
          // try { await trackMetaPurchaseForOrder(charge?.identifier, charge?.correlationID, req); } catch(_) {}
          try { setTimeout(() => { try { dispatchPendingOrganicos(); } catch(_) {} }, 0); } catch(_) {}
        } catch (sendErr) {
          console.error('⚠️ Falha ao enviar para Fama24h', sendErr?.message || String(sendErr));
        }
        return res.status(200).json({ ok: true, event, matched: result.matchedCount, modified: result.modifiedCount });
        } catch (dbErr) {
          return res.status(500).json({ ok: false, error: 'mongo_update_failed', details: dbErr?.message || String(dbErr) });
        }
      }

    // Somente para CHARGE_CREATED: dispara InitiateCheckout na CAPI
    if (!/CHARGE_CREATED/.test(event)) {
      return res.status(200).json({ ok: true, ignored: true });
    }

    const charge = body.charge || {};
    const addInfoArr = Array.isArray(charge.additionalInfo) ? charge.additionalInfo : [];
    const addInfo = addInfoArr.reduce((acc, item) => {
      const k = String(item?.key || '').trim();
      const v = String(item?.value || '').replace(/[`]/g, '').trim();
      acc[k] = v;
      return acc;
    }, {});

    const tipo = addInfo['tipo_servico'] || '';
    const qtd = Number(addInfo['quantidade'] || 0) || 0;
    const pacote = addInfo['pacote'] || '';
    const phoneRaw = addInfo['phone'] || charge?.customer?.phone || '';
    const correlationID = charge.correlationID || charge?.customer?.correlationID || '';
    const valueCents = Number(charge.value || 0) || 0;
    const valueBRL = valueCents ? (Math.round(valueCents) / 100) : 0;
    const paymentLinkUrl = String(charge.paymentLinkUrl || '').replace(/[`]/g, '').trim();

    // Monta payload para CAPI
    const contents = (tipo && qtd) ? [{ id: tipo, quantity: qtd }] : [];
    const userAgent = req.headers['user-agent'] || '';
    const clientIp = (req.headers['x-forwarded-for']?.split(',')[0] || req.socket?.remoteAddress || '').toString();
    const eventSourceUrl = paymentLinkUrl || `https://agenciaoppus.site/checkout${phoneRaw ? `?phone=${encodeURIComponent(phoneRaw)}` : ''}`;

    /*
    // Reutiliza endpoint de rastreio
    const trackResp = await fetch(`${req.protocol}://${req.get('host')}/api/meta/track`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        eventName: 'InitiateCheckout',
        value: valueBRL,
        currency: 'BRL',
        contentName: pacote,
        contents,
        phone: phoneRaw,
        correlationID,
        eventSourceUrl
      })
    });
    const trackData = await trackResp.json();
    if (!trackResp.ok) {
      return res.status(trackResp.status).json({ ok: false, error: trackData?.error || 'capi_error', details: trackData });
    }
    return res.json({ ok: true, capi: trackData });
    */
    return res.json({ ok: true, capi: 'disabled' });
  } catch (err) {
    return res.status(500).json({ ok: false, error: 'exception', details: err?.message || String(err) });
  }
});

// Fallback: Disparar envio de serviço para fornecedor (Fama24h/FornecedorSocial) manualmente
app.post('/api/services/dispatch', async (req, res) => {
  try {
    const identifier = String((req.body && (req.body.identifier || req.body.id)) || req.query.identifier || '').trim();
    const correlationID = String((req.body && req.body.correlationID) || req.query.correlationID || '').trim();
    if (!identifier && !correlationID) return res.status(400).json({ ok: false, error: 'missing_identifier' });
    const col = await getCollection('checkout_orders');
    const filter = identifier ? { $or: [ { identifier }, { 'woovi.identifier': identifier } ] } : { correlationID };
    const record = await col.findOne(filter);
    if (!record) return res.status(404).json({ ok: false, error: 'not_found' });
  const additionalInfoMap = record.additionalInfoMapPaid || (Array.isArray(record.additionalInfoPaid) ? record.additionalInfoPaid.reduce((acc, it) => { acc[it.key] = it.value; return acc; }, {}) : (Array.isArray(record.additionalInfo) ? record.additionalInfo.reduce((acc, it) => { acc[it.key] = it.value; return acc; }, {}) : {}));
  const tipo = additionalInfoMap['tipo_servico'] || record.tipo || record.tipoServico || '';
  const qtdBase = Number(additionalInfoMap['quantidade'] || record.quantidade || record.qtd || 0) || 0;
    const instaUserRaw = additionalInfoMap['instagram_username'] || record.instagramUsername || record.instauser || '';
    const instaUser = (/^https?:\/\//i.test(String(instaUserRaw))) ? String(instaUserRaw) : `https://instagram.com/${String(instaUserRaw)}`;
    const alreadySentFama = !!(record && record.fama24h && record.fama24h.orderId);
    const alreadySentFS = !!(record && record.fornecedor_social && record.fornecedor_social.orderId);
  const bumpsStr0 = additionalInfoMap['order_bumps'] || (record.additionalInfoPaid || []).find(it => it && it.key === 'order_bumps')?.value || (record.additionalInfo || []).find(it => it && it.key === 'order_bumps')?.value || '';
  const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
    let upgradeAdd = 0;
    if (isFollowers && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
      if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) upgradeAdd = 1000; else {
        const map = { 50: 50, 150: 150, 300: 200, 500: 200, 700: 300, 1000: 1000, 1200: 800, 2000: 1000, 3000: 1000, 4000: 1000, 5000: 2500, 7500: 2500, 10000: 5000 };
        upgradeAdd = map[qtdBase] || 0;
      }
    }
    const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
  const pacoteStr = String(additionalInfoMap['pacote'] || '').toLowerCase();
  const categoriaServ = String(additionalInfoMap['categoria_servico'] || '').toLowerCase();
  const isViewsBase = categoriaServ === 'visualizacoes' || /^visualizacoes_reels$/i.test(tipo);
  const isCurtidasBase = pacoteStr.includes('curtida') || categoriaServ === 'curtidas';
  const isOrganicos = /organicos/i.test(tipo) && !isCurtidasBase && !isViewsBase;
  if (!isOrganicos) {
    const key = process.env.FAMA24H_API_KEY || '';
    let serviceId = null;
    if (isViewsBase) {
      serviceId = 250;
    } else if (isCurtidasBase) {
      if (/^mistos$/i.test(tipo)) {
        serviceId = 671;
      } else if (/^brasileiros$/i.test(tipo)) {
        serviceId = 679;
      } else if (/^organicos$/i.test(tipo)) {
        serviceId = 670;
      }
    } else {
      if (/^mistos$/i.test(tipo)) {
        serviceId = 663;
      } else if (/^brasileiros$/i.test(tipo)) {
        serviceId = 23;
      }
    }
      const canSend = !!key && !!serviceId && !!instaUser && qtd > 0 && !alreadySentFama;
      if (!canSend) return res.status(400).json({ ok: false, error: 'cannot_send_fama', reason: { hasKey: !!key, serviceId, instaUser: !!instaUserRaw, qtd, alreadySentFama } });
      const axios = require('axios');
      const payload = new URLSearchParams({ key, action: 'add', service: String(serviceId), link: String(instaUser), quantity: String(qtd) });
      const famaResp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
      const famaData = famaResp.data || {};
      const orderId = famaData.order || famaData.id || null;
      await col.updateOne(filter, { $set: { fama24h: { orderId, status: orderId ? 'created' : 'unknown', requestPayload: { service: serviceId, link: instaUser, quantity: qtd }, response: famaData, requestedAt: new Date().toISOString() } } });
      try { await broadcastPaymentPaid(identifier, correlationID); } catch(_) {}
      try { await ensureRefilLink(identifier, correlationID, req); } catch(_) {}
      return res.json({ ok: true, provider: 'fama24h', orderId, data: famaData });
    } else {
      const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
      const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
      const canSendFS = !!keyFS && !!instaUser && qtd > 0 && !alreadySentFS;
      if (!canSendFS) return res.status(400).json({ ok: false, error: 'cannot_send_fs', reason: { hasKeyFS: !!keyFS, instaUser: !!instaUserRaw, qtd, alreadySentFS } });
      const axios = require('axios');
      const payloadFS = new URLSearchParams({ key: keyFS, action: 'add', service: String(serviceFS), link: String(instaUser), quantity: String(qtd) });
      const respFS = await axios.post('https://fornecedorsocial.com/api/v2', payloadFS.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
      const dataFS = respFS.data || {};
      const orderIdFS = dataFS.order || dataFS.id || null;
      await col.updateOne(filter, { $set: { fornecedor_social: { orderId: orderIdFS, status: orderIdFS ? 'created' : 'unknown', requestPayload: { service: serviceFS, link: instaUser, quantity: qtd }, response: dataFS, requestedAt: new Date().toISOString() } } });
      try { await broadcastPaymentPaid(record?.identifier, record?.correlationID); } catch(_) {}
      try { await ensureRefilLink(record?.identifier, record?.correlationID, req); } catch(_) {}
      return res.json({ ok: true, provider: 'fornecedor_social', orderId: orderIdFS, data: dataFS });
    }
  } catch (err) {
    return res.status(500).json({ ok: false, error: 'exception', details: err?.message || String(err) });
  }
});

// Healthcheck MongoDB endpoints
app.get('/api/mongo/health', async (req, res) => {
  try {
    const col = await getCollection('health_checks');
    const doc = { ts: new Date().toISOString(), ua: req.get('User-Agent') || '', ip: req.realIP || req.ip || null };
    const result = await col.insertOne(doc);
    res.json({ ok: true, insertedId: result.insertedId });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/mongo/ping', async (req, res) => {
  try {
    const col = await getCollection('health_checks');
    const one = await col.findOne({}, { projection: { _id: 1 }, sort: { _id: -1 } });
    res.json({ ok: true, last: one?._id || null });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/mongo/validated-count', async (req, res) => {
  try {
    const vu = await getCollection('validated_insta_users');
    const c = await vu.countDocuments();
    const last = await vu.find({}).sort({checkedAt:-1,_id:-1}).limit(5).toArray();
    res.json({ ok: true, count: c, last });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/debug/validated', async (req, res) => {
  try {
    const username = String((req.body && req.body.username) || '').trim().toLowerCase();
    if (!username) return res.status(400).json({ ok: false, error: 'missing_username' });
    const vu = await getCollection('validated_insta_users');
    const doc = { username, checkedAt: new Date().toISOString(), source: 'api.debug' };
    const ins = await vu.insertOne(doc);
    res.json({ ok: true, insertedId: ins.insertedId });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/instagram/validated', async (req, res) => {
  try {
    const col = await getCollection('validated_insta_users');
    const cursor = col.find({}, { projection: { _id: 1, username: 1, checkedAt: 1, isPrivate: 1, isVerified: 1, linkId: 1 } }).sort({ checkedAt: -1, _id: -1 }).limit(20);
    const rows = await cursor.toArray();
    res.json({ ok: true, items: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/instagram/validet', async (req, res) => {
  try {
    const { username } = req.query || {};
    const col = await getCollection('validated_insta_users');
    const filter = username ? { username: String(username).trim() } : {};
    const cursor = col.find(filter, { projection: { _id: 1, username: 1, checkedAt: 1 } }).sort({ checkedAt: -1, _id: -1 }).limit(50);
    const rows = await cursor.toArray();
    res.json({ ok: true, items: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/instagram/validet-track', async (req, res) => {
  try {
    const username = String((req.body && req.body.username) || '').trim().toLowerCase();
    if (!username) return res.json({ ok: false, error: 'missing_username' });
    
    let trackStored = true;
    let trackError = null;
    try {
      const vu = await getCollection('validated_insta_users');
      const doc = { username, ip: req.realIP || req.ip || null, userAgent: req.get('User-Agent') || '', source: 'api.validet.track', lastTrackAt: new Date().toISOString() };
      await vu.updateOne({ username }, { $setOnInsert: { username, firstSeenAt: new Date().toISOString() }, $set: doc }, { upsert: true });
    } catch (eStore) {
      trackStored = false;
      trackError = eStore?.message || String(eStore);
    }

    // Link audio_logs to username
    try {
        const al = await getCollection('audio_logs');
        let ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
        if (ip.includes(',')) ip = ip.split(',')[0].trim();
        if (ip.startsWith('::ffff:')) ip = ip.substring(7);

        // Normalize ::1 to 127.0.0.1 if needed, or query both to be safe
        const ips = [ip];
        if (ip === '::1') ips.push('127.0.0.1');
        if (ip === '127.0.0.1') ips.push('::1');

        let { browserId } = req.body;
        if (browserId === 'null' || browserId === 'undefined') browserId = null;

        console.log(`[ValidEt-Track] Linking audio_logs for username=${username}, IPs=${JSON.stringify(ips)}, browserId=${browserId}`);

        // Construct filter: priority to browserId, fallback to IP
        let matchQuery = {
            createdAt: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) }
        };

        const emptyUserClause = { $or: [ { username: null }, { username: '' }, { username: { $exists: false } } ] };

        if (browserId) {
             // If browserId matches, we update regardless of existing username (overwrite)
             // If only IP matches, we only update if username is empty
             matchQuery.$or = [
                 { browserId: browserId },
                 { 
                     ip: { $in: ips },
                     ...emptyUserClause
                 }
             ];
        } else {
             // No browserId: match IP only if username is empty
             matchQuery.ip = { $in: ips };
             matchQuery.$and = [ emptyUserClause ];
        }

        console.log(`[ValidEt-Track] Query:`, JSON.stringify(matchQuery, null, 2));

        const recentLog = await al.updateMany(matchQuery, { $set: { username, updatedAt: new Date() } });

        console.log(`[ValidEt-Track] Updated ${recentLog.modifiedCount} audio_logs. Matched ${recentLog.matchedCount}.`);

    } catch (eLog) {
        console.error('Failed to link audio_log in validet-track:', eLog);
    }

    return res.json({ ok: true, trackStored, trackError });
  } catch (e) {
    return res.json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/orderbump/resend', async (req, res) => {
  try {
    const identifier = String((req.body && (req.body.identifier || req.body.id)) || req.query.identifier || '').trim();
    const correlationID = String((req.body && req.body.correlationID) || req.query.correlationID || '').trim();
    if (!identifier && !correlationID) return res.status(400).json({ ok: false, error: 'missing_identifier' });
    const col = await getCollection('checkout_orders');
    const filter = identifier ? { $or: [ { identifier }, { 'woovi.identifier': identifier } ] } : { correlationID };
    const record = await col.findOne(filter);
    if (!record) return res.status(404).json({ ok: false, error: 'not_found' });
    const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
    const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
    const mapPaid = record?.additionalInfoMapPaid || {};
    const mapArr = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
    const map = Object.assign({}, mapArr, mapPaid);
    const bumpsStr = map['order_bumps'] || '';
    const parts = typeof bumpsStr === 'string' ? bumpsStr.split(';') : [];
    const vPart = parts.find(p => /^views:\d+$/i.test(String(p||'').trim()));
    const lPart = parts.find(p => /^likes:\d+$/i.test(String(p||'').trim()));
    const viewsQty = vPart ? Number(String(vPart).split(':')[1]) : 0;
    const likesQty = lPart ? Number(String(lPart).split(':')[1]) : 0;
    const sanitize = (s) => { const v = String(s || '').replace(/[`\s]/g, '').trim(); const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v); return ok ? (v.endsWith('/') ? v : (v + '/')) : ''; };
    const viewsLink = sanitize(map['orderbump_post_views']);
    const likesLink = sanitize(map['orderbump_post_likes'] || map['orderbump_post_views']);
    const key = process.env.FAMA24H_API_KEY || '';
    const results = { views: null, likes: null };
    if (key && viewsQty > 0 && viewsLink) {
      const axios = require('axios');
      const payload = new URLSearchParams({ key, action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
      try {
        const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
        const data = normalizeProviderResponseData(resp.data);
        const orderIdViews = extractProviderOrderId(data);
        const providerErrViews = (data && (data.error || (data.data && data.data.error) || (data.response && data.response.error))) || null;
        if (providerErrViews && !orderIdViews) {
          const errStr = typeof providerErrViews === 'string' ? providerErrViews : JSON.stringify(providerErrViews);
          const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
          if (st === 'duplicate') {
            await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': data, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
            results.views = { duplicate: true, orderId: record?.fama24h_views?.orderId || null, data };
          } else {
            await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': data, 'fama24h_views.requestedAt': new Date().toISOString() } });
            results.views = { orderId: null, data };
          }
        } else {
          const setObj = { 'fama24h_views.status': orderIdViews ? 'created' : 'unknown', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': data, 'fama24h_views.requestedAt': new Date().toISOString() };
          if (orderIdViews) setObj['fama24h_views.orderId'] = orderIdViews;
          await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_views.error': '', 'fama24h_views.duplicate': '' } });
          results.views = { orderId: orderIdViews, data };
        }
      } catch (e) {
        const errVal = e?.response?.data || e?.message || String(e);
        const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
        const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
        if (st === 'duplicate') {
          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
          results.views = { duplicate: true, orderId: record?.fama24h_views?.orderId || null, error: null };
        } else {
          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
          results.views = { error: e?.message || String(e) };
        }
      }
    }
    if (key && likesQty > 0 && likesLink) {
      const alreadyLikes = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
      if (!alreadyLikes) {
        const lockUpdate = await col.updateOne(
          { ...filter, 'fama24h_likes.status': { $nin: ['processing', 'created'] }, 'fama24h_likes.orderId': { $exists: false } },
          { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } }
        );
        if (lockUpdate.modifiedCount > 0) {
          const axios = require('axios');
          const payload = new URLSearchParams({ key, action: 'add', service: '671', link: String(likesLink), quantity: String(likesQty) });
          try {
            const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
            const data = normalizeProviderResponseData(resp.data);
            const orderIdLikes = extractProviderOrderId(data);
            const providerErrLikes = (data && (data.error || (data.data && data.data.error) || (data.response && data.response.error))) || null;
            if (providerErrLikes && !orderIdLikes) {
              const errStr = typeof providerErrLikes === 'string' ? providerErrLikes : JSON.stringify(providerErrLikes);
              const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
              if (st === 'duplicate') {
                await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                results.likes = { duplicate: true, orderId: record?.fama24h_likes?.orderId || null, data };
              } else {
                await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                results.likes = { orderId: null, data };
              }
            } else {
              const setObj = { 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data, 'fama24h_likes.requestedAt': new Date().toISOString() };
              if (orderIdLikes) setObj['fama24h_likes.orderId'] = orderIdLikes;
              await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_likes.error': '', 'fama24h_likes.duplicate': '' } });
              results.likes = { orderId: orderIdLikes, data };
            }
          } catch (e) {
            const errVal = e?.response?.data || e?.message || String(e);
            const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
            const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
            if (st === 'duplicate') {
              await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': errVal, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
              results.likes = { duplicate: true, orderId: record?.fama24h_likes?.orderId || null, error: null };
            } else {
              await col.updateOne(filter, { $set: { 'fama24h_likes.error': errVal, 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
              results.likes = { error: e?.message || String(e) };
            }
          }
        } else {
           results.likes = { error: 'already_processing_or_created' };
        }
      } else {
         results.likes = { error: 'already_exists' };
      }
    }
    try { await broadcastPaymentPaid(identifier, correlationID); } catch(_) {}
    return res.json({ ok: true, results });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err?.message || String(err) });
  }
});
app.post('/api/orderbump/fix-latest', async (req, res) => {
  try {
    const identifier = String((req.body && (req.body.identifier || req.body.id)) || req.query.identifier || '').trim();
    const correlationID = String((req.body && req.body.correlationID) || req.query.correlationID || '').trim();
    const col = await getCollection('checkout_orders');
    let records = [];
    if (identifier || correlationID) {
      const filter = identifier ? { $or: [ { identifier }, { 'woovi.identifier': identifier } ] } : { correlationID };
      const one = await col.findOne(filter);
      records = one ? [one] : [];
    } else {
      records = await col.find({ $or: [ { status: 'pago' }, { 'woovi.status': 'pago' } ] }).sort({ _id: -1 }).limit(20).toArray();
    }
    const key = process.env.FAMA24H_API_KEY || '';
    const out = [];
    for (const record of records) {
      const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
      const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
      const map = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
      const bumpsStr = map['order_bumps'] || '';
      const parts = typeof bumpsStr === 'string' ? bumpsStr.split(';') : [];
      const vPart = parts.find(p => /^views:\d+$/i.test(String(p||'').trim()));
      const lPart = parts.find(p => /^likes:\d+$/i.test(String(p||'').trim()));
      const viewsQty = vPart ? Number(String(vPart).split(':')[1]) : 0;
      const likesQty = lPart ? Number(String(lPart).split(':')[1]) : 0;
      const sanitize = (s) => { const v = String(s || '').replace(/[`\s]/g, '').trim(); const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v); return ok ? (v.endsWith('/') ? v : (v + '/')) : ''; };
      const viewsLink = sanitize(map['orderbump_post_views']);
      const likesLink = sanitize(map['orderbump_post_likes'] || map['orderbump_post_views']);
      const filter = { _id: record._id };
      const resultItem = { id: String(record._id), views: null, likes: null };
      if (key && viewsQty > 0 && viewsLink) {
        const currentViewsLinkRaw = String(record?.fama24h_views?.requestPayload?.link || '');
        const currentViewsLink = sanitize(currentViewsLinkRaw);
        const isInvalid = !!currentViewsLinkRaw && !currentViewsLink;
        if (currentViewsLink && currentViewsLink !== currentViewsLinkRaw) {
          await col.updateOne(filter, { $set: { 'fama24h_views.requestPayload.link': currentViewsLink } });
        }
        if (isInvalid) {
          const axios = require('axios');
          const payload = new URLSearchParams({ key, action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
          try {
            const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
            const data = normalizeProviderResponseData(resp.data);
            const orderIdViews = extractProviderOrderId(data);
            const providerErrViews = (data && (data.error || (data.data && data.data.error) || (data.response && data.response.error))) || null;
            if (providerErrViews && !orderIdViews) {
              const errStr = typeof providerErrViews === 'string' ? providerErrViews : JSON.stringify(providerErrViews);
              const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
              if (st === 'duplicate') {
                await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': data, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
                resultItem.views = { duplicate: true, orderId: record?.fama24h_views?.orderId || null };
              } else {
                await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': providerErrViews, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': data, 'fama24h_views.requestedAt': new Date().toISOString() } });
                resultItem.views = { orderId: null };
              }
            } else {
              const setObj = { 'fama24h_views.status': orderIdViews ? 'created' : 'unknown', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': data, 'fama24h_views.requestedAt': new Date().toISOString() };
              if (orderIdViews) setObj['fama24h_views.orderId'] = orderIdViews;
              await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_views.error': '', 'fama24h_views.duplicate': '' } });
              resultItem.views = { orderId: orderIdViews };
            }
          } catch (e) {
            const errVal = e?.response?.data || e?.message || String(e);
            const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
            const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
            if (st === 'duplicate') {
              await col.updateOne(filter, { $set: { 'fama24h_views.status': 'duplicate', 'fama24h_views.duplicate': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } });
              resultItem.views = { duplicate: true, orderId: record?.fama24h_views?.orderId || null };
            } else {
              await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
              resultItem.views = { error: e?.message || String(e) };
            }
          }
        }
      }
      if (key && likesQty > 0 && likesLink) {
        const currentLikesLinkRaw = String(record?.fama24h_likes?.requestPayload?.link || '');
        const currentLikesLink = sanitize(currentLikesLinkRaw);
        const isInvalidLikes = !!currentLikesLinkRaw && !currentLikesLink;
        if (currentLikesLink && currentLikesLink !== currentLikesLinkRaw) {
          await col.updateOne(filter, { $set: { 'fama24h_likes.requestPayload.link': currentLikesLink } });
        }
        const isProcessing = record?.fama24h_likes?.status === 'processing';
        if (isInvalidLikes && !isProcessing) {
          const lockUpdate = await col.updateOne(
             { ...filter, 'fama24h_likes.status': { $ne: 'processing' } },
             { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestedAt': new Date().toISOString() } }
          );
          if (lockUpdate.modifiedCount > 0) {
              const axios = require('axios');
              const payload = new URLSearchParams({ key, action: 'add', service: '671', link: String(likesLink), quantity: String(likesQty) });
              try {
                const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const data = normalizeProviderResponseData(resp.data);
                const orderIdLikes = extractProviderOrderId(data);
                const providerErrLikes = (data && (data.error || (data.data && data.data.error) || (data.response && data.response.error))) || null;
                if (providerErrLikes && !orderIdLikes) {
                  const errStr = typeof providerErrLikes === 'string' ? providerErrLikes : JSON.stringify(providerErrLikes);
                  const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                  if (st === 'duplicate') {
                    await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                    resultItem.likes = { duplicate: true, orderId: record?.fama24h_likes?.orderId || null };
                  } else {
                    await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': providerErrLikes, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                    resultItem.likes = { orderId: null };
                  }
                } else {
                  const setObj = { 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data, 'fama24h_likes.requestedAt': new Date().toISOString() };
                  if (orderIdLikes) setObj['fama24h_likes.orderId'] = orderIdLikes;
                  await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_likes.error': '', 'fama24h_likes.duplicate': '' } });
                  resultItem.likes = { orderId: orderIdLikes };
                }
              } catch (e) {
                const errVal = e?.response?.data || e?.message || String(e);
                const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                if (st === 'duplicate') {
                  await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'duplicate', 'fama24h_likes.duplicate': errVal, 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } });
                  resultItem.likes = { duplicate: true, orderId: record?.fama24h_likes?.orderId || null };
                } else {
                  await col.updateOne(filter, { $set: { 'fama24h_likes.error': errVal, 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 671, link: likesLink, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                  resultItem.likes = { error: e?.message || String(e) };
                }
              }
          }
        }
      }
      out.push(resultItem);
    }
    return res.json({ ok: true, results: out });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err?.message || String(err) });
  }
});
app.get('/api/orders', async (req, res) => {
  try {
    const phone = String(req.query.phone || '').trim();
    if (!phone) return res.status(400).json({ ok: false, error: 'missing_phone' });
    const col = await getCollection('orders');
    const orders = await col.find({ $or: [ { 'customer.phone': phone }, { 'additionalInfo': { $elemMatch: { key: 'phone', value: phone } } } ] }).sort({ _id: -1 }).limit(20).toArray();
    res.json({ ok: true, orders });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/checkout-orders', async (req, res) => {
  try {
    const phone = String(req.query.phone || '').trim();
    if (!phone) return res.status(400).json({ ok: false, error: 'missing_phone' });
    const col = await getCollection('checkout_orders');
    const orders = await col.find({ $or: [ { 'customer.phone': phone }, { 'additionalInfo': { $elemMatch: { key: 'phone', value: phone } } } ] }).sort({ _id: -1 }).limit(20).toArray();
    res.json({ ok: true, orders });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/checkout/payment-state', async (req, res) => {
  try {
    const id = String(req.query.id || '').trim();
    const identifier = String(req.query.identifier || '').trim();
    const correlationID = String(req.query.correlationID || '').trim();
    const col = await getCollection('checkout_orders');
    const conds = [];
    
    if (id) {
        conds.push({ 'woovi.chargeId': id });
        conds.push({ 'woovi.id': id });
        // Try as ObjectId if valid
        if (/^[0-9a-fA-F]{24}$/.test(id)) {
            const { ObjectId } = require('mongodb');
            try { conds.push({ _id: new ObjectId(id) }); } catch(_) {}
        }
    }
    if (identifier) { 
        conds.push({ 'woovi.identifier': identifier }); 
        conds.push({ identifier: identifier }); 
    }
    if (correlationID) conds.push({ correlationID });
    
    const filter = conds.length ? { $or: conds } : {};
    const doc = await col.findOne(filter, { projection: { status: 1, woovi: 1 } });
    
    const paid = !!doc && (
        String(doc.status).toLowerCase() === 'pago' || 
        String(doc.woovi?.status || '').toLowerCase() === 'pago' ||
        String(doc.woovi?.status || '').toLowerCase() === 'completed'
    );
    
    return res.json({ ok: true, paid, order: doc || null });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/pedido', async (req, res) => {
  try {
    const identifier = String(req.query.identifier || req.query.t || '').trim();
     const correlationID = String(req.query.correlationID || req.query.ref || '').trim();
     const orderIDRaw = String(req.query.orderID || req.query.orderid || req.query.oid || '').trim();
     const phoneRaw = String(req.query.phone || '').trim();
    const hasQuery = !!(identifier || correlationID || orderIDRaw || phoneRaw);
    const hasSessionCtx = !!(req.session && (req.session.selectedOrderID || req.session.lastPaidIdentifier || req.session.lastPaidCorrelationID));
    if (!hasQuery && !hasSessionCtx) {
      return res.redirect('/cliente');
    }
    const col = await getCollection('checkout_orders');
    let doc = null;
    // 1) Se houver parâmetros na URL, priorizar sempre a busca por eles
    if (hasQuery) {
      const conds = [];
      if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
      if (correlationID) conds.push({ correlationID });
      if (orderIDRaw) {
        const maybeNum = Number(orderIDRaw);
        if (!Number.isNaN(maybeNum)) {
          conds.push({ 'fama24h.orderId': maybeNum });
          conds.push({ 'fornecedor_social.orderId': maybeNum });
        }
        conds.push({ 'fama24h.orderId': orderIDRaw });
        conds.push({ 'fornecedor_social.orderId': orderIDRaw });
        // Support woovi.chargeId
        conds.push({ 'woovi.chargeId': orderIDRaw });
        conds.push({ 'woovi.id': orderIDRaw });
        
        const { ObjectId } = require('mongodb');
        if (/^[0-9a-fA-F]{24}$/.test(orderIDRaw)) {
             try { conds.push({ _id: new ObjectId(orderIDRaw) }); } catch(_) {}
        }
      }
      if (phoneRaw) {
        const digits = phoneRaw.replace(/\D/g, '');
        if (digits) {
          conds.push({ 'customer.phone': `+55${digits}` });
          conds.push({ additionalInfo: { $elemMatch: { key: 'phone', value: digits } } });
        }
      }
      if (conds.length) {
        doc = await col.findOne({ $or: conds });
      }
    }
    // 2) Se não veio query ou ela não encontrou nada, usar pedido selecionado em sessão
    if (!doc && req.session && req.session.selectedOrderID) {
      const soid = req.session.selectedOrderID;
      const { ObjectId } = require('mongodb');
      const orConds = [ { 'fama24h.orderId': soid }, { 'fornecedor_social.orderId': soid } ];
      if (typeof soid === 'string' && /^[0-9a-fA-F]{24}$/.test(soid)) {
        try { orConds.push({ _id: new ObjectId(soid) }); } catch(_) {}
      }
      doc = await col.findOne({ $or: orConds });
    }
    // 3) Por último, usar último pago guardado em sessão (apenas se recente)
    if (!doc && req.session && (req.session.lastPaidIdentifier || req.session.lastPaidCorrelationID)) {
      const markedAt = Number(req.session.lastPaidMarkedAt || 0);
      const isFresh = markedAt && ((Date.now() - markedAt) <= (30 * 60 * 1000));
      if (isFresh) {
        const lpId = String(req.session.lastPaidIdentifier || '').trim();
        const lpCorr = String(req.session.lastPaidCorrelationID || '').trim();
        const firstConds = [];
        if (lpId) { firstConds.push({ 'woovi.identifier': lpId }); firstConds.push({ identifier: lpId }); }
        if (lpCorr) firstConds.push({ correlationID: lpCorr });
        if (firstConds.length) {
          const paidFilter = { $and: [ { $or: firstConds }, { $or: [ { status: 'pago' }, { 'woovi.status': 'pago' } ] } ] };
          try {
            const arr = await col.find(paidFilter).sort({ 'woovi.paidAt': -1, paidAt: -1 }).limit(1).toArray();
            doc = (Array.isArray(arr) && arr.length) ? arr[0] : null;
          } catch (_) {
            doc = await col.findOne({ $or: firstConds });
          }
        }
      }
    }
    
    // Garantir que o link de refil exista para este pedido (para o botão "Acessar ferramenta")
    if (doc) {
      try {
        if (!doc.refilLinkId) {
            // Tentar gerar/recuperar link se não existir
            const rLink = await ensureRefilLink(doc.identifier || doc['woovi.identifier'] || '', doc.correlationID || '', req);
            if (rLink && rLink.id) {
                doc.refilLinkId = rLink.id;
            }
        }
      } catch (errLink) {
        try { console.warn('Falha ao garantir refilLink em /pedido', errLink.message); } catch(_) {}
      }
    }

    const order = doc || {};
    let refilDaysLeft = null;
    try {
      const token = order && order.refilLinkId ? String(order.refilLinkId).trim() : '';
      if (token) {
        const tl = await getCollection('temporary_links');
        const linkRec = await tl.findOne({ id: token }, { projection: { createdAt: 1, expiresAt: 1, warrantyMode: 1, warrantyDays: 1 } });
        const isLifetime = String(linkRec?.warrantyMode || '').toLowerCase() === 'life';
        if (!isLifetime) {
          const brtOffsetMs = 3 * 60 * 60 * 1000;
          const dayMs = 24 * 60 * 60 * 1000;
          const brtYmdFromMs = (ms) => {
            const d = new Date(Number(ms || 0) - brtOffsetMs);
            return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
          };
          const dayNumFromYmd = (y, m, d) => Math.floor(Date.UTC(Number(y), Number(m) - 1, Number(d)) / dayMs);
          const nowYmd = brtYmdFromMs(Date.now());
          const nowDay = dayNumFromYmd(nowYmd.y, nowYmd.m, nowYmd.d);
          if (linkRec?.expiresAt) {
            const expMs = new Date(String(linkRec.expiresAt)).getTime();
            if (Number.isFinite(expMs) && expMs > 0) {
              const expYmd = brtYmdFromMs(expMs);
              const expDay = dayNumFromYmd(expYmd.y, expYmd.m, expYmd.d);
              refilDaysLeft = Math.max(0, expDay - nowDay);
            }
          }
        }
      }
    } catch (_) {}
    try {
      const map = order && order.additionalInfoMapPaid ? order.additionalInfoMapPaid : {};
      let uname = String(map['instagram_username'] || '').trim();
      if (!uname) uname = String(order.instagramUsername || order.instauser || '').trim();
      
      // Sempre verificar status mais recente no banco de validação
      if (uname) {
        const vu = await getCollection('validated_insta_users');
        const vUser = await vu.findOne({ username: String(uname).trim().toLowerCase() });
        
        // Atualizar em background se não existir ou se for antigo (> 1 hora)
        const nowMs = Date.now();
        const lastCheck = vUser && vUser.checkedAt ? new Date(vUser.checkedAt).getTime() : 0;
        const isOld = (nowMs - lastCheck) > (60 * 60 * 1000);
        
        if (!vUser || isOld) {
             const mockReq = { session: {}, query: {}, body: {} };
             // Disparar verificação sem aguardar (fire-and-forget) para não travar o carregamento
             verifyInstagramProfile(uname, 'Background-Pedido', req.ip || '127.0.0.1', mockReq, null)
                 .catch(err => { try { console.error('❌ [pedido] Falha ao atualizar perfil Instagram em background:', err.message); } catch(_) {} });
        }

        if (vUser) {
            // Se o banco diz que é privado, forçar status privado
            // Se o banco diz que é público, atualizar também (caso o usuário tenha aberto)
            order.profilePrivacy = { 
                username: uname, 
                isPrivate: !!vUser.isPrivate, 
                checkedAt: vUser.checkedAt,
                source: 'db_fresh_check' 
            };
            if (typeof vUser.followersCount === 'number') order.followersCount = vUser.followersCount;
        }
      }
    } catch (_) {}
    return res.render('pedido', { order, refilDaysLeft, PIXEL_ID: process.env.PIXEL_ID || '', logoLink: '/engajamento' });
  } catch (e) {
    return res.status(500).type('text/plain').send('Erro ao carregar pedido');
  }
});

app.get('/posts', async (req, res) => {
  try {
    const usernameParam = String(req.query.username || '').trim();
    const usernameSession = req.session && req.session.instagramProfile && req.session.instagramProfile.username ? String(req.session.instagramProfile.username) : '';
    const username = (usernameParam || usernameSession || '').toLowerCase();
    return res.render('posts', { username });
  } catch (e) {
    return res.status(500).send('Erro ao renderizar posts');
  }
});

app.get('/api/instagram/posts', async (req, res) => {
  try {
    const usernameParam = String(req.query.username || '').trim();
    const usernameSession = req.session && req.session.instagramProfile && req.session.instagramProfile.username ? String(req.session.instagramProfile.username) : '';
    const username = String(usernameParam || usernameSession || '').trim().toLowerCase();
    if (!username) return res.json({ success: false, error: 'missing_username', posts: [] });
    const debugInsert = String(req.query.debug || '').trim() === '1';
    let debugInfo = null;
    try {
      const vu = await getCollection('validated_insta_users');
      // Tentar buscar do banco primeiro se tiver posts recentes (ex: < 1h)
      // Se acabou de validar o perfil, os posts estarão lá
      const cachedDoc = await vu.findOne({ username });
      if (cachedDoc && cachedDoc.latestPosts && Array.isArray(cachedDoc.latestPosts) && cachedDoc.latestPosts.length > 0) {
          // Verificar idade do cache (opcional, mas bom pra não retornar post velho)
          // Mas se o usuário acabou de validar, é novo.
          const lastCheck = cachedDoc.checkedAt || cachedDoc.lastPostsAt;
          const isFresh = lastCheck && (Date.now() - new Date(lastCheck).getTime() < 3600000); // 1h
          
          if (isFresh) {
               console.log('[API] Retornando posts do banco (validated_insta_users)');
               return res.json({ success: true, username, posts: cachedDoc.latestPosts });
          }
      }

      const doc = { source: 'api.instagram.posts', lastPostsAt: new Date().toISOString() };
      // Remove username from $setOnInsert to avoid conflict if it's already in the filter
      await vu.updateOne({ username }, { $setOnInsert: { firstSeenAt: new Date().toISOString() }, $set: doc }, { upsert: true });
      debugInfo = { ok: true, username };
      try { console.log('🗃️ Posts route: upsert ok', debugInfo); } catch(_) {}
    } catch (err) { debugInfo = { ok: false, error: err?.message || String(err) }; try { console.error('❌ Posts route: upsert error', err?.message || String(err)); } catch(_) {} }
    
    // Otimização: Se cookies estão falhando muito, pular direto pro Apify/Fallback
    // Mas vamos manter a tentativa rápida (timeout reduzido)
    try {
      console.log('[API] tentando web_profile_info com cookies');
      // Reduzir timeout implícito na chamada se possível ou assumir que fetchInstagramRecentPosts foi otimizado
      const result = await fetchInstagramRecentPosts(username);
      if (result && result.success && Array.isArray(result.posts) && result.posts.length) {
        if (debugInsert) return res.json(Object.assign({}, result, { debugInsert: debugInfo }));
        return res.json(result);
      }
    } catch (e) { /* fallback abaixo */ }

    // Fallback: Tentar Apify (via verifyInstagramProfile que agora retorna posts)
    try {
        console.log('[API] tentando fallback APIFY');
        // Simulando req/res para verifyInstagramProfile ou chamando lógica direta
        // Vamos usar verifyInstagramProfile mas precisamos adaptar pois ele espera req, res e retorna cache/json
        // Melhor chamar a lógica de Apify diretamente ou usar uma função extraída.
        // Como verifyInstagramProfile já faz cache e tudo, podemos chamá-lo passando mocks ou extrair.
        // Para simplificar e manter DRY, vamos chamar verifyInstagramProfile se não tiver em cache
        
        // Hack: verificar cache primeiro
        const cached = getCachedProfile(username);
        let apifyData = cached;
        
        if (!apifyData || !apifyData.latestPosts) {
             // Se não tem cache ou cache sem posts, força verificação (que usa apify)
             // Precisamos de um jeito de chamar a lógica do Apify sem depender de req/res do express
             // Vamos duplicar a chamada do Apify aqui ou refatorar verifyInstagramProfile?
             // Refatorar é arriscado agora. Vamos duplicar a chamada do Apify para garantir posts.
             
             const apifyToken = process.env.APIFY_TOKEN;
                if (apifyToken) {
                 const apifyUrl = `https://api.apify.com/v2/acts/apify~instagram-profile-scraper/run-sync-get-dataset-items?token=${apifyToken}`;
                 const payload = { usernames: [username], resultsLimit: 1 };
                 const respA = await axios.post(apifyUrl, payload, { headers: { 'Content-Type': 'application/json' }, timeout: 30000 });
                 const itemsA = respA.data;
                 if (Array.isArray(itemsA) && itemsA.length > 0 && itemsA[0] && !itemsA[0].error) {
                     const item = itemsA[0];
                     if (item.latestPosts && Array.isArray(item.latestPosts)) {
                         const posts = item.latestPosts.map(p => ({
                             shortcode: p.shortCode || p.shortcode,
                             takenAt: p.timestamp || (p.date ? new Date(p.date).getTime()/1000 : null),
                             isVideo: p.type === 'Video' || p.isVideo,
                             displayUrl: p.displayUrl || p.displayURL || p.imageUrl || p.thumbnailSrc,
                             videoUrl: p.videoUrl || p.videoURL,
                             typename: p.type === 'Video' ? 'GraphVideo' : 'GraphImage'
                         })).slice(0, 12);
                         return res.json({ success: true, username, posts, debugInsert: debugInsert ? debugInfo : undefined });
                     }
                 }
             }
        } else if (apifyData && apifyData.latestPosts) {
             return res.json({ success: true, username, posts: apifyData.latestPosts, debugInsert: debugInsert ? debugInfo : undefined });
        }
    } catch (eApify) { console.error('[API] Fallback Apify error:', eApify.message); }

    try {
      console.log('[API] tentando fallback HTML');
      const basic = await fetchInstagramPosts(username);
      if (basic && basic.success && Array.isArray(basic.posts) && basic.posts.length) {
        const posts = basic.posts.slice(0, 8).map(sc => ({ shortcode: sc, takenAt: null, isVideo: false, displayUrl: null, videoUrl: null }));
        return res.json({ success: true, username, posts, debugInsert: debugInsert ? debugInfo : undefined });
      }
    } catch (e2) { /* sem fallback */ }
    return res.json({ success: true, username, posts: [], debugInsert: debugInsert ? debugInfo : undefined });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
});

app.post('/api/instagram/select-post', async (req, res) => {
  try {
    const username = String((req.body && req.body.username) || '').trim();
    const shortcode = String((req.body && req.body.shortcode) || '').trim();
    if (!shortcode) {
      return res.status(400).json({ success: false, error: 'missing_shortcode' });
    }
    const link = `https://www.instagram.com/p/${encodeURIComponent(shortcode)}/`;
    const prev = Array.isArray(req.session.selectedPosts) ? req.session.selectedPosts : [];
    const filtered = prev.filter(p => p.shortcode !== shortcode);
    const selected = { username, shortcode, link, savedAt: new Date().toISOString() };
    req.session.selectedPosts = [selected, ...filtered].slice(0, 50);
    return res.json({ success: true, selectedPost: selected, selectedPosts: req.session.selectedPosts });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
});

app.get('/api/instagram/selected-posts', async (req, res) => {
  try {
    const list = Array.isArray(req.session.selectedPosts) ? req.session.selectedPosts : [];
    return res.json({ success: true, posts: list });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
});

app.post('/api/instagram/select-post-for', async (req, res) => {
  try {
    const username = String((req.body && req.body.username) || '').trim();
    const shortcode = String((req.body && req.body.shortcode) || '').trim();
    const kind = String((req.body && req.body.kind) || '').trim();
    if (!shortcode) { return res.status(400).json({ success: false, error: 'missing_shortcode' }); }
    if (!kind) { return res.status(400).json({ success: false, error: 'missing_kind' }); }
    const link = `https://www.instagram.com/p/${encodeURIComponent(shortcode)}/`;
    if (!req.session.selectedFor) req.session.selectedFor = {};
    req.session.selectedFor[kind] = { username, shortcode, link, savedAt: new Date().toISOString() };
    return res.json({ success: true, selected: req.session.selectedFor[kind], selectedFor: req.session.selectedFor });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
});

app.get('/api/instagram/selected-for', async (req, res) => {
  try {
    const obj = req.session.selectedFor || {};
    return res.json({ success: true, selectedFor: obj });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
});

// Redefinições próximas ao bloco de Instagram para garantir registro
app.get('/api/instagram/validated', async (req, res) => {
  try {
    const col = await getCollection('validated_insta_users');
    const cursor = col.find({}, { projection: { _id: 1, username: 1, checkedAt: 1, isPrivate: 1, isVerified: 1, linkId: 1 } }).sort({ checkedAt: -1, _id: -1 }).limit(20);
    const rows = await cursor.toArray();
    res.json({ ok: true, items: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/instagram/validet', async (req, res) => {
  try {
    const { username } = req.query || {};
    const col = await getCollection('validated_insta_users');
    const filter = username ? { username: String(username).trim() } : {};
    const cursor = col.find(filter, { projection: { _id: 1, username: 1, checkedAt: 1 } }).sort({ checkedAt: -1, _id: -1 }).limit(50);
    const rows = await cursor.toArray();
    res.json({ ok: true, items: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
// rota duplicada removida: validet-track já está definida anteriormente

app.post('/api/instagram/selected-posts/clear', async (req, res) => {
  try {
    req.session.selectedPosts = [];
    return res.json({ success: true, cleared: true });
  } catch (e) {
    return res.status(500).json({ success: false, error: e?.message || String(e) });
  }
});

app.post('/pedido/select', async (req, res) => {
  try {
    const orderIDRaw = String((req.body && (req.body.orderID || req.body.orderid)) || '').trim();
    if (!orderIDRaw) {
      return res.status(400).json({ ok: false, error: 'missing_orderid' });
    }
    const maybeNum = Number(orderIDRaw);
    req.session.selectedOrderID = !Number.isNaN(maybeNum) ? maybeNum : orderIDRaw;
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/session/mark-paid', async (req, res) => {
  try {
    const identifier = String((req.body && req.body.identifier) || '').trim();
    const correlationID = String((req.body && req.body.correlationID) || '').trim();
    if (!identifier && !correlationID) {
      return res.status(400).json({ ok: false, error: 'missing_keys' });
    }
    req.session.lastPaidIdentifier = identifier || req.session.lastPaidIdentifier || '';
    req.session.lastPaidCorrelationID = correlationID || req.session.lastPaidCorrelationID || '';
    req.session.lastPaidMarkedAt = Date.now();
    res.json({ ok: true });
    (async () => {
      try {
        const col = await getCollection('checkout_orders');
        const filter = identifier ? { $or: [ { 'woovi.identifier': identifier }, { identifier } ] } : { correlationID };
        const record = await col.findOne(filter);
        if (!record) return;
        try { console.log('🧩 [mark-paid] record_found', { identifier, correlationID, orderId: String(record?._id || '') }); } catch(_) {}
        const additionalInfoMap = record.additionalInfoMapPaid || (Array.isArray(record.additionalInfoPaid) ? record.additionalInfoPaid.reduce((acc, it) => { acc[it.key] = it.value; return acc; }, {}) : {});
        const tipo = additionalInfoMap['tipo_servico'] || record.tipo || record.tipoServico || '';
        const qtdBase = Number(additionalInfoMap['quantidade'] || record.quantidade || record.qtd || 0) || 0;
        const instaUserRaw = additionalInfoMap['instagram_username'] || record.instagramUsername || record.instauser || '';
        const instaUser = (/^https?:\/\//i.test(String(instaUserRaw))) ? String(instaUserRaw) : `https://instagram.com/${String(instaUserRaw)}`;
        const alreadySentFS = !!(record && record.fornecedor_social && record.fornecedor_social.orderId);
        const alreadySentFama = !!(record && record.fama24h && record.fama24h.orderId);
        const bumpsStr0 = additionalInfoMap['order_bumps'] || '';
        const pacoteStr = String(additionalInfoMap['pacote'] || '').toLowerCase();
        const categoriaServ = String(additionalInfoMap['categoria_servico'] || '').toLowerCase();
        const isViewsBase = categoriaServ === 'visualizacoes' || /^visualizacoes_reels$/i.test(tipo);
        const isCurtidasBase = pacoteStr.includes('curtida') || categoriaServ === 'curtidas';
        const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
        let upgradeAdd = 0;
        if (isFollowers && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
          if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) upgradeAdd = 1000; else {
            const map = { 50: 50, 150: 150, 300: 200, 500: 200, 700: 300, 1000: 1000, 1200: 800, 2000: 1000, 3000: 1000, 4000: 1000, 5000: 2500, 7500: 2500, 10000: 5000 };
            upgradeAdd = map[qtdBase] || 0;
          }
        }
        const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
        const isOrganicosFollowers = /organicos/i.test(tipo) && !isCurtidasBase && !isViewsBase;
        if (isOrganicosFollowers && !alreadySentFS) {
          const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
          const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
          if (!!keyFS && !!instaUser && qtd > 0) {
            const lockUpdate = await col.updateOne(
              { _id: record._id, 'fornecedor_social.orderId': { $exists: false }, 'fornecedor_social.status': { $ne: 'processing' } },
              { $set: { 'fornecedor_social.status': 'processing', 'fornecedor_social.attemptedAt': new Date().toISOString() } }
            );
            if (lockUpdate.modifiedCount > 0) {
              const axios = require('axios');
              const payloadFS = new URLSearchParams({ key: keyFS, action: 'add', service: String(serviceFS), link: String(instaUser), quantity: String(qtd) });
              try {
                const respFS = await axios.post('https://fornecedorsocial.com/api/v2', payloadFS.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const dataFS = respFS.data || {};
                const orderIdFS = dataFS.order || dataFS.id || null;
                await col.updateOne(filter, { $set: { fornecedor_social: { orderId: orderIdFS, status: orderIdFS ? 'created' : 'unknown', requestPayload: { service: serviceFS, link: instaUser, quantity: qtd }, response: dataFS, requestedAt: new Date().toISOString() } } });
                try { await broadcastPaymentPaid(identifier, correlationID); } catch(_) {}
              } catch (_) {}
            }
          }
        }
          try {
            const arrPaidX = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
            const arrOrigX = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
            const addMapX = (arrPaidX.length ? arrPaidX : arrOrigX).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
            const isRefilExt = String(addMapX['tipo_servico'] || '').trim() === 'refil_extensao';
            const linkIdX = String(addMapX['refil_link_id'] || '').trim();
            const modeX = String(addMapX['refil_mode'] || '').trim();
            if (isRefilExt && linkIdX) {
              const tl = await getCollection('temporary_links');
              const nowMsX = Date.now();
              const linkRecX = await tl.findOne({ id: linkIdX }, { projection: { expiresAt: 1 } });
              const currentExpMsX = (() => {
                try {
                  const t = new Date(linkRecX?.expiresAt).getTime();
                  return Number.isFinite(t) ? t : 0;
                } catch (_) { return 0; }
              })();
              const baseMsX = Math.max(nowMsX, currentExpMsX || 0);
              const setsX = {};
              if (modeX === 'life') {
                setsX.expiresAt = new Date('2099-12-31T23:59:59.999Z').toISOString();
                setsX.warrantyMode = 'life';
                setsX.warrantyDays = null;
              } else {
                const expMsX = baseMsX + 30 * 24 * 60 * 60 * 1000;
                setsX.expiresAt = new Date(expMsX).toISOString();
                setsX.warrantyMode = '30';
                setsX.warrantyDays = 30;
              }
              await tl.updateOne({ id: linkIdX }, { $set: setsX });
              try { await col.updateOne(filter, { $set: { refilLinkId: linkIdX } }); } catch(_) {}
            }
          } catch(_) {}
          try {
            const axios = require('axios');
          const mapPaid = record?.additionalInfoMapPaid || {};
          const bumpsStr = String(bumpsStr0 || '').trim();
          let viewsQty = 0;
          let likesQty = 0;
          if (bumpsStr) {
            const parts = bumpsStr.split(';').map(p => String(p || '').trim());
            const vPart = parts.find(p => /^views:\d+$/i.test(p));
            const lPart = parts.find(p => /^likes:\d+$/i.test(p));
            if (vPart) { const num = Number(vPart.split(':')[1]); if (!Number.isNaN(num) && num > 0) viewsQty = num; }
            if (lPart) { const numL = Number(lPart.split(':')[1]); if (!Number.isNaN(numL) && numL > 0) likesQty = numL; }
          }
          const sanitizeLink = (s) => {
            let v = String(s || '').replace(/[`\s]/g, '').trim();
            if (!v) return '';
            if (!/^https?:\/\//i.test(v)) {
              if (/^www\./i.test(v)) v = `https://${v}`;
              else if (/^instagram\.com\//i.test(v)) v = `https://${v}`;
              else if (/^\/\/+/i.test(v)) v = `https:${v}`;
            }
            v = v.split('#')[0].split('?')[0];
            const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?$/i.test(v);
            return ok ? (v.endsWith('/') ? v : (v + '/')) : '';
          };
          const selectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};
          const selViews = selectedFor && selectedFor.views && selectedFor.views.link ? String(selectedFor.views.link) : '';
          const selLikes = selectedFor && selectedFor.likes && selectedFor.likes.link ? String(selectedFor.likes.link) : '';
          const viewsLinkRaw = mapPaid['orderbump_post_views'] || selViews || (record?.additionalInfoPaid || []).find(it => it && it.key === 'orderbump_post_views')?.value || (record?.additionalInfo || []).find(it => it && it.key === 'orderbump_post_views')?.value || '';
          const likesLinkRaw = mapPaid['orderbump_post_likes'] || selLikes || (record?.additionalInfoPaid || []).find(it => it && it.key === 'orderbump_post_likes')?.value || (record?.additionalInfo || []).find(it => it && it.key === 'orderbump_post_likes')?.value || '';
          const viewsLink = sanitizeLink(viewsLinkRaw);
          const likesLinkSel = sanitizeLink(likesLinkRaw);
          try { console.log('🔎 [mark-paid] orderbump_raw', { identifier, correlationID, viewsLinkRaw, likesLinkRaw, selectedFor, viewsQty, likesQty }); } catch(_) {}
          try { console.log('🔎 [mark-paid] orderbump_sanitized', { viewsLink, likesLinkSel }); } catch(_) {}
          const alreadyViews3 = !!(record && record.fama24h_views && (record.fama24h_views.orderId || record.fama24h_views.status === 'processing' || record.fama24h_views.status === 'created' || typeof record.fama24h_views.error !== 'undefined'));
          if ((process.env.FAMA24H_API_KEY || '') && viewsQty > 0 && !alreadyViews3) {
            if (!viewsLink) {
              try { console.warn('⚠️ [mark-paid] views_link_invalid', { viewsLinkRaw }); } catch(_) {}
              await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': 'invalid_link', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
            } else {
              const lockUpdate = await col.updateOne(
                { ...filter, 'fama24h_views.orderId': { $exists: false }, 'fama24h_views.status': { $nin: ['processing', 'created'] } },
                { $set: { 'fama24h_views.status': 'processing', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } }
              );
              if (lockUpdate.modifiedCount > 0) {
                const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
                try { console.log('🚀 [mark-paid] sending_fama24h_views', { service: 250, link: viewsLink, quantity: viewsQty }); } catch(_) {}
                try {
                  const respV = await postFormWithRetry('https://fama24h.net/api/v2', payloadViews.toString(), 60000, 3);
                  const dataV = normalizeProviderResponseData(respV.data);
                  const orderIdV = extractProviderOrderId(dataV);
                  const setObj = { 'fama24h_views.status': orderIdV ? 'created' : 'unknown', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataV, 'fama24h_views.requestedAt': new Date().toISOString() };
                  if (orderIdV) setObj['fama24h_views.orderId'] = orderIdV;
                  await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_views.error': '' } });
                } catch (e2) {
                  const errVal = e2?.response?.data || e2?.message || String(e2);
                  const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                  const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                  try { console.error('❌ [mark-paid] fama24h_views_error', errVal); } catch(_) {}
                  await col.updateOne(filter, { $set: { 'fama24h_views.status': st, 'fama24h_views.error': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
                }
              }
            }
          }
          const alreadyLikes3 = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
          if ((process.env.FAMA24H_API_KEY || '') && likesQty > 0 && !alreadyLikes3) {
            if (!likesLinkSel) {
              try { console.warn('⚠️ [mark-paid] likes_link_invalid', { likesLinkRaw }); } catch(_) {}
              await col.updateOne(filter, { $set: { 'fama24h_likes.status': 'error', 'fama24h_likes.error': 'invalid_link', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
            } else {
               const lockUpdate = await col.updateOne(
                  { ...filter, 'fama24h_likes.orderId': { $exists: false }, 'fama24h_likes.status': { $nin: ['processing', 'created'] } },
                  { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } }
               );
               if (lockUpdate.modifiedCount > 0) {
                  const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '671', link: String(likesLinkSel), quantity: String(likesQty) });
                  try { console.log('🚀 [mark-paid] sending_fama24h_likes', { service: 671, link: likesLinkSel, quantity: likesQty }); } catch(_) {}
                  try {
                    const respL = await postFormWithRetry('https://fama24h.net/api/v2', payloadLikes.toString(), 60000, 3);
                    const dataL = normalizeProviderResponseData(respL.data);
                    const orderIdL = extractProviderOrderId(dataL);
                    const setObj = { 'fama24h_likes.status': orderIdL ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.response': dataL, 'fama24h_likes.requestedAt': new Date().toISOString() };
                    if (orderIdL) setObj['fama24h_likes.orderId'] = orderIdL;
                    await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_likes.error': '' } });
                  } catch (e3) {
                    const errVal = e3?.response?.data || e3?.message || String(e3);
                    const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                    const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                    try { console.error('❌ [mark-paid] fama24h_likes_error', errVal); } catch(_) {}
                    await col.updateOne(filter, { $set: { 'fama24h_likes.error': errVal, 'fama24h_likes.status': st, 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
                  }
               }
            }
          }

          // Order Bump: Comments (WorldsMM)
          let commentsQty = 0;
          if (bumpsStr) {
            const parts = bumpsStr.split(';').map(p => String(p || '').trim());
            const cPart = parts.find(p => /^comments:\d+$/i.test(p));
            if (cPart) { const numC = Number(cPart.split(':')[1]); if (!Number.isNaN(numC) && numC > 0) commentsQty = numC; }
          }
          
          const commentsLinkRaw = mapPaid['orderbump_post_comments'] || 
                                  mapPaid['post_link'] || 
                                  (record?.additionalInfoPaid || []).find(it => it && it.key === 'post_link')?.value || 
                                  (record?.additionalInfo || []).find(it => it && it.key === 'post_link')?.value || 
                                  '';
          const commentsLink = sanitizeLink(commentsLinkRaw);
          
          const alreadyComments = !!(record && record.worldsmm_comments && (record.worldsmm_comments.orderId || record.worldsmm_comments.status === 'processing' || record.worldsmm_comments.status === 'created'));
          
          if ((process.env.WORLDSMM_API_KEY || '') && commentsQty > 0 && !alreadyComments) {
            if (!commentsLink) {
               try { console.warn('⚠️ [mark-paid] comments_link_invalid', { commentsLinkRaw }); } catch(_) {}
               const serviceIdRaw = String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90');
               const serviceIdNum = Number(serviceIdRaw);
               const serviceId = Number.isFinite(serviceIdNum) ? serviceIdNum : 90;
               await col.updateOne(filter, { $set: { worldsmm_comments: { error: 'invalid_link', requestPayload: { service: serviceId, link: commentsLink, quantity: commentsQty }, requestedAt: new Date().toISOString() } } });
            } else {
               const lockUpdate = await col.updateOne(
                  { ...filter, $or: [{ 'worldsmm_comments.status': { $exists: false } }, { 'worldsmm_comments.status': { $in: ['error', 'unknown'] } }] },
                  { $set: { 'worldsmm_comments.status': 'processing', 'worldsmm_comments.requestedAt': new Date().toISOString() }, $unset: { 'worldsmm_comments.error': '' } }
               );
               if (lockUpdate.modifiedCount > 0) {
                  const serviceIdRaw = String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90');
                  const serviceIdNum = Number(serviceIdRaw);
                  const serviceId = Number.isFinite(serviceIdNum) ? serviceIdNum : 90;
                  const payloadComments = new URLSearchParams({ key: String(process.env.WORLDSMM_API_KEY), action: 'add', service: serviceIdRaw, link: String(commentsLink), quantity: String(commentsQty) });
                  try {
                    const worldsmmUrl = 'https://worldsmm.com.br/api/v2';
                    const timeoutMs = 60000;
                    const maxAttempts = 3;
                    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                    let respC = null;
                    let lastErr = null;
                    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
                      try {
                        try { console.log('🚀 [mark-paid] sending_worldsmm_comments', { service: String(process.env.WORLDSMM_SERVICE_ID_COMMENTS || '90'), link: commentsLink, quantity: commentsQty, attempt, timeoutMs }); } catch(_) {}
                        respC = await axios.post(worldsmmUrl, payloadComments.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: timeoutMs });
                        lastErr = null;
                        break;
                      } catch (err) {
                        lastErr = err;
                        const msg = String(err?.message || '');
                        const isTimeout = msg.toLowerCase().includes('timeout') || msg.toLowerCase().includes('ecconnaborted');
                        if (attempt < maxAttempts && isTimeout) {
                          await sleep(1500 * attempt);
                          continue;
                        }
                        throw err;
                      }
                    }
                    const dataC = normalizeProviderResponseData(respC.data);
                    const orderIdC = extractProviderOrderId(dataC);
                    const setObj = { 'worldsmm_comments.status': orderIdC ? 'created' : 'unknown', 'worldsmm_comments.requestPayload': { service: serviceId, link: commentsLink, quantity: commentsQty }, 'worldsmm_comments.response': dataC };
                    if (orderIdC) setObj['worldsmm_comments.orderId'] = orderIdC;
                    await col.updateOne(filter, { $set: setObj });
                  } catch (e4) {
                    try { console.error('❌ [mark-paid] worldsmm_comments_error', e4?.response?.data || e4?.message || String(e4)); } catch(_) {}
                    await col.updateOne(filter, { $set: { 'worldsmm_comments.error': e4?.response?.data || e4?.message || String(e4), 'worldsmm_comments.status': 'error', 'worldsmm_comments.requestPayload': { service: serviceId, link: commentsLink, quantity: commentsQty } } });
                  }
               }
            }
          }
        } catch(_) {}
      } catch (_) {}
    })();
    (async () => { try { await ensureRefilLink(identifier, correlationID, req); } catch(_) {} })();
    return;
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/order', async (req, res) => {
  try {
    const id = req.query.id ? String(req.query.id).trim() : '';
    const identifier = String(req.query.identifier || req.query.t || '').trim();
     const correlationID = String(req.query.correlationID || req.query.ref || '').trim();
     const orderIDRaw = String(req.query.orderID || req.query.orderid || req.query.oid || '').trim();
     const phoneRaw = String(req.query.phone || '').trim();
     const refilToken = String(req.query.refilToken || req.query.refilLinkId || '').trim().replace(/[^0-9a-z]/gi, '');
    const col = await getCollection('checkout_orders');
    let doc = null;
    if (id) {
      try { 
          if (/^[0-9a-fA-F]{24}$/.test(id)) {
             doc = await col.findOne({ _id: new (require('mongodb').ObjectId)(id) }); 
          }
      } catch(_) {}
      if (!doc) {
         // Also check woovi.chargeId if id provided
         try { doc = await col.findOne({ 'woovi.chargeId': id }); } catch(_) {}
         if (!doc) try { doc = await col.findOne({ 'woovi.id': id }); } catch(_) {}
      }
    }
    if (!doc && req.session && req.session.selectedOrderID) {
      const soid = req.session.selectedOrderID;
      doc = await col.findOne({ $or: [ { 'fama24h.orderId': soid }, { 'fornecedor_social.orderId': soid } ] });
    }
    if (!doc) {
      const conds = [];
      if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
      if (correlationID) conds.push({ correlationID });
      if (refilToken) conds.push({ refilLinkId: refilToken });
      if (orderIDRaw) {
        const maybeNum = Number(orderIDRaw);
        if (!Number.isNaN(maybeNum)) { conds.push({ 'fama24h.orderId': maybeNum }); conds.push({ 'fornecedor_social.orderId': maybeNum }); }
        conds.push({ 'fama24h.orderId': orderIDRaw });
        conds.push({ 'fornecedor_social.orderId': orderIDRaw });
        const { ObjectId } = require('mongodb');
        if (/^[0-9a-fA-F]{24}$/.test(orderIDRaw)) {
             try { conds.push({ _id: new ObjectId(orderIDRaw) }); } catch(_) {}
        }
      }
      if (phoneRaw) {
        const digits = phoneRaw.replace(/\D/g, '');
        if (digits) {
          conds.push({ 'customer.phone': `+55${digits}` });
          conds.push({ additionalInfo: { $elemMatch: { key: 'phone', value: digits } } });
        }
      }
      const filter = conds.length ? { $or: conds } : {};
      if (conds.length) {
        try {
          const arr = await col.find(filter).sort({ 'woovi.paidAt': -1, paidAt: -1, _id: -1 }).limit(1).toArray();
          doc = (Array.isArray(arr) && arr.length) ? arr[0] : null;
        } catch (_) {
          doc = await col.findOne(filter);
        }
      }
    }
    return res.json({ ok: true, order: doc || null });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/refil/links', async (req, res) => {
  try {
    const onlyValid = String(req.query.onlyValid || '').toLowerCase() === 'true';
    const limit = Math.max(1, Math.min(200, Number(req.query.limit || 50)));
    const tl = await getCollection('temporary_links');
    const filter = { purpose: 'refil' };
    if (onlyValid) {
      filter.expiresAt = { $gt: new Date().toISOString() };
    }
    const cursor = await tl.find(filter).sort({ createdAt: -1 }).limit(limit);
    const rows = await cursor.toArray();
    const col = await getCollection('checkout_orders');
    const enriched = await Promise.all(rows.map(async (r) => {
      let order = null;
      try { order = await col.findOne({ _id: new (require('mongodb').ObjectId)(r.orderId) }, { projection: { _id: 1, identifier: 1, correlationID: 1, 'woovi.paidAt': 1 } }); } catch(_) {}
      return { id: r.id, purpose: r.purpose, orderId: r.orderId, createdAt: r.createdAt, expiresAt: r.expiresAt, order };
    }));
    return res.json({ ok: true, links: enriched });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/refil/order', async (req, res) => {
  try {
    const tokenRaw = String(req.query.token || '').trim().replace(/[^0-9a-z]/gi, '');
    if (!tokenRaw) return res.status(400).json({ ok: false, error: 'missing_token' });

    const tl = await getCollection('temporary_links');
    const linkRec = await tl.findOne({ id: tokenRaw, purpose: 'refil' });
    if (!linkRec) return res.status(404).json({ ok: false, error: 'token_not_found' });

    const { ObjectId } = require('mongodb');
    const ids = [];
    const tryPush = (v) => {
      const s = String(v || '').trim();
      if (/^[0-9a-fA-F]{24}$/.test(s)) {
        try { ids.push(new ObjectId(s)); } catch (_) {}
      }
    };
    tryPush(linkRec.orderId);
    tryPush(linkRec?.order?._id);
    if (Array.isArray(linkRec.orders)) {
      linkRec.orders.forEach(tryPush);
    }

    if (!ids.length) return res.status(404).json({ ok: false, error: 'order_not_found' });

    const col = await getCollection('checkout_orders');
    const pickPaid = (d) => {
      if (!d) return false;
      const st = String(d.status || '').toLowerCase();
      const wst = String(d?.woovi?.status || '').toLowerCase();
      if (st === 'pago' || wst === 'pago') return true;
      if (d.paidAt) return true;
      if (d?.woovi?.paidAt) return true;
      if (d?.payment?.paidAt) return true;
      return false;
    };
    const arr = await col.find({ _id: { $in: ids } }).sort({ 'woovi.paidAt': -1, paidAt: -1, createdAt: -1, _id: -1 }).limit(20).toArray();
    const doc = arr.find(pickPaid) || (arr.length ? arr[0] : null);
    return res.json({ ok: true, token: tokenRaw, order: doc || null });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/api/refil/link-of-order', async (req, res) => {
  try {
    const identifier = String(req.query.identifier || '').trim();
    const correlationID = String(req.query.correlationID || '').trim();
    const phoneRaw = String(req.query.phone || '').trim();
    const conds = [];
    if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
    if (correlationID) conds.push({ correlationID });
    if (phoneRaw) {
      const digits = phoneRaw.replace(/\D/g, '');
      if (digits) {
        conds.push({ 'customer.phone': `+55${digits}` });
        conds.push({ additionalInfo: { $elemMatch: { key: 'phone', value: digits } } });
      }
    }
    const filter = conds.length ? { $or: conds } : {};
    const col = await getCollection('checkout_orders');
    const doc = await col.findOne(filter);
    if (!doc) return res.status(404).json({ ok: false, error: 'order_not_found' });
    if (!doc.refilLinkId) {
      try { await ensureRefilLink(identifier, correlationID, req); } catch(_) {}
    }
    const tl = await getCollection('temporary_links');
    let linkRec = await tl.findOne({ orderId: String(doc._id), purpose: 'refil' });
    if (!linkRec) {
      const arrPaid = Array.isArray(doc?.additionalInfoPaid) ? doc.additionalInfoPaid : [];
      const arrOrig = Array.isArray(doc?.additionalInfo) ? doc.additionalInfo : [];
      const map = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
      const phoneFromCustomer = (doc && doc.customer && doc.customer.phone) ? String(doc.customer.phone).replace(/\D/g, '') : '';
      const phoneFromMap = map['phone'] ? String(map['phone']).replace(/\D/g, '') : '';
      const phoneDigits = phoneFromCustomer || phoneFromMap || '';
      if (phoneDigits) {
        linkRec = await tl.findOne({ purpose: 'refil', phone: phoneDigits });
      }
    }
    if (!linkRec) return res.status(404).json({ ok: false, error: 'link_not_found' });
    return res.json({ ok: true, token: linkRec.id, expiresAt: linkRec.expiresAt, orderId: String(doc._id) });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/refil/backfill', async (req, res) => {
  try {
    const identifier = String((req.body && req.body.identifier) || req.query.identifier || '').trim();
    const correlationID = String((req.body && req.body.correlationID) || req.query.correlationID || '').trim();
    if (!identifier && !correlationID) {
      return res.status(400).json({ ok: false, error: 'missing_identifier_or_correlationID' });
    }
    const rec = await ensureRefilLink(identifier, correlationID, req);
    if (!rec || !rec.id) {
      return res.status(404).json({ ok: false, error: 'order_not_found_or_failed' });
    }
    return res.json({ ok: true, token: rec.id, expiresAt: rec.expiresAt });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/refil/create', async (req, res) => {
  try {
    const orderIdInput = String((req.body && (req.body.order_id || req.body.orderId)) || '').trim();
    if (!orderIdInput) return res.status(400).json({ ok: false, error: 'missing_order_id' });

    const col = await getCollection('checkout_orders');
    const conds = [];
    
    if (/^[0-9a-fA-F]{24}$/.test(orderIdInput)) {
       try { conds.push({ _id: new (require('mongodb').ObjectId)(orderIdInput) }); } catch(_) {}
    }
    const numId = Number(orderIdInput);
    if (!Number.isNaN(numId)) {
        conds.push({ 'fama24h.orderId': numId });
        conds.push({ 'fornecedor_social.orderId': numId });
        conds.push({ 'fama24h.orderId': String(numId) });
        conds.push({ 'fornecedor_social.orderId': String(numId) });
    }
    conds.push({ identifier: orderIdInput });
    conds.push({ 'woovi.identifier': orderIdInput });
    conds.push({ correlationID: orderIdInput });

    const order = await col.findOne({ $or: conds });
    if (!order) return res.status(404).json({ ok: false, error: 'order_not_found', message: 'Pedido não encontrado' });

    let provider = '';
    let externalOrderId = null;
    let apiKey = '';
    let apiUrl = '';

    if (order.fama24h && (order.fama24h.orderId || order.fama24h.id)) {
        provider = 'fama24h';
        externalOrderId = order.fama24h.orderId || order.fama24h.id;
        apiKey = process.env.FAMA24H_API_KEY;
        apiUrl = 'https://fama24h.net/api/v2';
    } else if (order.fornecedor_social && (order.fornecedor_social.orderId || order.fornecedor_social.id)) {
        provider = 'fornecedor_social';
        externalOrderId = order.fornecedor_social.orderId || order.fornecedor_social.id;
        apiKey = process.env.FORNECEDOR_SOCIAL_API_KEY;
        apiUrl = 'https://fornecedorsocial.com/api/v2';
    }

    if (!provider || !externalOrderId) {
        return res.status(400).json({ ok: false, error: 'no_provider_order', message: 'Este pedido não possui registro em fornecedor elegível para refil.' });
    }
    if (!apiKey) {
        return res.status(500).json({ ok: false, error: 'missing_api_key', message: 'Configuração de API ausente no servidor.' });
    }

    const axios = require('axios');
    const params = new URLSearchParams();
    params.append('key', apiKey);
    params.append('action', 'refill');
    params.append('order', String(externalOrderId));

    try {
        console.log(`[Refil] Enviando solicitação para ${provider} (Order: ${externalOrderId})`);
        const resp = await axios.post(apiUrl, params.toString(), {
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: 30000
        });
        const data = resp.data || {};
        
        console.log(`[Refil] Resposta de ${provider}:`, JSON.stringify(data));

        const refillLog = {
            requestedAt: new Date().toISOString(),
            provider,
            externalOrderId,
            response: data,
            status: data.refill ? 'initiated' : (data.error ? 'error' : 'unknown')
        };
        
        await col.updateOne({ _id: order._id }, { $push: { refillHistory: refillLog } });

        if (data.error) {
            return res.status(400).json({ ok: false, error: data.error, message: `Resposta do Fornecedor: ${data.error}` });
        }

        return res.json({
            ok: true,
            message: 'Refil solicitado com sucesso',
            data: {
                refill_id: data.refill,
                status: 'initiated',
                provider_response: data
            }
        });

    } catch (apiErr) {
        console.error(`[Refil] Erro de conexão/API com ${provider}:`, apiErr.message);
        const errMsg = apiErr?.response?.data?.error || apiErr?.message || String(apiErr);
        await col.updateOne({ _id: order._id }, { $push: { refillHistory: {
            requestedAt: new Date().toISOString(),
            provider,
            externalOrderId,
            error: errMsg,
            status: 'failed'
        }}});
        return res.status(500).json({ ok: false, error: 'provider_error', message: `Erro ao comunicar com fornecedor: ${errMsg}` });
    }

  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/refil/history', async (req, res) => {
  try {
    const orderIdInput = String((req.body && (req.body.order_id || req.body.orderId)) || '').trim();
    if (!orderIdInput) return res.status(400).json({ ok: false, error: 'missing_order_id' });

    const col = await getCollection('checkout_orders');
    const conds = [];

    if (/^[0-9a-fA-F]{24}$/.test(orderIdInput)) {
      try { conds.push({ _id: new (require('mongodb').ObjectId)(orderIdInput) }); } catch(_) {}
    }
    const numId = Number(orderIdInput);
    if (!Number.isNaN(numId)) {
      conds.push({ 'fama24h.orderId': numId });
      conds.push({ 'fornecedor_social.orderId': numId });
      conds.push({ 'fama24h.orderId': String(numId) });
      conds.push({ 'fornecedor_social.orderId': String(numId) });
    }
    conds.push({ identifier: orderIdInput });
    conds.push({ 'woovi.identifier': orderIdInput });
    conds.push({ correlationID: orderIdInput });

    const order = await col.findOne({ $or: conds }, { projection: { refillHistory: 1 } });
    if (!order) return res.status(404).json({ ok: false, error: 'order_not_found', message: 'Pedido não encontrado' });

    const historyRaw = Array.isArray(order.refillHistory) ? order.refillHistory : [];
    const history = historyRaw
      .filter((h) => h && (h.status === 'initiated' || (h.response && h.response.refill)))
      .map((h) => ({
        requestedAt: h.requestedAt || null,
        refillId: (h.response && h.response.refill) ? String(h.response.refill) : null,
        status: h.status || 'initiated'
      }))
      .sort((a, b) => String(b.requestedAt || '').localeCompare(String(a.requestedAt || '')))
      .slice(0, 20);

    return res.json({ ok: true, history });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/api/woovi/charge/dev', async (req, res) => {
  try {
    const { correlationID, value, comment, customer, additionalInfo } = req.body || {};
    if (!value || typeof value !== 'number') {
      return res.status(400).json({ error: 'invalid_value' });
    }
    const sanitizeText = (s) => {
      if (typeof s !== 'string') return s;
      return s.replace(/[\u2012-\u2015]/g, '-').replace(/[\uD800-\uDFFF]/g, '').trim();
    };
    const normalizePhone = (s) => {
      const raw = typeof s === 'string' ? s : '';
      const digits = raw.replace(/\D/g, '');
      if (!digits) return '';
      if (raw.trim().startsWith('+')) return `+${digits}`;
      if (digits.startsWith('55')) return `+${digits}`;
      if (digits.length >= 11) return `+55${digits}`;
      return `+${digits}`;
    };
    const addInfoArrRaw = Array.isArray(additionalInfo) ? additionalInfo.map((item) => ({ key: sanitizeText(String(item?.key ?? '')), value: sanitizeText(String(item?.value ?? '')) })) : [];
    const addInfoArr = addInfoArrRaw.filter((it) => typeof it.key === 'string' && it.key.trim().length > 0 && typeof it.value === 'string' && it.value.trim().length > 0).map((it) => ({ key: it.key.trim(), value: it.value.trim() }));
    const addInfo = addInfoArr.reduce((acc, item) => { acc[String(item.key || '')] = String(item.value || ''); return acc; }, {});
    const tipo = addInfo['tipo_servico'] || '';
    const qtd = Number(addInfo['quantidade'] || 0) || 0;

    // Validação de Preço (Dev) - Mesma lógica da produção para garantir consistência
    let validatedPriceCents = null;
    // verifyPrice está disponível no escopo global (require no topo)
    const verification = await verifyPrice(tipo, qtd, addInfoArr, value);
    
    if (verification.isValid) {
        validatedPriceCents = verification.matchedPrice;
    } else {
        return res.status(400).json({ 
            error: 'value_mismatch', 
            message: 'Dev: Valor incorreto. O valor deve bater com o cálculo do sistema.',
            details: verification 
        });
    }

    const instauserFromClient = addInfo['instagram_username'] || '';
    let utms = {};
    try {
      const refUrl = req.get('Referer') || req.headers['referer'] || '';
      const u = new URL(refUrl);
      const p = u.searchParams;
      utms = { source: p.get('utm_source') || '', medium: p.get('utm_medium') || '', campaign: p.get('utm_campaign') || '', term: p.get('utm_term') || '', content: p.get('utm_content') || '', gclid: p.get('gclid') || '', fbclid: p.get('fbclid') || '', ref: refUrl };
    } catch(_) { const refUrl = req.get('Referer') || req.headers['referer'] || ''; utms = { ref: refUrl }; }
    const customerPayload = { name: sanitizeText((customer && customer.name) ? customer.name : 'Cliente Checkout'), phone: normalizePhone((customer && customer.phone) ? customer.phone : '') };
    const createdIso = new Date().toISOString();
    const record = {
      nomeUsuario: null,
      telefone: customerPayload.phone || '',
      correlationID: correlationID || `dev-${Date.now()}`,
      instauser: instauserFromClient,
      criado: createdIso,
      identifier: 'dev',
      status: 'pendente',
      qtd,
      tipo,
      utms,
      valueCents: value,
      expectedValueCents: validatedPriceCents,
      customer: customerPayload,
      additionalInfo: addInfoArr,
      tipoServico: tipo,
      quantidade: qtd,
      instagramUsername: instauserFromClient,
      woovi: { chargeId: null, identifier: 'dev', brCode: null, qrCodeImage: null, status: 'pendente' }
    };
    const col = await getCollection('checkout_orders');
    const insertResult = await col.insertOne(record);
    res.status(200).json({ ok: true, insertedId: insertResult.insertedId, username: instauserFromClient });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
const DEFAULT_COST_SETTINGS = {
  seguidores_mistos: 5.40,
  seguidores_brasileiros: 15.48,
  seguidores_organicos: 35.0,
  curtidas_mistos: 0.75,
  curtidas: 2.0,
  comentarios: 0.3,
  visualizacoes: 0.01
};

app.get('/painel/recuperacao', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');

    // 15 minutes ago
    const cutoffDate = new Date(Date.now() - 15 * 60 * 1000).toISOString();

    const normalizeUsername = (u) => {
      try {
        let s = String(u || '').trim();
        if (!s) return '';
        s = s.replace(/\s+/g, '');
        s = s.replace(/^@/, '');
        s = s.split('#')[0].split('?')[0];
        if (s.includes('/')) {
          const parts = s.split('/').filter(Boolean);
          s = parts.length ? parts[parts.length - 1] : s;
        }
        return String(s || '').toLowerCase().trim();
      } catch (_) {
        return '';
      }
    };

    const extractUsername = (o) => {
      try {
        const paidArr = Array.isArray(o?.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const baseArr = Array.isArray(o?.additionalInfo) ? o.additionalInfo : [];
        return o.instagramUsername
          || o.instauser
          || (o.additionalInfoMapPaid && o.additionalInfoMapPaid.instagram_username)
          || (o.additionalInfoMap && o.additionalInfoMap.instagram_username)
          || paidArr.find(i => i && i.key === 'instagram_username')?.value
          || baseArr.find(i => i && i.key === 'instagram_username')?.value
          || '';
      } catch (_) {
        return '';
      }
    };

    const query = {
      $and: [
        { status: { $ne: 'pago' } },
        { 'woovi.status': { $ne: 'pago' } },
        { createdAt: { $lt: cutoffDate } }
      ]
    };

    // Get pending orders (limit to recent 200 to avoid overload)
    let pendingOrders = await col.find(query).sort({ createdAt: -1 }).limit(200).toArray();

    if (pendingOrders.length > 0) {
      // Logic to filter out orders where the user paid in a subsequent order
      // Extract usernames and phones
      const usernames = pendingOrders
        .map(o => extractUsername(o))
        .filter(u => u && typeof u === 'string')
        .map(u => normalizeUsername(u));

      const phones = pendingOrders
        .map(o => (o.customer && (o.customer.phone || o.customer.phone_number)))
        .filter(p => p && typeof p === 'string');
      
      const uniqueUsernames = [...new Set(usernames)].filter(Boolean);
      
      // Escape special characters for Regex
      const escapeRegExp = (string) => {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      };

      if (uniqueUsernames.length > 0) {
         // Find any PAID orders for these users created after the oldest pending order in this batch
         const oldestPending = pendingOrders[pendingOrders.length - 1].createdAt;
         
         const usernameRegexes = uniqueUsernames.map(u => new RegExp(escapeRegExp(u), 'i'));

         const paidQuery = {
            $and: [
              {
                $or: [
                  { status: 'pago' },
                  { 'woovi.status': 'pago' }
                ]
              },
              {
                $or: [
                   { instagramUsername: { $in: usernameRegexes } },
                   { instauser: { $in: usernameRegexes } },
                   { 'additionalInfoMap.instagram_username': { $in: usernameRegexes } },
                   { 'additionalInfoMapPaid.instagram_username': { $in: usernameRegexes } }
                ]
              },
              { createdAt: { $gt: oldestPending } }
            ]
         };

         const paidOrders = await col.find(paidQuery).project({ instagramUsername: 1, instauser: 1, additionalInfoMap: 1, additionalInfoMapPaid: 1, additionalInfoPaid: 1, additionalInfo: 1, createdAt: 1 }).toArray();
         const paidLatestByUser = new Map();
         for (const paid of paidOrders) {
           const u = normalizeUsername(extractUsername(paid));
           if (!u) continue;
           const ms = new Date(paid.createdAt).getTime();
           if (!Number.isFinite(ms)) continue;
           const prev = paidLatestByUser.get(u) || 0;
           if (ms > prev) paidLatestByUser.set(u, ms);
         }
         
         // Filter pending orders
         pendingOrders = pendingOrders.filter(pending => {
            // Normalize pending user
            let pUser = normalizeUsername(extractUsername(pending));
            if (!pUser) return true; // Keep if no username to match

            const pendingMs = new Date(pending.createdAt).getTime();
            const paidLatestMs = paidLatestByUser.get(pUser) || 0;
            const hasNewerPaid = Number.isFinite(pendingMs) && paidLatestMs > pendingMs;

            return !hasNewerPaid;
         });
      }
    }

    const seen = new Set();
    pendingOrders = pendingOrders.filter(o => {
      const key = normalizeUsername(extractUsername(o));
      if (!key) return true;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    res.render('painel_recuperacao', { orders: pendingOrders });
  } catch (err) {
    console.error('Error in /painel/recuperacao:', err);
    res.status(500).send('Internal Server Error');
  }
});

app.get('/painel/privados', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');

    const { startDate, endDate, orderSent, noContact, contactStatus, searchPhone, archived } = req.query;

    const query = {
        $and: [
            {
                $or: [
                    { status: 'pago' },
                    { 'woovi.status': 'pago' }
                ]
            },
            {
                $or: [
                    { isPrivate: true },
                    { 'additionalInfoMap.isPrivate': true },
                    { 'additionalInfoMap.isPrivate': "true" }
                ]
            }
        ]
    };

    // Date Filters
    if (startDate || endDate) {
        const dateFilter = {};
        // Adjust for Brazil Time (UTC-3)
        // User input 2023-10-27 implies 00:00 SP, which is 03:00 UTC
        if (startDate) {
            const start = new Date(`${startDate}T00:00:00.000Z`);
            start.setUTCHours(start.getUTCHours() + 3);
            dateFilter.$gte = start.toISOString();
        }
        if (endDate) {
            const end = new Date(`${endDate}T23:59:59.999Z`);
            end.setUTCHours(end.getUTCHours() + 3);
            dateFilter.$lte = end.toISOString();
        }
        query.$and.push({ createdAt: dateFilter });
    }

    // Status Filters
    if (orderSent === 'yes') {
        query.$and.push({ orderSent: true });
    } else if (orderSent === 'no') {
        query.$and.push({ $or: [{ orderSent: false }, { orderSent: { $exists: false } }] });
    }

    if (noContact === 'yes') {
        query.$and.push({ noContact: true });
    } else if (noContact === 'no') {
        query.$and.push({ $or: [{ noContact: false }, { noContact: { $exists: false } }] });
    }

    // Contact Count Filters
    if (contactStatus === 'yes') {
        query.$and.push({ contactCount: { $gt: 0 } });
    } else if (contactStatus === 'no') {
        query.$and.push({ $or: [{ contactCount: 0 }, { contactCount: { $exists: false } }] });
    }

    // Phone Search Filter
    if (searchPhone) {
        const rawSearch = String(searchPhone || '').trim();
        const digits = rawSearch.replace(/\D/g, '');
        const phoneRegex = digits
          ? new RegExp(digits.split('').join('\\D*'), 'i')
          : new RegExp(rawSearch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
        query.$and.push({
            $or: [
                { 'customer.phone': { $regex: phoneRegex } },
                { 'customer.phone_number': { $regex: phoneRegex } },
                { 'additionalInfoMap.phone': { $regex: phoneRegex } },
                { 'additionalInfoPaid': { $elemMatch: { key: 'phone', value: { $regex: phoneRegex } } } }
            ]
        });
    }

    if (archived === 'yes') {
        query.$and.push({ archived: true });
    } else if (archived !== 'all') {
        query.$and.push({ $or: [{ archived: false }, { archived: { $exists: false } }] });
    }

    const unknownTokens = ['', null, 'unknown', 'unknow', 'null'];
    const noFamaId = { $or: [{ 'fama24h.orderId': { $exists: false } }, { 'fama24h.orderId': { $in: unknownTokens } }] };
    const noFsId = { $or: [{ 'fornecedor_social.orderId': { $exists: false } }, { 'fornecedor_social.orderId': { $in: unknownTokens } }] };
    query.$and.push({ $or: [{ orderSent: { $ne: true } }, { $and: [{ orderSent: true }, noFamaId, noFsId] }] });
    
    const orders = await col.find(query).sort({ createdAt: -1 }).limit(500).toArray();
    res.render('painel_privados', { orders, page: 'privados', startDate, endDate, orderSent, noContact, contactStatus, searchPhone, archived });
  } catch (err) {
    console.error(err);
    res.status(500).send('Erro interno');
  }
});

app.get('/painel/gerenciamento-seguidores', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const monitorCol = await getCollection('followers_monitor');
    const validatedCol = await getCollection('validated_insta_users');
    const tlCol = await getCollection('temporary_links');
    const settingsCol = await getCollection('settings');

    const followersMgmtSettingsDoc = await settingsCol.findOne({ _id: 'followers_mgmt_settings' }, { projection: { _id: 0, values: 1 } });
    const followersMgmtSettingsRaw = (followersMgmtSettingsDoc && followersMgmtSettingsDoc.values && typeof followersMgmtSettingsDoc.values === 'object')
      ? followersMgmtSettingsDoc.values
      : {};
    const followersMgmtSettings = {
      dailyLimit: Math.min(1000, Math.max(1, parseInt(String(followersMgmtSettingsRaw.dailyLimit || '50'), 10) || 50)),
      order: (String(followersMgmtSettingsRaw.order || 'newest').toLowerCase() === 'oldest') ? 'oldest' : 'newest'
    };

    const paidQuery = { $or: [{ status: 'pago' }, { 'woovi.status': 'pago' }] };
    const followersQuery = {
      $and: [
        paidQuery,
        {
          $or: [
            { additionalInfoPaid: { $elemMatch: { key: 'categoria_servico', value: { $regex: '^seguidores$', $options: 'i' } } } },
            { 'additionalInfoMapPaid.categoria_servico': { $regex: '^seguidores$', $options: 'i' } },
            { 'additionalInfoMap.categoria_servico': { $regex: '^seguidores$', $options: 'i' } },
            { tipo: { $regex: 'seguidores', $options: 'i' } },
            { tipoServico: { $regex: 'seguidores', $options: 'i' } }
          ]
        }
      ]
    };

    const orders = await col.find(followersQuery, {
      projection: {
        _id: 1,
        createdAt: 1,
        paidAt: 1,
        woovi: 1,
        tipo: 1,
        tipoServico: 1,
        qtd: 1,
        quantidade: 1,
        instagramUsername: 1,
        instauser: 1,
        fama24h: 1,
        fornecedor_social: 1,
        additionalInfo: 1,
        additionalInfoPaid: 1,
        additionalInfoMap: 1,
        additionalInfoMapPaid: 1,
        initialFollowersCount: 1,
        initialFollowersCheckedAt: 1
      }
    }).sort({ createdAt: -1 }).limit(4000).toArray();

    const extractInfoAny = (o, key) => {
      try {
        if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
        if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return o.additionalInfoMap[key];
        const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const itemPaid = arrPaid.find(i => i && i.key === key);
        if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
        const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
        const item = arr.find(i => i && i.key === key);
        if (item && typeof item.value !== 'undefined') return item.value;
      } catch (_) {}
      return '';
    };

    const normalizeUsernameKey = (u) => {
      const s0 = String(u || '').trim();
      if (!s0) return '';
      const s1 = s0.startsWith('@') ? s0.slice(1) : s0;
      return s1.toLowerCase().trim();
    };

    const resolveUsername = (o) => {
      const candidates = [
        extractInfoAny(o, 'instagram_username'),
        o.instagramUsername,
        o.instauser
      ];
      for (const c of candidates) {
        const k = normalizeUsernameKey(c);
        if (k) return k;
      }
      return '';
    };

    const resolveDateMs = (o) => {
      const dateStr = o.woovi?.paidAt || o.paidAt || o.createdAt || null;
      const t = dateStr ? new Date(dateStr).getTime() : 0;
      return Number.isFinite(t) ? t : 0;
    };

    const resolveQty = (o) => {
      const direct = Number(o.quantidade || o.qtd || 0) || 0;
      if (direct) return direct;
      const q = extractInfoAny(o, 'quantidade');
      const n = Number(String(q || '').replace(/[^\d]/g, '')) || 0;
      return n;
    };

    const resolveOrderBumps = (o) => {
      const raw = extractInfoAny(o, 'order_bumps');
      return String(raw || '');
    };

    const getUpgradeAddQtd = (tipo, base) => {
      try {
        const t0 = String(tipo || '').toLowerCase().trim();
        const t = t0.startsWith('seguidores_') ? t0.replace(/^seguidores_/, '') : t0;
        const b = Number(base) || 0;
        if (!b) return 0;
        if (t === 'organicos' && b === 50) return 50;
        if ((t === 'brasileiros' || t === 'organicos') && b === 1000) return 1000;
        const map = { 50: 50, 150: 150, 300: 200, 500: 200, 700: 300, 1000: 1000, 1200: 800, 2000: 1000, 3000: 1000, 4000: 1000, 5000: 2500, 7500: 2500, 10000: 5000 };
        return map[b] || 0;
      } catch (_) {
        return 0;
      }
    };

    const resolveQtyWithUpgrade = (o) => {
      const baseQty = resolveQty(o);
      if (!baseQty) return baseQty;
      const bumpsStr = String(resolveOrderBumps(o) || '').toLowerCase();
      const m = bumpsStr.match(/(?:^|;)\s*upgrade\s*:\s*(\d+)/i);
      const upgradeQty = m ? Number(m[1]) : 0;
      if (!upgradeQty) return baseQty;
      const tipo = String(o.tipo || o.tipoServico || '').trim();
      const add = getUpgradeAddQtd(tipo, baseQty);
      return baseQty + add;
    };

    const resolveTipo = (o) => {
      const t = String(extractInfoAny(o, 'tipo_servico') || o.tipo || o.tipoServico || '').trim();
      return t;
    };

    const resolveFornecedor = (o) => {
      const isUnknown = (v) => {
        const s = String(v ?? '').toLowerCase().trim();
        return s === '' || s === 'unknown' || s === 'unknow' || s === 'null' || s === 'undefined';
      };

      const getByPath = (obj, path) => {
        try {
          return path.split('.').reduce((acc, k) => (acc && typeof acc === 'object') ? acc[k] : undefined, obj);
        } catch (_) {
          return undefined;
        }
      };

      const pickProviderOrderId = (providerObj) => {
        if (!providerObj || typeof providerObj !== 'object') return '';
        const candidates = [
          'orderId',
          'orderID',
          'orderid',
          'order_id',
          'id',
          'pedidoId',
          'pedido_id',
          'data.orderId',
          'data.orderID',
          'data.order_id',
          'data.id',
          'response.orderId',
          'response.orderID',
          'response.order_id',
          'response.id',
          'payload.orderId',
          'payload.orderID',
          'payload.order_id',
          'payload.id',
          'statusPayload.orderId',
          'statusPayload.orderID',
          'statusPayload.order_id',
          'statusPayload.id'
        ];
        for (const p of candidates) {
          const v = getByPath(providerObj, p);
          if (typeof v === 'number' && Number.isFinite(v)) return String(v);
          if (typeof v === 'string' && !isUnknown(v)) return String(v).trim();
        }
        return '';
      };

      const fsOid = pickProviderOrderId(o && o.fornecedor_social ? o.fornecedor_social : null);
      if (fsOid) return 'Fornecedor Social';
      const famaOid = pickProviderOrderId(o && o.fama24h ? o.fama24h : null);
      if (famaOid) return 'Fama24h';
      return '-';
    };

    const resolveFornecedorOrderId = (o) => {
      const isUnknown = (v) => {
        const s = String(v ?? '').toLowerCase().trim();
        return s === '' || s === 'unknown' || s === 'unknow' || s === 'null' || s === 'undefined';
      };

      const getByPath = (obj, path) => {
        try {
          return path.split('.').reduce((acc, k) => (acc && typeof acc === 'object') ? acc[k] : undefined, obj);
        } catch (_) {
          return undefined;
        }
      };

      const pickProviderOrderId = (providerObj) => {
        if (!providerObj || typeof providerObj !== 'object') return '';
        const candidates = [
          'orderId',
          'orderID',
          'orderid',
          'order_id',
          'id',
          'pedidoId',
          'pedido_id',
          'data.orderId',
          'data.orderID',
          'data.order_id',
          'data.id',
          'response.orderId',
          'response.orderID',
          'response.order_id',
          'response.id',
          'payload.orderId',
          'payload.orderID',
          'payload.order_id',
          'payload.id',
          'statusPayload.orderId',
          'statusPayload.orderID',
          'statusPayload.order_id',
          'statusPayload.id'
        ];
        for (const p of candidates) {
          const v = getByPath(providerObj, p);
          if (typeof v === 'number' && Number.isFinite(v)) return String(v);
          if (typeof v === 'string' && !isUnknown(v)) return String(v).trim();
        }
        return '';
      };

      const fs = o && o.fornecedor_social ? o.fornecedor_social : null;
      const fama = o && o.fama24h ? o.fama24h : null;

      const fsOid = pickProviderOrderId(fs);
      if (fsOid) return fsOid;
      const famaOid = pickProviderOrderId(fama);
      if (famaOid) return famaOid;
      return '';
    };

    const resolveIdentifier = (o) => {
      const id = o && o.woovi && (o.woovi.identifier || o.woovi.chargeId || '');
      const raw = id || o.identifier || '';
      return String(raw || '').trim();
    };

    const parseOptionalNumber = (v) => {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    const pageRaw = parseInt(String(req.query.page || '1'), 10);
    const pageSizeRaw = parseInt(String(req.query.pageSize || '50'), 10);
    const pageSize = Math.min(200, Math.max(10, Number.isFinite(pageSizeRaw) ? pageSizeRaw : 50));
    const page = Math.max(1, Number.isFinite(pageRaw) ? pageRaw : 1);
    const minPct = (String(req.query.minPct || '').trim() !== '') ? parseOptionalNumber(req.query.minPct) : null;
    const maxPct = (String(req.query.maxPct || '').trim() !== '') ? parseOptionalNumber(req.query.maxPct) : null;
    const sortBy = String(req.query.sortBy || 'lastPurchaseAtMs').trim();
    const sortDirRaw = String(req.query.sortDir || 'desc').trim().toLowerCase();
    const sortDir = (sortDirRaw === 'asc' || sortDirRaw === 'desc') ? sortDirRaw : 'desc';
    const q = String(req.query.q || '').trim();
    const qType = String(req.query.qType || 'username').trim();
    const filledOnly = String(req.query.filled || '').trim() === '1';
    const lifetimeOnly = String(req.query.lifetime || '').trim() === '1';

    const usernames = Array.from(new Set(orders.map(resolveUsername).filter(Boolean)));
    const monitorDocs = usernames.length ? await monitorCol.find({ username: { $in: usernames } }, { projection: { _id: 0 } }).toArray() : [];
    const monitorMap = new Map(monitorDocs.map(d => [String(d.username || '').toLowerCase(), d]));
    const validatedDocs = usernames.length ? await validatedCol.find({ username: { $in: usernames } }, { projection: { _id: 0, username: 1, followersCount: 1, checkedAt: 1 } }).toArray() : [];
    const validatedMap = new Map(validatedDocs.map(d => [String(d.username || '').toLowerCase(), d]));

    const backfillOps = [];
    const dayMs = 24 * 60 * 60 * 1000;
    const nowMs = Date.now();
    const brtOffsetMs = 3 * 60 * 60 * 1000;
    const brtYmdFromMs = (ms) => {
      const d = new Date(Number(ms || 0) - brtOffsetMs);
      return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
    };
    const dayNumFromYmd = (y, m, d) => Math.floor(Date.UTC(Number(y), Number(m) - 1, Number(d)) / dayMs);
    const nowDayNumBrt = dayNumFromYmd(...Object.values(brtYmdFromMs(nowMs)));
    const daysInMonth = (y, m) => new Date(Date.UTC(Number(y), Number(m), 0)).getUTCDate();
    const addMonthsEndOfDayBrt = (baseMs, monthsToAdd) => {
      const base = brtYmdFromMs(baseMs);
      let y = base.y;
      let m = base.m + Number(monthsToAdd || 0);
      while (m > 12) { y += 1; m -= 12; }
      while (m < 1) { y -= 1; m += 12; }
      const maxDay = daysInMonth(y, m);
      const d = Math.min(base.d, maxDay);
      const utcMs = Date.UTC(y, m - 1, d, 23, 59, 59, 999) + brtOffsetMs;
      return { iso: new Date(utcMs).toISOString(), y, m, d };
    };
    const warrantyFromBumpsStr = (bumpsStr) => {
      const s = String(bumpsStr || '').trim();
      const bumpQtyMap = {};
      if (s) {
        const parts = s.split(';');
        for (const raw of parts) {
          const part = String(raw || '').trim();
          if (!part) continue;
          const segs = part.split(':');
          const key = String(segs[0] || '').trim().toLowerCase();
          if (!key) continue;
          const qtyRaw = segs.length > 1 ? String(segs[1] || '').trim() : '';
          const qtyParsed = qtyRaw ? Number(qtyRaw) : 1;
          const qty = Number.isFinite(qtyParsed) ? qtyParsed : 1;
          bumpQtyMap[key] = Number(bumpQtyMap[key] || 0) + qty;
        }
      }
      const hasLifetime = (() => {
        const get = (k) => Number(bumpQtyMap[k] || 0) || 0;
        if (get('warranty_lifetime') > 0 || get('warranty_life') > 0) return true;
        if (get('warranty60') > 0 || get('warranty_60') > 0 || get('warrenty60') > 0 || get('warrenty_60') > 0) return true;
        if (get('warrenty') > 0) return true;
        return false;
      })();
      if (hasLifetime) return { isLifetime: true, months: null, mode: 'life', days: null };
      return { isLifetime: false, months: 1, mode: '30', days: 30 };
    };
    const isEligibleRefilTipo = (tipoRaw) => {
      const t = String(tipoRaw || '').toLowerCase();
      return t.includes('mistos') || t.includes('brasileir');
    };

    const isFollowersServico = (tipoRaw) => {
      const t = String(tipoRaw || '').toLowerCase();
      if (!t) return true;
      if (t.includes('curtida') || t.includes('like')) return false;
      if (t.includes('visualiza') || t.includes('view')) return false;
      return true;
    };

    const computeWarrantyFromOrder = (o) => {
      try {
        const orderBaseMs = (() => {
          const toMs = (v) => {
            try {
              if (!v) return 0;
              if (typeof v === 'number' && Number.isFinite(v)) return v;
              const t = new Date(v).getTime();
              return Number.isFinite(t) ? t : 0;
            } catch (_) {
              return 0;
            }
          };
          const a = toMs(o?.woovi?.paidAt);
          if (a) return a;
          const b = toMs(o?.paidAt);
          if (b) return b;
          const c = toMs(o?.createdAt);
          if (c) return c;
          return 0;
        })();

        const w = warrantyFromBumpsStr(resolveOrderBumps(o) || '');
        if (w.isLifetime) return { isLifetime: true, daysLeft: null, expiresAt: '2099-12-31T23:59:59.999Z' };
        const base = orderBaseMs || nowMs;
        const exp = addMonthsEndOfDayBrt(base, Number(w.months || 1));
        const expDayNum = dayNumFromYmd(exp.y, exp.m, exp.d);
        const left = expDayNum - nowDayNumBrt;
        return { isLifetime: false, daysLeft: Math.max(0, left), expiresAt: exp.iso };
      } catch (_) {
        return { isLifetime: false, daysLeft: null, expiresAt: null };
      }
    };

    const orderInfoById = new Map();
    for (const o of orders) {
      const idStr = o && o._id ? String(o._id) : '';
      if (!idStr) continue;
      const tipo = resolveTipo(o);
      const eligible = isEligibleRefilTipo(tipo);
      const baseMs = resolveDateMs(o);
      const w = warrantyFromBumpsStr(resolveOrderBumps(o) || '');
      orderInfoById.set(idStr, { baseMs: Number(baseMs || 0), eligible, warranty: w });
    }

    const allRows = orders.map((o) => {
      const username = resolveUsername(o);
      const contracted = resolveQtyWithUpgrade(o);
      const dateMs = resolveDateMs(o);
      const mon = username ? (monitorMap.get(username) || null) : null;
      const vu = username ? (validatedMap.get(username) || null) : null;

      const currentFollowersCount = mon && typeof mon.followersCount === 'number' ? mon.followersCount : null;
      const currentCheckedAt = mon ? (mon.checkedAt || null) : null;
      const isPrivate = mon ? (typeof mon.isPrivate === 'boolean' ? mon.isPrivate : null) : null;

      let initialFollowersCount = (typeof o.initialFollowersCount === 'number') ? o.initialFollowersCount : null;
      let initialFollowersCheckedAt = o.initialFollowersCheckedAt ? String(o.initialFollowersCheckedAt) : null;

      if (initialFollowersCount == null && vu && typeof vu.followersCount === 'number') {
        initialFollowersCount = vu.followersCount;
        initialFollowersCheckedAt = vu.checkedAt || null;
      } else if (initialFollowersCount == null && mon && typeof mon.followersCount === 'number') {
        initialFollowersCount = mon.followersCount;
        initialFollowersCheckedAt = mon.checkedAt || null;
      }

      if ((typeof o.initialFollowersCount !== 'number') && initialFollowersCount != null) {
        backfillOps.push({
          updateOne: {
            filter: { _id: o._id, $or: [{ initialFollowersCount: { $exists: false } }, { initialFollowersCount: null }] },
            update: { $set: { initialFollowersCount: Number(initialFollowersCount), initialFollowersCheckedAt: initialFollowersCheckedAt || new Date().toISOString() } }
          }
        });
      }

      const resultado = (initialFollowersCount != null && contracted) ? (Number(initialFollowersCount) + Number(contracted)) : null;
      const diffAbs = (resultado != null && currentFollowersCount != null) ? (Number(resultado) - Number(currentFollowersCount)) : null;
      const diffPct = (diffAbs != null && resultado) ? ((Number(diffAbs) / Number(resultado)) * 100) : null;
      const tipo = resolveTipo(o);
      const warrantyBase = (isEligibleRefilTipo(tipo) ? computeWarrantyFromOrder(o) : { isLifetime: false, daysLeft: null, expiresAt: null });

      return {
        orderId: o._id,
        orderIdentifier: resolveIdentifier(o),
        fornecedorOrderId: resolveFornecedorOrderId(o),
        username,
        tipo,
        fornecedor: resolveFornecedor(o),
        contracted: Number(contracted || 0),
        initialFollowersCount,
        initialFollowersCheckedAt,
        resultado,
        currentFollowersCount,
        currentCheckedAt,
        isPrivate,
        lastPurchaseAtMs: dateMs,
        diffAbs,
        diffPct,
        refilIsLifetime: !!warrantyBase.isLifetime,
        refilDaysLeft: (typeof warrantyBase.daysLeft === 'number') ? warrantyBase.daysLeft : null,
        refilExpiresAt: warrantyBase.expiresAt || null
      };
    }).filter(r => {
      if (!r.username) return false;
      if (!(r.contracted > 0)) return false;
      if (!isFollowersServico(r.tipo)) return false;
      if (!String(r.fornecedorOrderId || '').trim()) return false;
      return true;
    }).sort((a, b) => Number(b.lastPurchaseAtMs || 0) - Number(a.lastPurchaseAtMs || 0));

    const mergeWarranty = (a, b, allowLifetime) => {
      if (!a) return b;
      if (!b) return a;
      if (allowLifetime && (a.isLifetime || b.isLifetime)) return { isLifetime: true, daysLeft: null, expiresAt: a.expiresAt || b.expiresAt || null };
      const aDays = (typeof a.daysLeft === 'number') ? a.daysLeft : null;
      const bDays = (typeof b.daysLeft === 'number') ? b.daysLeft : null;
      if (aDays == null) return b;
      if (bDays == null) return a;
      return (bDays > aDays) ? b : a;
    };

    try {
      const orderIdStrs = Array.from(new Set(allRows.map(r => (r && r.orderId) ? String(r.orderId) : '').filter(Boolean)));
      if (orderIdStrs.length) {
        const orderIdSet = new Set(orderIdStrs);
        const tlDocs = await tlCol.find(
          { purpose: 'refil', $or: [{ orderId: { $in: orderIdStrs } }, { orders: { $in: orderIdStrs } }] },
          { projection: { _id: 0, orderId: 1, orders: 1, createdAt: 1, expiresAt: 1, warrantyMode: 1, warrantyDays: 1 } }
        ).toArray();

        const safeDateMs = (iso) => {
          try {
            const t = new Date(iso).getTime();
            return Number.isFinite(t) ? t : 0;
          } catch (_) {
            return 0;
          }
        };

        const byOrder = new Map();
        for (const d of (tlDocs || [])) {
          const linked = new Set();
          if (d?.orderId) linked.add(String(d.orderId));
          const arr = Array.isArray(d?.orders) ? d.orders : [];
          for (const x of arr) { if (x) linked.add(String(x)); }
          let best = null;
          let bestId = '';
          for (const oid of linked) {
            const info = orderInfoById.get(String(oid));
            if (!info || !info.eligible || !info.baseMs) continue;
            if (!best || info.baseMs > best.baseMs) { best = info; bestId = String(oid); }
          }
          if (!best || !bestId) continue;
          const linkMode = String(d?.warrantyMode || '').trim().toLowerCase();
          const linkExpMs = safeDateMs(d?.expiresAt);
          const anyLifetime = (() => {
            for (const oid of linked) {
              const info = orderInfoById.get(String(oid));
              if (info && info.eligible && info.warranty && info.warranty.isLifetime) return true;
            }
            return false;
          })();
          const computed = anyLifetime
            ? { isLifetime: true, daysLeft: null, expiresAt: '2099-12-31T23:59:59.999Z' }
            : (linkExpMs && linkMode !== 'life' && linkExpMs < safeDateMs('2099-01-01T00:00:00.000Z')
              ? (() => {
                const ymd = brtYmdFromMs(linkExpMs);
                const expDayNum = dayNumFromYmd(ymd.y, ymd.m, ymd.d);
                const left = expDayNum - nowDayNumBrt;
                return { isLifetime: false, daysLeft: Math.max(0, left), expiresAt: new Date(linkExpMs).toISOString() };
              })()
              : (() => {
                const w = best.warranty || { isLifetime: false, months: 1 };
                if (w.isLifetime) return { isLifetime: true, daysLeft: null, expiresAt: '2099-12-31T23:59:59.999Z' };
                const exp = addMonthsEndOfDayBrt(best.baseMs, Number(w.months || 1));
                const expDayNum = dayNumFromYmd(exp.y, exp.m, exp.d);
                const left = expDayNum - nowDayNumBrt;
                return { isLifetime: false, daysLeft: Math.max(0, left), expiresAt: exp.iso };
              })());
          for (const oid of linked) {
            if (!oid) continue;
            if (!orderIdSet.has(oid)) continue;
            const info = orderInfoById.get(String(oid));
            if (!info || !info.eligible) continue;
            const allowLifetime = !!(info?.warranty?.isLifetime);
            byOrder.set(oid, mergeWarranty(byOrder.get(oid), computed, allowLifetime));
          }
        }
        for (const r of allRows) {
          const oid = r && r.orderId ? String(r.orderId) : '';
          if (!oid) continue;
          const info = byOrder.get(oid);
          if (!info) continue;
          const orderInfo = orderInfoById.get(oid);
          const allowLifetime = !!(r.refilIsLifetime || (orderInfo && orderInfo.warranty && orderInfo.warranty.isLifetime));
          const merged = mergeWarranty(
            { isLifetime: !!r.refilIsLifetime, daysLeft: (typeof r.refilDaysLeft === 'number' ? r.refilDaysLeft : null), expiresAt: r.refilExpiresAt || null },
            info,
            allowLifetime
          );
          r.refilIsLifetime = !!merged.isLifetime;
          r.refilDaysLeft = (typeof merged.daysLeft === 'number') ? merged.daysLeft : null;
          r.refilExpiresAt = merged.expiresAt || null;
        }
      }
    } catch (_) {}

    if (backfillOps.length) {
      try {
        await col.bulkWrite(backfillOps, { ordered: false });
      } catch (_) {}
    }

    const qNorm = String(q || '').trim().toLowerCase();
    const qTypeNorm = String(qType || 'username').trim().toLowerCase();

    const filtered = allRows.filter((r) => {
      if (filledOnly) {
        if (r.currentFollowersCount == null) return false;
        if (r.resultado == null) return false;
      }
      if (lifetimeOnly) {
        if (r.refilIsLifetime !== true) return false;
      }

      if (qNorm) {
        if (qTypeNorm === 'orderid') {
          const fId = String(r.fornecedorOrderId || '').toLowerCase();
          if (!fId.includes(qNorm)) return false;
        } else {
          const u = String(r.username || '').toLowerCase();
          if (!u.includes(qNorm)) return false;
        }
      }

      if (minPct == null && maxPct == null) return true;
      if (r.diffPct == null) return false;
      if (minPct != null && r.diffPct < minPct) return false;
      if (maxPct != null && r.diffPct > maxPct) return false;
      return true;
    });

    const cmpNum = (a, b) => {
      const aa = (a == null) ? null : Number(a);
      const bb = (b == null) ? null : Number(b);
      const aOk = Number.isFinite(aa);
      const bOk = Number.isFinite(bb);
      if (!aOk && !bOk) return 0;
      if (!aOk) return 1;
      if (!bOk) return -1;
      return aa - bb;
    };

    const sortKey = (function () {
      const s = String(sortBy || '').trim();
      if (s === 'diffPct') return 'diffPct';
      if (s === 'lastPurchaseAtMs') return 'lastPurchaseAtMs';
      return 'lastPurchaseAtMs';
    })();

    filtered.sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1;
      const primary = cmpNum(a[sortKey], b[sortKey]) * dir;
      if (primary) return primary;
      const fallback = cmpNum(a.lastPurchaseAtMs, b.lastPurchaseAtMs) * -1;
      if (fallback) return fallback;
      return String(a.username || '').localeCompare(String(b.username || ''), 'pt-BR', { sensitivity: 'base' });
    });

    const totalRows = filtered.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
    const safePage = Math.min(totalPages, page);
    const start = (safePage - 1) * pageSize;
    const pageRows = filtered.slice(start, start + pageSize);

    res.render('painel', {
      view: 'gerenciamento_seguidores',
      followersOrders: pageRows,
      pagination: { page: safePage, pageSize, totalRows, totalPages },
      filters: { minPct, maxPct, q, qType, filled: filledOnly, lifetime: lifetimeOnly, sortBy: sortKey, sortDir },
      followersMgmtSettings,
      period: 'all'
    });
  } catch (err) {
    console.error('Erro em /painel/gerenciamento-seguidores:', err);
    res.status(500).send('Erro interno');
  }
});

app.get('/painel/vendas-whatsapp', requireAdmin, async (req, res) => {
  try {
    return res.render('painel', { view: 'vendas_whatsapp', period: 'all' });
  } catch (err) {
    console.error('Erro em /painel/vendas-whatsapp:', err);
    return res.status(500).send('Erro interno');
  }
});

app.get('/api/painel/whatsapp-sales', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const limitRaw = Number(req.query?.limit || 50) || 50;
    const limit = Math.min(200, Math.max(1, Math.floor(limitRaw)));
    const purchaseRaw = String(req.query?.purchase || '').trim();

    const wppBaseQuery = { $or: [{ saleChannel: 'whatsapp' }, { isWhatsappSale: true }, { 'additionalInfoMapPaid.saleChannel': 'whatsapp' }, { 'additionalInfoMap.saleChannel': 'whatsapp' }] };
    let query = wppBaseQuery;

    if (purchaseRaw) {
      const safe = purchaseRaw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const rx = new RegExp(safe, 'i');
      const digits = purchaseRaw.replace(/\D/g, '');
      const usernameRaw = purchaseRaw.replace(/^@+/, '').trim();
      const orConds = [
        { identifier: rx },
        { correlationID: rx },
        { instagramUsername: rx },
        { instauser: rx },
        { 'additionalInfoMapPaid.instagram_username': rx },
        { 'additionalInfoMapPaid.phone': rx },
        { 'additionalInfoPaid': { $elemMatch: { key: 'instagram_username', value: rx } } },
        { 'additionalInfoPaid': { $elemMatch: { key: 'phone', value: rx } } }
      ];

      if (digits && digits.length >= 6) {
        const digitsRx = new RegExp(digits.split('').join('\\D*'), 'i');
        orConds.push({ 'customer.phone': { $regex: digitsRx } });
      }

      if (/^[0-9a-fA-F]{24}$/.test(purchaseRaw)) {
        try {
          const { ObjectId } = require('mongodb');
          orConds.push({ _id: new ObjectId(purchaseRaw) });
        } catch (_) {}
      }

      if (usernameRaw) {
        const userSafe = usernameRaw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const userRx = new RegExp(userSafe, 'i');
        orConds.push({ instagramUsername: userRx });
        orConds.push({ instauser: userRx });
        orConds.push({ 'additionalInfoMapPaid.instagram_username': userRx });
      }

      query = { $and: [wppBaseQuery, { $or: orConds }] };
    }

    const docs = await col.find(
      query,
      { projection: { _id: 1, createdAt: 1, paidAt: 1, woovi: 1, status: 1, customer: 1, instagramUsername: 1, instauser: 1, tipo: 1, tipoServico: 1, qtd: 1, quantidade: 1, additionalInfoMapPaid: 1, additionalInfoPaid: 1 } }
    ).sort({ createdAt: -1, _id: -1 }).limit(limit).toArray();
    res.json({ ok: true, sales: docs });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post('/api/painel/whatsapp-sales', requireAdmin, async (req, res) => {
  try {
    const body = req.body || {};
    const phoneRaw = String(body.phone || '').trim();
    const phoneDigits = phoneRaw.replace(/\D/g, '');
    const phone = phoneDigits ? (`+55${phoneDigits.replace(/^55/, '')}`) : '';
    const usernameRaw = String(body.username || body.instauser || body.instagram_username || '').trim();
    const username = usernameRaw.replace(/^@+/, '').trim();
    const tipoRaw = String(body.tipo || body.tipoServico || body.serviceType || '').trim();
    const tipo = tipoRaw ? tipoRaw : '';
    const qty = Math.max(0, Math.floor(Number(body.quantidade || body.qtd || 0) || 0));
    const parseMoneyToCents = (v) => {
      const raw = String(v == null ? '' : v).trim();
      if (!raw) return 0;
      let s = raw.replace(/[^\d,.\-]/g, '');
      const hasComma = s.includes(',');
      const hasDot = s.includes('.');
      if (hasComma && hasDot) {
        if (s.lastIndexOf(',') > s.lastIndexOf('.')) {
          s = s.replace(/\./g, '').replace(/,/g, '.');
        } else {
          s = s.replace(/,/g, '');
        }
      } else if (hasComma && !hasDot) {
        s = s.replace(/,/g, '.');
      }
      const n = Number(s);
      if (!Number.isFinite(n) || n <= 0) return 0;
      return Math.round(n * 100);
    };
    const valueCents = parseMoneyToCents(body.value || body.valor || 0);

    if (!phone || phone.replace(/\D/g, '').length < 10) return res.status(400).json({ ok: false, error: 'Telefone inválido' });
    if (!username) return res.status(400).json({ ok: false, error: 'Perfil inválido' });
    if (!tipo) return res.status(400).json({ ok: false, error: 'Tipo inválido' });
    if (!qty) return res.status(400).json({ ok: false, error: 'Quantidade inválida' });

    const now = new Date();
    const nowIso = now.toISOString();
    const rand = Math.random().toString(36).slice(2, 8);
    const identifier = `wpp_${Date.now()}_${rand}`;
    const correlationID = `wpp_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;

    const additionalInfoMapPaid = {
      phone: phoneDigits.replace(/^55/, ''),
      instagram_username: username,
      categoria_servico: 'seguidores',
      tipo_servico: tipo,
      quantidade: String(qty),
      saleChannel: 'whatsapp'
    };
    if (valueCents) additionalInfoMapPaid.valor = String((valueCents / 100).toFixed(2));

    const additionalInfoPaid = Object.keys(additionalInfoMapPaid).map((k) => ({ key: k, value: String(additionalInfoMapPaid[k]) }));

    const doc = {
      createdAt: now,
      paidAt: nowIso,
      status: 'pago',
      woovi: { status: 'pago', paidAt: nowIso, paymentMethods: { pix: { value: valueCents || 0 } } },
      valueCents: valueCents || 0,
      identifier,
      correlationID,
      customer: { phone },
      instagramUsername: username,
      instauser: username,
      tipo,
      tipoServico: tipo,
      qtd: qty,
      quantidade: qty,
      additionalInfoPaid,
      additionalInfoMapPaid,
      saleChannel: 'whatsapp',
      isWhatsappSale: true
    };

    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const ins = await col.insertOne(doc);
    res.json({ ok: true, id: ins?.insertedId ? String(ins.insertedId) : null });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.get('/api/painel/whatsapp-sales/followers-current', requireAdmin, async (req, res) => {
  try {
    const purchaseRaw = String(req.query?.purchase || '').trim();
    const force = String(req.query?.force || '').trim() === '1';
    if (!purchaseRaw) return res.status(400).json({ ok: false, error: 'missing_purchase' });

    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');

    const wppBaseQuery = { $or: [{ saleChannel: 'whatsapp' }, { isWhatsappSale: true }, { 'additionalInfoMapPaid.saleChannel': 'whatsapp' }, { 'additionalInfoMap.saleChannel': 'whatsapp' }] };

    const safe = purchaseRaw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const rx = new RegExp(safe, 'i');
    const digits = purchaseRaw.replace(/\D/g, '');
    const usernameCandidate = purchaseRaw.replace(/^@+/, '').trim();
    const orConds = [
      { identifier: rx },
      { correlationID: rx },
      { instagramUsername: rx },
      { instauser: rx },
      { 'additionalInfoMapPaid.instagram_username': rx },
      { 'additionalInfoMapPaid.phone': rx },
      { 'additionalInfoPaid': { $elemMatch: { key: 'instagram_username', value: rx } } },
      { 'additionalInfoPaid': { $elemMatch: { key: 'phone', value: rx } } }
    ];

    if (digits && digits.length >= 6) {
      const digitsRx = new RegExp(digits.split('').join('\\D*'), 'i');
      orConds.push({ 'customer.phone': { $regex: digitsRx } });
    }

    if (/^[0-9a-fA-F]{24}$/.test(purchaseRaw)) {
      try {
        const { ObjectId } = require('mongodb');
        orConds.push({ _id: new ObjectId(purchaseRaw) });
      } catch (_) {}
    }

    if (usernameCandidate) {
      const userSafe = usernameCandidate.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const userRx = new RegExp(userSafe, 'i');
      orConds.push({ instagramUsername: userRx });
      orConds.push({ instauser: userRx });
      orConds.push({ 'additionalInfoMapPaid.instagram_username': userRx });
    }

    const doc = await col.findOne(
      { $and: [wppBaseQuery, { $or: orConds }] },
      { projection: { _id: 1, createdAt: 1, paidAt: 1, woovi: 1, identifier: 1, correlationID: 1, status: 1, customer: 1, instagramUsername: 1, instauser: 1, tipo: 1, tipoServico: 1, qtd: 1, quantidade: 1, additionalInfoMapPaid: 1, additionalInfoPaid: 1 } }
    );
    if (!doc) return res.status(404).json({ ok: false, error: 'not_found' });

    const pickInfoPaid = (o, key) => {
      try {
        if (o && o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
        const arrPaid = Array.isArray(o && o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const itemPaid = arrPaid.find(i => i && i.key === key);
        if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
      } catch (_) {}
      return '';
    };

    const usernameRaw = String(pickInfoPaid(doc, 'instagram_username') || doc.instagramUsername || doc.instauser || '').trim();
    const username = usernameRaw.toLowerCase().replace(/^@+/, '').trim();
    if (!username) return res.status(400).json({ ok: false, error: 'missing_username' });

    const out = await followersMgmtGetCurrent(req, username, force);
    return res.status(out.code).json({
      purchase: purchaseRaw,
      sale: {
        id: doc._id ? String(doc._id) : null,
        identifier: doc.identifier ? String(doc.identifier) : null,
        correlationID: doc.correlationID ? String(doc.correlationID) : null,
        phone: (doc.customer && doc.customer.phone) ? String(doc.customer.phone) : null,
        paidAt: doc.woovi?.paidAt || doc.paidAt || doc.createdAt || null,
        tipo: String(pickInfoPaid(doc, 'tipo_servico') || doc.tipoServico || doc.tipo || '').trim() || null,
        quantidade: Number(doc.quantidade || doc.qtd || pickInfoPaid(doc, 'quantidade') || 0) || null
      },
      ...out.body
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

const followersMgmtSerial = new Map();
const followersMgmtThrottle = new Map();
const followersMgmtEnqueue = (key, fn) => {
  const prev = followersMgmtSerial.get(key) || Promise.resolve();
  const next = prev.catch(() => undefined).then(fn);
  followersMgmtSerial.set(key, next.finally(() => {
    try {
      if (followersMgmtSerial.get(key) === next) followersMgmtSerial.delete(key);
    } catch (_) {}
  }));
  return next;
};

const followersMgmtGetCurrent = async (req, username, force) => {
  const { getCollection } = require('./mongodbClient');
  const monitorCol = await getCollection('followers_monitor');

  const key = String((req.session && req.session.adminUser && req.session.adminUser.username) ? req.session.adminUser.username : 'admin') + '|' + String(req.realIP || req.ip || '');

  return followersMgmtEnqueue(key, async () => {
    const cached = await monitorCol.findOne({ username }, { projection: { _id: 0 } });
    if (!force && cached && cached.checkedAt) {
      const ageMs = Date.now() - new Date(String(cached.checkedAt)).getTime();
      if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs < (5 * 60 * 1000)) {
        return { code: 200, body: { ok: true, cached: true, username, followersCount: cached.followersCount, isPrivate: cached.isPrivate, checkedAt: cached.checkedAt, source: cached.source || null } };
      }
    }

    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const minIntervalMs = 1500;
    const now = Date.now();
    const nextAllowedAt = Number(followersMgmtThrottle.get(key) || 0) || 0;
    const waitMs = Math.max(0, nextAllowedAt - now);
    if (waitMs > 0) await sleep(waitMs);
    await sleep(Math.floor(Math.random() * 350));
    followersMgmtThrottle.set(key, Date.now() + minIntervalMs);

    let result = null;
    let profile = null;
    let source = 'web_profile_info';
    let isPrivate = null;
    let followersCount = null;
    let error = null;

    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        result = await fetchInstagramFollowersInfo(username);
        profile = result && result.success ? result.profile : null;
        source = profile ? String(profile.source || 'web_profile_info') : 'web_profile_info';
        isPrivate = profile ? !!profile.isPrivate : null;
        followersCount = profile && typeof profile.followersCount === 'number' ? profile.followersCount : null;
        error = result && !result.success ? (result.error || 'unknown_error') : null;
        if (profile) break;
      } catch (e) {
        error = e?.message || String(e);
      }
      if (attempt < 3) await sleep(900 * attempt);
    }

    if (!profile) {
      try {
        const userAgent = req.get("User-Agent") || "";
        const ip = req.realIP || req.ip || req.connection.remoteAddress || "";
        const mockReq = { session: {}, query: {}, body: {} };
        const fallback = await verifyInstagramProfile(username, userAgent, ip, mockReq, null, true);
        if (fallback && fallback.success && fallback.profile) {
          profile = fallback.profile;
          source = 'verifyInstagramProfile';
          isPrivate = !!profile.isPrivate;
          followersCount = typeof profile.followersCount === 'number' ? profile.followersCount : null;
          error = null;
        }
      } catch (_) {}
    }

    const nowIso = new Date().toISOString();
    const hasFresh = !!(profile && (typeof followersCount === 'number' || typeof isPrivate === 'boolean'));

    if (!hasFresh) {
      const prevFollowers = (cached && typeof cached.followersCount === 'number') ? cached.followersCount : null;
      const prevPrivate = (cached && typeof cached.isPrivate === 'boolean') ? cached.isPrivate : null;
      const prevCheckedAt = (cached && cached.checkedAt) ? cached.checkedAt : null;

      if (followersCount == null && prevFollowers != null) followersCount = prevFollowers;
      if (isPrivate == null && prevPrivate != null) isPrivate = prevPrivate;

      await monitorCol.updateOne(
        { username },
        { $set: { username, followersCount, isPrivate, source, error, lastAttemptAt: nowIso } },
        { upsert: true }
      );

      return { code: 200, body: { ok: true, cached: !!prevCheckedAt, stale: true, username, followersCount, isPrivate, checkedAt: prevCheckedAt, source, error } };
    }

    await monitorCol.updateOne(
      { username },
      { $set: { username, followersCount, isPrivate, checkedAt: nowIso, source, error, lastAttemptAt: nowIso } },
      { upsert: true }
    );

    return { code: 200, body: { ok: true, cached: false, username, followersCount, isPrivate, checkedAt: nowIso, source, error } };
  });
};

app.post('/api/painel/gerenciamento-seguidores/settings', requireAdmin, async (req, res) => {
  try {
    const dailyLimitRaw = parseInt(String((req.body && req.body.dailyLimit) || '50'), 10);
    const dailyLimit = Math.min(1000, Math.max(1, Number.isFinite(dailyLimitRaw) ? dailyLimitRaw : 50));
    const orderRaw = String((req.body && req.body.order) || 'newest').trim().toLowerCase();
    const order = (orderRaw === 'oldest') ? 'oldest' : 'newest';

    const { getCollection } = require('./mongodbClient');
    const settingsCol = await getCollection('settings');
    await settingsCol.updateOne(
      { _id: 'followers_mgmt_settings' },
      { $set: { values: { dailyLimit, order }, updatedAt: new Date().toISOString() } },
      { upsert: true }
    );
    return res.json({ ok: true, dailyLimit, order });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.get('/api/painel/gerenciamento-seguidores/current', requireAdmin, async (req, res) => {
  try {
    const username = String(req.query.username || '').trim().toLowerCase().replace(/^@/, '');
    const force = String(req.query.force || '').trim() === '1';
    if (!username) return res.status(400).json({ ok: false, error: 'missing_username' });

    const out = await followersMgmtGetCurrent(req, username, force);
    return res.status(out.code).json(out.body);
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post('/api/painel/gerenciamento-seguidores/test-old', requireAdmin, async (req, res) => {
  try {
    const limitRaw = parseInt(String((req.body && req.body.limit) || req.query.limit || '10'), 10);
    const limit = Math.min(50, Math.max(1, Number.isFinite(limitRaw) ? limitRaw : 10));

    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const monitorCol = await getCollection('followers_monitor');

    const paidQuery = { $or: [{ status: 'pago' }, { 'woovi.status': 'pago' }] };
    const followersQuery = {
      $and: [
        paidQuery,
        {
          $or: [
            { 'fama24h.orderId': { $exists: true, $ne: '' } },
            { 'fornecedor_social.orderId': { $exists: true, $ne: '' } }
          ]
        }
      ]
    };

    const orders = await col.find(followersQuery, {
      projection: {
        _id: 1,
        createdAt: 1,
        paidAt: 1,
        woovi: 1,
        tipo: 1,
        tipoServico: 1,
        qtd: 1,
        quantidade: 1,
        instagramUsername: 1,
        instauser: 1,
        fama24h: 1,
        fornecedor_social: 1,
        additionalInfo: 1,
        additionalInfoPaid: 1,
        additionalInfoMap: 1,
        additionalInfoMapPaid: 1
      }
    }).sort({ createdAt: 1 }).limit(2000).toArray();

    const extractInfoAny = (o, key) => {
      try {
        if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
        if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return o.additionalInfoMap[key];
        const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const itemPaid = arrPaid.find(i => i && i.key === key);
        if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
        const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
        const item = arr.find(i => i && i.key === key);
        if (item && typeof item.value !== 'undefined') return item.value;
      } catch (_) {}
      return '';
    };

    const normalizeUsernameKey = (u) => {
      const s0 = String(u || '').trim();
      if (!s0) return '';
      const s1 = s0.startsWith('@') ? s0.slice(1) : s0;
      return s1.toLowerCase().trim();
    };

    const resolveUsername = (o) => {
      const candidates = [
        extractInfoAny(o, 'instagram_username'),
        o.instagramUsername,
        o.instauser
      ];
      for (const c of candidates) {
        const k = normalizeUsernameKey(c);
        if (k) return k;
      }
      return '';
    };

    const resolveTipoRaw = (o) => {
      const candidates = [
        extractInfoAny(o, 'tipo_servico'),
        extractInfoAny(o, 'tipo'),
        o.tipo,
        o.tipoServico
      ];
      for (const c of candidates) {
        const s = String(c || '').trim();
        if (s) return s;
      }
      return '';
    };

    const isFollowersServico = (tipoRaw) => {
      const t = String(tipoRaw || '').toLowerCase();
      if (!t) return true;
      if (t.includes('curtida') || t.includes('like')) return false;
      if (t.includes('visualiza') || t.includes('view')) return false;
      return true;
    };

    const resolveQty = (o) => {
      const candidates = [
        extractInfoAny(o, 'quantidade'),
        extractInfoAny(o, 'qtd'),
        extractInfoAny(o, 'quantity'),
        o.quantidade,
        o.qtd
      ];
      for (const c of candidates) {
        const n = parseInt(String(c || '').replace(/[^\d]/g, ''), 10);
        if (Number.isFinite(n) && n > 0) return n;
      }
      return 0;
    };

    const resolveQtyWithUpgrade = (o) => {
      const base = resolveQty(o);
      const extraRaw = extractInfoAny(o, 'upgrade_extra_qtd');
      const extra = parseInt(String(extraRaw || '').replace(/[^\d-]/g, ''), 10);
      if (Number.isFinite(extra) && extra > 0) return base + extra;
      return base;
    };

    const usernames = [];
    const seen = new Set();
    for (const o of orders) {
      const tipoRaw = resolveTipoRaw(o);
      if (!isFollowersServico(tipoRaw)) continue;
      const contracted = resolveQtyWithUpgrade(o);
      if (!contracted || contracted <= 0) continue;
      const u = resolveUsername(o);
      if (!u || seen.has(u)) continue;
      seen.add(u);
      usernames.push(u);
      if (usernames.length >= limit) break;
    }

    const results = [];
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    for (const u of usernames) {
      const started = Date.now();
      let cached = null;
      let profile = null;
      let source = null;
      let followersCount = null;
      let isPrivate = null;
      let error = null;

      try {
        cached = await monitorCol.findOne({ username: u }, { projection: { _id: 0 } });
      } catch (_) {}

      try {
        let r = null;
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            r = await fetchInstagramFollowersInfo(u);
            profile = r && r.success ? r.profile : null;
            source = profile ? String(profile.source || 'web_profile_info') : 'web_profile_info';
            isPrivate = profile ? !!profile.isPrivate : null;
            followersCount = profile && typeof profile.followersCount === 'number' ? profile.followersCount : null;
            error = r && !r.success ? (r.error || 'unknown_error') : null;
            if (profile) break;
          } catch (e) {
            error = e?.message || String(e);
          }
          if (attempt < 3) await sleep(900 * attempt);
        }
      } catch (e) {
        error = e?.message || String(e);
      }

      if (!profile) {
        try {
          const userAgent = req.get("User-Agent") || "";
          const ip = req.realIP || req.ip || req.connection.remoteAddress || "";
          const mockReq = { session: {}, query: {}, body: {} };
          const fallback = await verifyInstagramProfile(u, userAgent, ip, mockReq, null, true);
          if (fallback && fallback.success && fallback.profile) {
            profile = fallback.profile;
            source = 'verifyInstagramProfile';
            isPrivate = !!profile.isPrivate;
            followersCount = typeof profile.followersCount === 'number' ? profile.followersCount : null;
            error = null;
          }
        } catch (_) {}
      }

      const nowIso = new Date().toISOString();
      const hasFresh = !!(profile && (typeof followersCount === 'number' || typeof isPrivate === 'boolean'));
      const prevFollowers = (cached && typeof cached.followersCount === 'number') ? cached.followersCount : null;
      const prevPrivate = (cached && typeof cached.isPrivate === 'boolean') ? cached.isPrivate : null;
      const prevCheckedAt = (cached && cached.checkedAt) ? cached.checkedAt : null;
      if (!hasFresh) {
        if (followersCount == null && prevFollowers != null) followersCount = prevFollowers;
        if (isPrivate == null && prevPrivate != null) isPrivate = prevPrivate;
      }

      try {
        await monitorCol.updateOne(
          { username: u },
          { $set: { username: u, followersCount, isPrivate, source: source || null, error: error || null, lastAttemptAt: nowIso, ...(hasFresh ? { checkedAt: nowIso } : {}) } },
          { upsert: true }
        );
      } catch (_) {}

      results.push({
        username: u,
        ok: typeof followersCount === 'number',
        followersCount,
        isPrivate,
        source,
        error,
        checkedAt: hasFresh ? nowIso : (prevCheckedAt || null),
        ms: Date.now() - started
      });

      await sleep(1500 + Math.floor(Math.random() * 450));
    }

    return res.json({ ok: true, limit, tested: results.length, results });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post('/api/admin/update-private-order', requireAdmin, async (req, res) => {
    try {
        console.log('Update Private Order:', req.body);
        const { id, field, value } = req.body;
        if (!id || !field) return res.status(400).json({ error: 'Missing id or field' });
        
        const allowedFields = ['orderSent', 'noContact', 'contactCount', 'supplierInfo', 'archived'];
        if (!allowedFields.includes(field)) return res.status(400).json({ error: 'Invalid field' });
        
        const { getCollection } = require('./mongodbClient');
        const { ObjectId } = require('mongodb');
        const col = await getCollection('checkout_orders');
        
        let updateSet = {};
        let updateUnset = {};

        if (field === 'contactCount') {
            updateSet['contactCount'] = Number(value);
        } else if (field === 'supplierInfo') {
            const { provider, orderId } = value;
            if (!provider || !orderId) return res.status(400).json({ error: 'Missing provider info' });
            
            updateSet['orderSent'] = true;
            
            if (provider === 'fama24h') {
                updateSet['fama24h.orderId'] = orderId;
                updateSet['fama24h.status'] = 'Pending'; 
                updateUnset['fama24h.error'] = '';
            } else if (provider === 'fornecedor_social') {
                updateSet['fornecedor_social.orderId'] = orderId;
                updateSet['fornecedor_social.status'] = 'Pending';
                updateUnset['fornecedor_social.error'] = '';
            } else if (provider === 'worldsmm_comments') {
                updateSet['worldsmm_comments.orderId'] = orderId;
                updateSet['worldsmm_comments.status'] = 'created';
                updateUnset['worldsmm_comments.error'] = '';
            } else if (provider === 'fama24h_views') {
                updateSet['fama24h_views.orderId'] = orderId;
                updateSet['fama24h_views.status'] = 'created';
                updateUnset['fama24h_views.error'] = '';
            } else if (provider === 'fama24h_likes') {
                updateSet['fama24h_likes.orderId'] = orderId;
                updateSet['fama24h_likes.status'] = 'created';
                updateUnset['fama24h_likes.error'] = '';
            }
        } else if (field === 'archived') {
            const v = !!value;
            updateSet['archived'] = v;
            updateSet['archivedAt'] = v ? new Date().toISOString() : null;
        } else {
            updateSet[field] = value;
        }
        
        const update = { $set: updateSet };
        if (updateUnset && Object.keys(updateUnset).length > 0) update.$unset = updateUnset;
        await col.updateOne({ _id: new ObjectId(id) }, update);
        res.json({ ok: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/admin/unknown_orderid/refresh', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const { ObjectId } = require('mongodb');
    const col = await getCollection('checkout_orders');

    const idsRaw = Array.isArray(req.body && req.body.ids) ? req.body.ids : [];
    const ids = idsRaw.map(v => String(v || '').trim()).filter(Boolean);
    if (!ids.length) return res.json({ ok: true, updated: 0, updatedPrivacy: 0, updatedLinks: 0 });

    const objectIds = [];
    for (const s of ids) {
      try { objectIds.push(new ObjectId(s)); } catch (_) {}
    }
    if (!objectIds.length) return res.json({ ok: true, updated: 0, updatedPrivacy: 0, updatedLinks: 0 });

    const orders = await col.find(
      { _id: { $in: objectIds } },
      { projection: { instauser: 1, instaUser: 1, instagramUsername: 1, instagramUser: 1, additionalInfoPaid: 1, additionalInfo: 1, additionalInfoMap: 1, additionalInfoMapPaid: 1, fama24h_views: 1, fama24h_likes: 1, worldsmm_comments: 1, isPrivate: 1, profilePrivacy: 1 } }
    ).toArray();

    const normalizeUsernameKey = (u) => {
      const s0 = String(u || '').trim();
      if (!s0) return '';
      let s = s0;
      if (/instagram\.com\//i.test(s)) {
        try {
          const u1 = new URL(s.startsWith('http') ? s : `https://${s.replace(/^\/+/, '')}`);
          const parts = u1.pathname.split('/').map(p => p.trim()).filter(Boolean);
          if (parts.length) s = parts[0];
        } catch (_) {
          const parts = s.split('?')[0].split('#')[0].split('/').map(p => p.trim()).filter(Boolean);
          if (parts.length) s = parts[parts.length - 1];
        }
      }
      if (s.startsWith('@')) s = s.slice(1);
      s = s.replace(/[^a-zA-Z0-9._]/g, '').toLowerCase().trim();
      return s;
    };

    const resolveUser = (o) => {
      if (o.instaUser) return String(o.instaUser);
      if (o.instauser) return String(o.instauser);
      if (o.instagramUser) return String(o.instagramUser);
      if (o.additionalInfoMap && (o.additionalInfoMap.instauser || o.additionalInfoMap.instaUser)) return String(o.additionalInfoMap.instauser || o.additionalInfoMap.instaUser);
      if (o.additionalInfoPaid) {
        const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const uItem = arr.find(i => i && (i.key === 'instauser' || i.key === 'instaUser' || i.key === 'instagram_username' || i.key === 'user'));
        if (uItem && typeof uItem.value !== 'undefined') return String(uItem.value);
      }
      return '';
    };

    const resolveOrderBumps = (o) => {
      try {
        if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid.order_bumps === 'string') return o.additionalInfoMapPaid.order_bumps;
        const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const paidItem = arrPaid.find(i => i && i.key === 'order_bumps');
        if (paidItem && typeof paidItem.value === 'string') return paidItem.value;
        if (o.additionalInfoMap && typeof o.additionalInfoMap.order_bumps === 'string') return o.additionalInfoMap.order_bumps;
        const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
        const item = arr.find(i => i && i.key === 'order_bumps');
        if (item && typeof item.value === 'string') return item.value;
      } catch (_) {}
      return '';
    };

    const parseBumps = (raw) => {
      const text = String(raw || '');
      const parts = text.split(';').map(p => p.trim()).filter(Boolean);
      const res = { views: 0, likes: 0, comments: 0, upgrade: 0 };
      for (const p of parts) {
        const [kRaw, vRaw] = p.split(':');
        const k = String(kRaw || '').toLowerCase().trim();
        const v = Number(String(vRaw || '').replace(/[^\d]/g, '')) || 0;
        if (k === 'views') res.views = v;
        else if (k === 'likes') res.likes = v;
        else if (k === 'comments') res.comments = v;
        else if (k === 'upgrade') res.upgrade = v || 1;
      }
      return res;
    };

    const extractInfoAny = (o, key) => {
      try {
        if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
        if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return o.additionalInfoMap[key];
        const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const itemPaid = arrPaid.find(i => i && i.key === key);
        if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
        const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
        const item = arr.find(i => i && i.key === key);
        if (item && typeof item.value !== 'undefined') return item.value;
      } catch (_) {}
      return '';
    };

    const normalizeUrl = (u) => {
      const s0 = String(u || '').trim();
      if (!s0) return '';
      if (/^https?:\/\//i.test(s0)) return s0;
      if (/^www\./i.test(s0)) return `https://${s0}`;
      if (/instagram\.com\//i.test(s0)) return `https://${s0.replace(/^\/+/, '')}`;
      return s0;
    };

    const sanitizeInstagramPostLink = (u) => {
      const s0 = normalizeUrl(u);
      let v = String(s0 || '').trim();
      if (!v) return '';
      v = v.split('#')[0].split('?')[0];
      const m = v.match(/^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/([A-Za-z0-9_-]+)\/?$/i);
      if (!m) return '';
      const kind = String(m[2] || '').toLowerCase();
      const code = String(m[3] || '');
      if (!kind || !code) return '';
      return `https://www.instagram.com/${kind}/${code}/`;
    };

    const getExistingLink = (o, kind) => {
      if (kind === 'views') return sanitizeInstagramPostLink(o?.fama24h_views?.requestPayload?.link || extractInfoAny(o, 'orderbump_post_views') || extractInfoAny(o, 'post_link'));
      if (kind === 'likes') return sanitizeInstagramPostLink(o?.fama24h_likes?.requestPayload?.link || extractInfoAny(o, 'orderbump_post_likes') || extractInfoAny(o, 'post_link'));
      if (kind === 'comments') return sanitizeInstagramPostLink(o?.worldsmm_comments?.requestPayload?.link || extractInfoAny(o, 'orderbump_post_comments') || extractInfoAny(o, 'post_link'));
      return '';
    };

    const postsCache = new Map();
    let updated = 0;
    let updatedPrivacy = 0;
    let updatedLinks = 0;

    for (const o of orders) {
      const id = o && o._id ? o._id : null;
      if (!id) continue;

      const username = normalizeUsernameKey(resolveUser(o));
      if (!username) continue;
      const set = {};
      const bumps = parseBumps(resolveOrderBumps(o));
      const wantsViews = bumps.views > 0 || !!(o && o.fama24h_views);
      const wantsLikes = bumps.likes > 0 || !!(o && o.fama24h_likes);
      const wantsComments = bumps.comments > 0 || !!(o && o.worldsmm_comments);

      const needViews = wantsViews && !getExistingLink(o, 'views');
      const needLikes = wantsLikes && !getExistingLink(o, 'likes');
      const needComments = wantsComments && !getExistingLink(o, 'comments');

      if (needViews || needLikes || needComments) {
        let posts = postsCache.get(username);
        if (typeof posts === 'undefined') {
          try {
            const r = await fetchInstagramRecentPosts(username);
            posts = (r && r.success && Array.isArray(r.posts)) ? r.posts : null;
          } catch (_) {
            posts = null;
          }
          postsCache.set(username, posts);
        }

        if (Array.isArray(posts) && posts.length) {
          const isVideo = (p) => !!(p && (p.isVideo || /video|clip/i.test(String(p.typename || ''))));
          const newest = posts[0];
          const newestVideo = posts.find(isVideo) || null;

          if (needViews && newestVideo && newestVideo.shortcode) {
            set['additionalInfoMapPaid.orderbump_post_views'] = `https://www.instagram.com/reel/${encodeURIComponent(String(newestVideo.shortcode))}/`;
          }
          if (needLikes && newest && newest.shortcode) {
            set['additionalInfoMapPaid.orderbump_post_likes'] = `https://www.instagram.com/p/${encodeURIComponent(String(newest.shortcode))}/`;
          }
          if (needComments && newest && newest.shortcode) {
            set['additionalInfoMapPaid.orderbump_post_comments'] = `https://www.instagram.com/p/${encodeURIComponent(String(newest.shortcode))}/`;
          }
        }
      }

      if (Object.keys(set).length) {
        await col.updateOne({ _id: id }, { $set: set });
        updated += 1;
        if (typeof set['additionalInfoMapPaid.orderbump_post_views'] !== 'undefined' || typeof set['additionalInfoMapPaid.orderbump_post_likes'] !== 'undefined' || typeof set['additionalInfoMapPaid.orderbump_post_comments'] !== 'undefined') {
          updatedLinks += 1;
        }
      }
    }

    return res.json({ ok: true, updated, updatedPrivacy, updatedLinks });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post('/api/admin/evolution/send-text', requireAdmin, async (req, res) => {
  try {
    const body = req.body || {};
    const id = String(body.id || '').trim();
    const rawPhone = String(body.phone || '').trim();
    const text = String(body.text || '').trim();

    const baseUrlRaw = String(process.env.EVOLUTION_API_URL || '').trim();
    const apiKey = String(process.env.EVOLUTION_API_KEY || '').trim();
    const instanceName = String(process.env.EVOLUTION_INSTANCE_NAME || 'oppus').trim();
    const instanceToken = String(process.env.EVOLUTION_INSTANCE_TOKEN || '').trim();

    if (!baseUrlRaw) return res.status(500).json({ ok: false, error: 'missing_evolution_api_url' });
    if (!apiKey) return res.status(500).json({ ok: false, error: 'missing_evolution_api_key' });
    if (!instanceName) return res.status(500).json({ ok: false, error: 'missing_evolution_instance_name' });
    if (!rawPhone) return res.status(400).json({ ok: false, error: 'missing_phone' });
    if (!text) return res.status(400).json({ ok: false, error: 'missing_text' });

    const normalizePhone = (p) => {
      const digits = String(p || '').replace(/[^\d]/g, '');
      if (!digits) return '';
      if (digits.startsWith('55') && digits.length >= 12) return digits;
      if (digits.length === 10 || digits.length === 11) return '55' + digits;
      return digits;
    };

    const number = normalizePhone(rawPhone);
    if (!number || number.length < 10) return res.status(400).json({ ok: false, error: 'invalid_phone' });

    const baseUrl = baseUrlRaw.replace(/\/+$/, '');
    const axios = require('axios');
    const headers = Object.assign({ 'Content-Type': 'application/json', apikey: apiKey }, instanceToken ? { token: instanceToken } : {});

    const tryUrls = [
      `${baseUrl}/message/sendText/${encodeURIComponent(instanceName)}`,
      `${baseUrl}/api/message/sendText/${encodeURIComponent(instanceName)}`
    ];

    let resp = null;
    let lastErr = null;
    for (const url of tryUrls) {
      try {
        resp = await axios.post(url, { number, text }, { timeout: 20000, headers });
        lastErr = null;
        break;
      } catch (err) {
        const status = err?.response?.status;
        if (status && status !== 404) throw err;
        lastErr = err;
      }
    }
    if (!resp && lastErr) throw lastErr;

    try {
      if (id && /^[0-9a-fA-F]{24}$/.test(id)) {
        const { getCollection } = require('./mongodbClient');
        const { ObjectId } = require('mongodb');
        const col = await getCollection('checkout_orders');
        await col.updateOne(
          { _id: new ObjectId(id) },
          {
            $set: {
              lastContactAt: new Date().toISOString(),
              lastContactText: text,
              lastContactPhone: rawPhone,
              lastContactChannel: 'whatsapp',
              noContact: false
            },
            $inc: { contactCount: 1 }
          }
        );
      }
    } catch (_) {}

    return res.json({ ok: true, result: resp.data || null });
  } catch (e) {
    const status = e?.response?.status || 500;
    const data = e?.response?.data;
    const msg =
      (typeof data === 'string'
        ? data
        : (data && (typeof data.message === 'string' ? data.message : (data.error || data.message))) || null) ||
      e?.message ||
      String(e);
    const details = (data && typeof data === 'object') ? data : null;
    return res.status(status).json({ ok: false, error: String(msg), details });
  }
});

app.post('/api/admin/private-order-email', requireAdmin, async (req, res) => {
    try {
        const { id } = req.body || {};
        if (!id || !/^[0-9a-fA-F]{24}$/.test(String(id))) return res.status(400).json({ ok: false, error: 'invalid_id' });
        const { getCollection } = require('./mongodbClient');
        const { ObjectId } = require('mongodb');
        const col = await getCollection('checkout_orders');
        const order = await col.findOne(
            { _id: new ObjectId(String(id)) },
            {
                projection: {
                    customer: 1,
                    woovi: 1,
                    customerEmail: 1,
                    additionalInfoMap: 1,
                    additionalInfoMapPaid: 1,
                    additionalInfo: 1,
                    additionalInfoPaid: 1
                }
            }
        );
        const arrEmail = (arr) => {
            const a = Array.isArray(arr) ? arr : [];
            const item = a.find(it => {
                const k = String(it?.key || '').trim().toLowerCase();
                return k === 'email' || k === 'e-mail' || k === 'mail' || k === 'contact_email';
            });
            return item ? String(item.value || '') : '';
        };
        const normalizeEmail = (s) => {
            const raw = typeof s === 'string' ? s.trim() : '';
            if (!raw) return '';
            const email = raw.toLowerCase();
            const isValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
            return isValid ? email : '';
        };
        const mapEmail = (m) => {
            const obj = (m && typeof m === 'object') ? m : null;
            if (!obj) return '';
            const keys = Object.keys(obj);
            for (const k of keys) {
                const kk = String(k || '').toLowerCase();
                if (!kk.includes('email')) continue;
                const v = obj[k];
                if (typeof v === 'string' && v.trim()) return v;
            }
            return '';
        };
        const candidates = [
            order?.customer?.email,
            order?.customer?.mail,
            order?.customerEmail,
            order?.woovi?.customer?.email,
            order?.woovi?.charge?.customer?.email,
            order?.woovi?.charge?.charge?.customer?.email,
            order?.additionalInfoMapPaid?.email,
            order?.additionalInfoMap?.email,
            mapEmail(order?.additionalInfoMapPaid),
            mapEmail(order?.additionalInfoMap),
            arrEmail(order?.additionalInfoPaid),
            arrEmail(order?.additionalInfo)
        ];
        let emailOut = '';
        for (const c of candidates) {
            const normalized = normalizeEmail(typeof c === 'string' ? c : '');
            if (normalized) {
                emailOut = normalized;
                break;
            }
        }
        if (!emailOut) {
            return res.json({ ok: false, error: 'email_not_found' });
        }
        return res.json({ ok: true, email: emailOut });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

function normalizeApifyActorId(raw) {
    const s = String(raw || '').trim();
    if (!s) return '';
    if (!s.includes('~') && s.includes('/')) {
        const parts = s.split('/').map(p => String(p || '').trim()).filter(Boolean);
        if (parts.length === 2) return `${parts[0]}~${parts[1]}`;
    }
    return s;
}

function normalizeInstaUsernameForScrape(v) {
    try {
        let s = String(v || '').trim();
        if (!s) return '';
        if (/^https?:\/\//i.test(s)) {
            s = s.replace(/^https?:\/\/(www\.)?instagram\.com\//i, '');
            s = s.split('?')[0].split('#')[0];
            s = s.replace(/\/+$/, '');
            const parts = s.split('/').filter(Boolean);
            s = parts.length ? String(parts[parts.length - 1] || '') : s;
        }
        s = s.trim();
        if (s.startsWith('@')) s = s.slice(1);
        return s.toLowerCase().trim();
    } catch (_) {
        return '';
    }
}

function extractCookieFromSetCookieHeader(setCookieHeader, cookieName) {
    const arr = Array.isArray(setCookieHeader) ? setCookieHeader : [];
    const name = String(cookieName || '').trim();
    if (!name) return '';
    for (const raw of arr) {
        const s = String(raw || '');
        const idx = s.toLowerCase().indexOf(`${name.toLowerCase()}=`);
        if (idx === -1) continue;
        const after = s.slice(idx + name.length + 1);
        const val = after.split(';')[0];
        if (val) return val;
    }
    return '';
}

function igBuildProxyAgent(profile) {
    try {
        if (!profile || !profile.proxy) return null;
        return new HttpsProxyAgent(
            `http://${encodeURIComponent(profile.proxy.auth.username)}:${encodeURIComponent(profile.proxy.auth.password)}@${profile.proxy.host}:${profile.proxy.port}`,
            { rejectUnauthorized: false }
        );
    } catch (_) {
        return null;
    }
}

function igCookieHeader(profile) {
    const parts = [
        `sessionid=${String(profile?.sessionid || '').trim()}`,
        `ds_user_id=${String(profile?.ds_user_id || '').trim()}`
    ].filter((v) => !/=\s*$/.test(v));
    const csrf = String(profile?.csrftoken || '').trim();
    if (csrf) parts.push(`csrftoken=${csrf}`);
    return parts.join('; ');
}

async function igEnsureCsrfToken(profile, proxyAgent, userAgent) {
    if (!profile) return '';
    const existing = String(profile.csrftoken || '').trim();
    const fetchedAt = Number(profile.csrftokenFetchedAt || 0) || 0;
    if (existing && Date.now() - fetchedAt < 30 * 60 * 1000) return existing;

    const headers = {
        "User-Agent": String(userAgent || profile.userAgent || ''),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Cookie": igCookieHeader(profile)
    };

    const resp = await axios.get('https://www.instagram.com/', {
        headers,
        httpsAgent: proxyAgent || undefined,
        timeout: 7000,
        validateStatus: () => true
    });

    const token = extractCookieFromSetCookieHeader(resp?.headers?.['set-cookie'], 'csrftoken');
    if (token) {
        profile.csrftoken = token;
        profile.csrftokenFetchedAt = Date.now();
    }
    return String(profile.csrftoken || '').trim();
}

function igPickProfiles(max) {
    const now = Date.now();
    const available = cookieProfiles
        .filter((p) => p && p.disabledUntil <= now && !isCookieLocked(p.ds_user_id))
        .sort((a, b) => {
            if (a.errorCount !== b.errorCount) return a.errorCount - b.errorCount;
            return a.lastUsed - b.lastUsed;
        });
    const cap = Math.max(1, Number(max || 1));
    return available.slice(0, cap);
}

const IG_USERCOUNTS_CACHE = new Map();
const IG_USERCOUNTS_CACHE_TTL_MS = 30 * 60 * 1000;

function igGetCachedUserCounts(pkRaw) {
    const pk = String(pkRaw || '').trim();
    if (!pk) return null;
    const entry = IG_USERCOUNTS_CACHE.get(pk);
    if (!entry) return null;
    if (Date.now() > Number(entry.expiresAt || 0)) {
        IG_USERCOUNTS_CACHE.delete(pk);
        return null;
    }
    return entry.value || null;
}

function igSetCachedUserCounts(pkRaw, value) {
    const pk = String(pkRaw || '').trim();
    if (!pk) return;
    if (!value) return;
    if (IG_USERCOUNTS_CACHE.size > 15000) {
        const firstKey = IG_USERCOUNTS_CACHE.keys().next().value;
        if (firstKey) IG_USERCOUNTS_CACHE.delete(firstKey);
    }
    IG_USERCOUNTS_CACHE.set(pk, { value, expiresAt: Date.now() + IG_USERCOUNTS_CACHE_TTL_MS });
}

async function igRespectMinInterval(profile, minIntervalMs) {
    const p = profile || null;
    if (!p) return;
    const minMs = Math.max(0, Number(minIntervalMs || 0));
    if (!minMs) return;
    const last = Number(p.lastUsed || 0) || 0;
    const since = Date.now() - last;
    if (since < minMs) {
        const extra = Math.floor(Math.random() * 250);
        await sleepMs((minMs - since) + extra);
    }
}

async function igFetchWebProfileInfo(username) {
    const u = String(username || '').trim();
    if (!u) return { success: false, error: 'username_invalid' };

    const REQUEST_TIMEOUT = 4500;
    const candidates = igPickProfiles(3);

    const tryProfile = async (profile) => {
        if (!profile) throw new Error('No profile');
        if (isCookieLocked(profile.ds_user_id)) throw new Error('Locked');
        lockCookie(profile.ds_user_id);
        try {
            const proxyAgent = igBuildProxyAgent(profile);
            const headers = {
                "User-Agent": profile.userAgent,
                "X-IG-App-ID": "936619743392459",
                "Cookie": igCookieHeader(profile),
                "Accept": "application/json",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": `https://www.instagram.com/${encodeURIComponent(u)}/`
            };

            const resp = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(u)}`, {
                headers,
                httpsAgent: proxyAgent || undefined,
                timeout: REQUEST_TIMEOUT,
                validateStatus: () => true
            });

            if (resp.status !== 200) throw new Error(`HTTP ${resp.status}`);
            const user = resp?.data?.data?.user;
            if (!user || !user.username) return { success: false, error: 'Perfil inexistente' };

            profile.lastUsed = Date.now();
            profile.errorCount = 0;

            return {
                success: true,
                profile: {
                    id: String(user.id || ''),
                    username: String(user.username || u),
                    isPrivate: !!user.is_private,
                    followersCount: (user.edge_followed_by && typeof user.edge_followed_by.count === 'number') ? user.edge_followed_by.count : 0,
                    followingCount: (user.edge_follow && typeof user.edge_follow.count === 'number') ? user.edge_follow.count : 0
                }
            };
        } catch (err) {
            profile.errorCount++;
            if (profile.errorCount >= 5) profile.disabledUntil = Date.now() + (60 * 1000);
            throw err;
        } finally {
            unlockCookie(profile.ds_user_id);
        }
    };

    if (candidates.length) {
        try {
            return await Promise.any(candidates.map((p) => tryProfile(p)));
        } catch (_) {}
    }

    try {
        const FALLBACK_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
        const resp = await axios.get(`https://www.instagram.com/${encodeURIComponent(u)}/?__a=1&__d=dis`, {
            headers: {
                'User-Agent': FALLBACK_UA,
                'Accept': 'application/json',
                'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Referer': `https://www.instagram.com/${encodeURIComponent(u)}/`
            },
            timeout: 4500,
            validateStatus: () => true
        });
        if (resp.status !== 200) return { success: false, error: 'Falha ao buscar perfil' };
        if (typeof resp.data === 'string') return { success: false, error: 'Falha ao buscar perfil' };
        const root = resp.data || {};
        const user = (root.graphql && root.graphql.user) ? root.graphql.user : (root.user || null);
        if (!user || !user.username) return { success: false, error: 'Perfil inexistente' };
        return {
            success: true,
            profile: {
                id: String(user.id || ''),
                username: String(user.username || u),
                isPrivate: !!user.is_private,
                followersCount: (user.edge_followed_by && typeof user.edge_followed_by.count === 'number') ? user.edge_followed_by.count : 0,
                followingCount: (user.edge_follow && typeof user.edge_follow.count === 'number') ? user.edge_follow.count : 0
            }
        };
    } catch (_) {
        return { success: false, error: 'Falha ao buscar perfil' };
    }
}

async function igFetchFollowersBatchByUserId(userIdRaw, maxFollowersRaw) {
    const userId = String(userIdRaw || '').trim();
    if (!userId) return { success: false, error: 'user_id_missing', followers: [] };

    const maxFollowers = Math.min(100000, Math.max(1, Number(maxFollowersRaw || 1)));
    const out = [];
    const seenUsernames = new Set();
    const REQUEST_TIMEOUT = 8000;
    const pageCount = 50;

    let maxId = '';
    let safety = 0;

    while (out.length < maxFollowers && safety < 250) {
        safety++;
        const profile = igPickProfiles(1)[0];
        if (!profile) return { success: false, error: 'Sem perfis de cookie disponíveis', followers: out };

        if (isCookieLocked(profile.ds_user_id)) {
            await sleepMs(250);
            continue;
        }

        lockCookie(profile.ds_user_id);
        try {
            await igRespectMinInterval(profile, 1200);
            const proxyAgent = igBuildProxyAgent(profile);
            const csrf = await igEnsureCsrfToken(profile, proxyAgent, profile.userAgent);
            const headers = {
                "User-Agent": profile.userAgent,
                "X-IG-App-ID": "936619743392459",
                "Cookie": igCookieHeader(profile),
                "Accept": "application/json",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.instagram.com/",
                ...(csrf ? { "X-CSRFToken": csrf } : {})
            };

            const url = `https://www.instagram.com/api/v1/friendships/${encodeURIComponent(userId)}/followers/?count=${pageCount}&search_surface=follow_list_page${maxId ? `&max_id=${encodeURIComponent(maxId)}` : ''}`;
            const resp = await axios.get(url, {
                headers,
                httpsAgent: proxyAgent || undefined,
                timeout: REQUEST_TIMEOUT,
                validateStatus: () => true
            });

            if (resp.status === 429 || resp.status === 403) {
                profile.errorCount = Number(profile.errorCount || 0) + 2;
                profile.disabledUntil = Date.now() + (10 * 60 * 1000);
                await sleepMs(2000 + Math.floor(Math.random() * 2000));
                continue;
            }
            if (resp.status !== 200) return { success: false, error: `HTTP ${resp.status}`, followers: out };

            const users = Array.isArray(resp?.data?.users) ? resp.data.users : [];
            for (const usr of users) {
                const username = String(usr?.username || '').trim().toLowerCase();
                const pk = usr?.pk != null ? String(usr.pk) : '';
                if (!username || !pk) continue;
                if (seenUsernames.has(username)) continue;
                seenUsernames.add(username);
                out.push({ username, pk });
                if (out.length >= maxFollowers) break;
            }

            const nextMaxId = String(resp?.data?.next_max_id || resp?.data?.next_maxId || '').trim();
            profile.errorCount = 0;

            if (!nextMaxId) break;
            maxId = nextMaxId;
        } catch (err) {
            profile.errorCount = Number(profile.errorCount || 0) + 1;
            if (profile.errorCount >= 5) profile.disabledUntil = Date.now() + (60 * 1000);
            await sleepMs(700 + Math.floor(Math.random() * 700));
        } finally {
            profile.lastUsed = Date.now();
            unlockCookie(profile.ds_user_id);
        }
    }

    return { success: true, followers: out };
}

async function igFetchUserCountsById(pkRaw) {
    const pk = String(pkRaw || '').trim();
    if (!pk) return { success: false, error: 'pk_missing' };

    const cached = igGetCachedUserCounts(pk);
    if (cached && cached.username) {
        return { success: true, data: cached };
    }

    const profile = igPickProfiles(1)[0];
    if (!profile) return { success: false, error: 'Sem perfis de cookie disponíveis' };
    if (isCookieLocked(profile.ds_user_id)) return { success: false, error: 'cookie_locked' };

    lockCookie(profile.ds_user_id);
    try {
        await igRespectMinInterval(profile, 900);
        const proxyAgent = igBuildProxyAgent(profile);
        const csrf = await igEnsureCsrfToken(profile, proxyAgent, profile.userAgent);
        const headers = {
            "User-Agent": profile.userAgent,
            "X-IG-App-ID": "936619743392459",
            "Cookie": igCookieHeader(profile),
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.instagram.com/",
            ...(csrf ? { "X-CSRFToken": csrf } : {})
        };

        const url = `https://www.instagram.com/api/v1/users/${encodeURIComponent(pk)}/info/`;
        const resp = await axios.get(url, {
            headers,
            httpsAgent: proxyAgent || undefined,
            timeout: 6500,
            validateStatus: () => true
        });

        if (resp.status === 429 || resp.status === 403) {
            profile.errorCount = Number(profile.errorCount || 0) + 2;
            profile.disabledUntil = Date.now() + (10 * 60 * 1000);
            return { success: false, error: `HTTP ${resp.status}` };
        }
        if (resp.status !== 200) return { success: false, error: `HTTP ${resp.status}` };
        const user = resp?.data?.user;
        if (!user || !user.username) return { success: false, error: 'user_missing' };

        profile.errorCount = 0;

        const followersCount = Number(user.follower_count);
        const followsCount = Number(user.following_count);
        if (!Number.isFinite(followersCount) || !Number.isFinite(followsCount)) {
            return { success: false, error: 'counts_missing' };
        }

        const data = {
            username: String(user.username).trim().toLowerCase(),
            followersCount,
            followsCount
        };
        igSetCachedUserCounts(pk, data);
        return {
            success: true,
            data: {
                username: data.username,
                followersCount: data.followersCount,
                followsCount: data.followsCount
            }
        };
    } catch (err) {
        profile.errorCount = Number(profile.errorCount || 0) + 1;
        if (profile.errorCount >= 5) profile.disabledUntil = Date.now() + (60 * 1000);
        return { success: false, error: err?.message || String(err) };
    } finally {
        profile.lastUsed = Date.now();
        unlockCookie(profile.ds_user_id);
    }
}

function safeParseJson(raw) {
    try {
        return JSON.parse(String(raw || '').trim());
    } catch (_) {
        return null;
    }
}

function pickNumber(obj, keys) {
    const o = (obj && typeof obj === 'object') ? obj : null;
    if (!o) return null;
    const list = Array.isArray(keys) ? keys : [];
    for (const k of list) {
        try {
            const v = o[k];
            const n = Number(v);
            if (Number.isFinite(n)) return n;
        } catch (_) {}
    }
    return null;
}

function pickString(obj, keys) {
    const o = (obj && typeof obj === 'object') ? obj : null;
    if (!o) return '';
    const list = Array.isArray(keys) ? keys : [];
    for (const k of list) {
        try {
            const v = o[k];
            const s = String(v || '').trim();
            if (s) return s;
        } catch (_) {}
    }
    return '';
}

function apifyFormatAxiosError(e) {
    try {
        const status = e?.response?.status;
        const data = e?.response?.data;
        const msg =
            (data && typeof data === 'object'
                ? (data?.error?.message || data?.error?.type || data?.message || data?.error)
                : (typeof data === 'string' ? data : null)) ||
            e?.message ||
            String(e);
        if (status) return `HTTP ${status}: ${String(msg)}`;
        return String(msg);
    } catch (_) {
        return String(e?.message || e || 'erro');
    }
}

function sleepMs(ms) {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms || 0))));
}

async function apifyGetRun(runIdRaw, apifyToken) {
    const runId = String(runIdRaw || '').trim();
    const token = String(apifyToken || '').trim();
    if (!runId) throw new Error('apify_run_id_missing');
    if (!token) throw new Error('apify_token_missing');
    const url = `https://api.apify.com/v2/actor-runs/${encodeURIComponent(runId)}?token=${encodeURIComponent(token)}`;
    let resp;
    try {
        resp = await axios.get(url, { timeout: 60000 });
    } catch (e) {
        throw new Error(apifyFormatAxiosError(e));
    }
    const run = resp && resp.data && resp.data.data ? resp.data.data : null;
    if (!run) throw new Error('apify_run_missing');
    return run;
}

async function apifyWaitForRun(runId, apifyToken, maxWaitMs) {
    const deadline = Date.now() + Math.max(0, Number(maxWaitMs || 0));
    let last = null;
    while (Date.now() < deadline) {
        last = await apifyGetRun(runId, apifyToken);
        const status = String(last?.status || '').toUpperCase();
        if (status === 'SUCCEEDED' || status === 'FAILED' || status === 'ABORTED' || status === 'TIMED-OUT') return last;
        await sleepMs(3000);
    }
    return last;
}

async function apifyRunAndGetDatasetId(actorIdRaw, input, apifyToken, waitForFinishMs) {
    const actorId = normalizeApifyActorId(actorIdRaw);
    if (!actorId) throw new Error('apify_actor_id_missing');
    if (!apifyToken) throw new Error('apify_token_missing');
    const waitMs = Math.max(0, Number(waitForFinishMs || 0));
    const waitSeconds = Math.min(300, Math.max(0, Math.floor(waitMs / 1000)));
    const url = `https://api.apify.com/v2/acts/${encodeURIComponent(actorId)}/runs?waitForFinish=${encodeURIComponent(String(waitSeconds))}&token=${encodeURIComponent(String(apifyToken))}`;
    let resp;
    try {
        resp = await axios.post(url, input || {}, { timeout: Math.min(330000, Math.max(15000, waitMs + 45000)) });
    } catch (e) {
        throw new Error(apifyFormatAxiosError(e));
    }
    const run = resp && resp.data && resp.data.data ? resp.data.data : null;
    const runId = run && run.id ? String(run.id) : '';
    const datasetId = run && run.defaultDatasetId ? String(run.defaultDatasetId) : '';
    let status = run && run.status ? String(run.status) : '';
    if (!datasetId) throw new Error('apify_dataset_missing');
    const statusUpper = String(status || '').toUpperCase();
    if (statusUpper && statusUpper !== 'SUCCEEDED' && statusUpper !== 'TIMED-OUT' && statusUpper !== 'RUNNING' && statusUpper !== 'READY' && statusUpper !== 'STARTING') {
        throw new Error(`apify_run_status_${String(status || '').toLowerCase()}`);
    }
    if (runId && (statusUpper === 'READY' || statusUpper === 'STARTING')) {
        const extraWaitMs = Math.min(480000, waitMs + 180000);
        const waited = await apifyWaitForRun(runId, apifyToken, extraWaitMs);
        status = waited && waited.status ? String(waited.status) : status;
        const finalStatusUpper = String(status || '').toUpperCase();
        if (finalStatusUpper === 'READY') {
            throw new Error('Apify ainda está na fila (READY). Tente novamente em alguns minutos.');
        }
        if (finalStatusUpper && finalStatusUpper !== 'SUCCEEDED' && finalStatusUpper !== 'TIMED-OUT' && finalStatusUpper !== 'RUNNING') {
            throw new Error(`apify_run_status_${String(status || '').toLowerCase()}`);
        }
    }
    return datasetId;
}

async function apifyFetchDatasetItems(datasetIdRaw, apifyToken, maxItems) {
    const datasetId = String(datasetIdRaw || '').trim();
    if (!datasetId) return [];
    const token = String(apifyToken || '').trim();
    if (!token) throw new Error('apify_token_missing');
    const cap = Math.max(1, Number(maxItems || 1));
    const out = [];
    const limit = 250;
    let offset = 0;
    while (out.length < cap) {
        const url = `https://api.apify.com/v2/datasets/${encodeURIComponent(datasetId)}/items?clean=true&format=json&limit=${limit}&offset=${offset}&token=${encodeURIComponent(token)}`;
        let resp;
        try {
            resp = await axios.get(url, { timeout: 60000 });
        } catch (e) {
            throw new Error(apifyFormatAxiosError(e));
        }
        const items = Array.isArray(resp?.data) ? resp.data : [];
        for (const it of items) {
            out.push(it);
            if (out.length >= cap) break;
        }
        if (!items.length || items.length < limit) break;
        offset += limit;
    }
    return out;
}

app.get('/painel/consulta-perfil', requireAdmin, async (req, res) => {
    try {
        return res.render('painel_consulta_perfil', {
            error: null,
            result: null,
            input: {
                username: '',
                maxFollowers: 2000,
                followersMax: 10,
                followingMin: 50,
                maxAnalyze: 500,
                sortBy: 'followersAsc',
                followersActorId: process.env.APIFY_FOLLOWERS_ACTOR_ID || 'patient_discovery~instagram-followers-scraper---no-login',
                profilesActorId: process.env.APIFY_PROFILES_ACTOR_ID || 'apify~instagram-profile-scraper',
                cookiesJson: ''
            }
        });
    } catch (e) {
        return res.status(500).send(e?.message || String(e));
    }
});

app.post('/painel/consulta-perfil', requireAdmin, async (req, res) => {
    const usernameRaw = req?.body?.username;
    const maxFollowersRaw = req?.body?.maxFollowers;
    const followersMaxRaw = req?.body?.followersMax;
    const followingMinRaw = req?.body?.followingMin;
    const maxAnalyzeRaw = req?.body?.maxAnalyze;
    const sortByRaw = req?.body?.sortBy;
    const cookiesJsonRaw = req?.body?.cookiesJson;
    const followersActorRaw = req?.body?.followersActorId;
    const profilesActorRaw = req?.body?.profilesActorId;

    const username = normalizeInstaUsernameForScrape(usernameRaw);
    const maxFollowers = Math.min(100000, Math.max(1, parseInt(String(maxFollowersRaw || '2000'), 10) || 2000));
    const followersMax = Math.max(0, parseInt(String(followersMaxRaw || '10'), 10) || 10);
    const followingMin = Math.max(0, parseInt(String(followingMinRaw || '50'), 10) || 50);
    const maxAnalyze = Math.min(100000, Math.max(1, parseInt(String(maxAnalyzeRaw || '500'), 10) || 500));
    const sortBy = (String(sortByRaw || 'followersAsc') === 'ratioDesc') ? 'ratioDesc' : 'followersAsc';

    const followersActorId = String(followersActorRaw || process.env.APIFY_FOLLOWERS_ACTOR_ID || 'logical_scrapers~instagram-followers-scraper').trim();
    const profilesActorId = String(profilesActorRaw || process.env.APIFY_PROFILES_ACTOR_ID || 'apify~instagram-profile-scraper').trim();

    const inputEcho = {
        username: String(usernameRaw || ''),
        maxFollowers,
        followersMax,
        followingMin,
        maxAnalyze,
        sortBy,
        followersActorId,
        profilesActorId,
        cookiesJson: String(cookiesJsonRaw || '')
    };

    try {
        if (!username) throw new Error('username_invalid');
        if (!cookieProfiles || !cookieProfiles.length) throw new Error('Sem perfis de cookie configurados (instagramProfiles.json).');

        const targetInfo = await igFetchWebProfileInfo(username);
        if (!targetInfo || !targetInfo.success || !targetInfo.profile) {
            throw new Error(targetInfo?.error || 'Falha ao buscar perfil alvo');
        }

        if (targetInfo.profile.isPrivate) {
            throw new Error('Perfil privado: não é possível listar seguidores sem acesso (seguindo/permitido).');
        }

        if (!targetInfo.profile.id) {
            throw new Error('Falha ao identificar o ID do perfil alvo');
        }

        const followersResp = await igFetchFollowersBatchByUserId(targetInfo.profile.id, maxFollowers);
        const followers = Array.isArray(followersResp?.followers) ? followersResp.followers : [];
        if (!followers.length) throw new Error(followersResp?.error || 'nenhum_seguidor_encontrado');

        const toAnalyze = followers.slice(0, Math.min(maxAnalyze, followers.length));
        const concurrency = Math.max(1, Math.min(4, cookieProfiles.length || 3));
        const q = new PQueue({ concurrency, interval: 1000, intervalCap: concurrency });

        const byUsername = new Map();
        const tasks = toAnalyze.map((f) =>
            q.add(async () => {
                const pk = f && f.pk ? String(f.pk) : '';
                if (!pk) return;
                let r = await igFetchUserCountsById(pk);
                if (!r?.success && String(r?.error || '').toLowerCase() === 'cookie_locked') {
                    await sleepMs(250);
                    r = await igFetchUserCountsById(pk);
                }
                if (r && r.success && r.data && r.data.username) {
                    byUsername.set(String(r.data.username).toLowerCase(), {
                        username: String(r.data.username).toLowerCase(),
                        followersCount: Number(r.data.followersCount) || 0,
                        followsCount: Number(r.data.followsCount) || 0
                    });
                }
            })
        );

        await Promise.all(tasks);

        const suspicious = [];
        for (const f of toAnalyze) {
            const u = normalizeInstaUsernameForScrape(f?.username);
            if (!u) continue;
            const p = byUsername.get(u);
            if (!p) continue;
            if (p.followersCount < followersMax && p.followsCount >= followingMin) {
                const denom = p.followersCount > 0 ? p.followersCount : 1;
                suspicious.push({
                    username: p.username,
                    followersCount: p.followersCount,
                    followsCount: p.followsCount,
                    ratio: p.followsCount / denom
                });
            }
        }

        if (sortBy === 'ratioDesc') suspicious.sort((a, b) => Number(b.ratio || 0) - Number(a.ratio || 0));
        else suspicious.sort((a, b) => Number(a.followersCount || 0) - Number(b.followersCount || 0));

        const suspiciousLimited = suspicious.slice(0, 300);
        const suspiciousCount = suspicious.length;
        const profilesAnalyzed = byUsername.size;
        const suspiciousPct = profilesAnalyzed ? (suspiciousCount / profilesAnalyzed) * 100 : 0;

        return res.render('painel_consulta_perfil', {
            error: null,
            input: inputEcho,
            result: {
                target: targetInfo.profile.username || username,
                followersFetched: followers.length,
                profilesAnalyzed,
                suspiciousCount,
                suspiciousPct,
                followersMax,
                followingMin,
                suspicious: suspiciousLimited
            }
        });
    } catch (e) {
        const msg = String(e?.message || e || 'erro');
        return res.render('painel_consulta_perfil', {
            error: msg,
            result: null,
            input: inputEcho
        });
    }
});

app.get('/painel', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const settingsCol = await getCollection('settings');

    const view = String(req.query.view || 'dashboard');
    const sessionSelectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};

    const paidQuery = {
      $or: [
        { status: 'pago' },
        { 'woovi.status': 'pago' }
      ]
    };

    const toSP = (d) => new Date(d.getTime() - 3 * 60 * 60 * 1000);
    const nowUTC = new Date();
    const nowSP = toSP(nowUTC);
    const startOfTodaySP = new Date(Date.UTC(nowSP.getUTCFullYear(), nowSP.getUTCMonth(), nowSP.getUTCDate(), 0, 0, 0, 0));
    const startOfTomorrowSP = (() => { const d = new Date(startOfTodaySP); d.setUTCDate(d.getUTCDate() + 1); return d; })();

    const matchesPeriod = (o, period) => {
      const dateStr = o.createdAt || o.woovi?.paidAt || o.paidAt;
      if (!dateStr) return false;
      const orderDateUTC = new Date(dateStr);
      const orderDateSP = toSP(orderDateUTC);
      if (period === 'all') return true;
      if (period === 'today') return orderDateSP >= startOfTodaySP && orderDateSP < startOfTomorrowSP;
      if (period === 'last3days') { const start = new Date(startOfTodaySP); start.setUTCDate(start.getUTCDate() - 2); return orderDateSP >= start && orderDateSP < startOfTomorrowSP; }
      if (period === 'last7days') { const start = new Date(startOfTodaySP); start.setUTCDate(start.getUTCDate() - 6); return orderDateSP >= start && orderDateSP < startOfTomorrowSP; }
      if (period === 'thismonth') { const start = new Date(startOfTodaySP); start.setUTCDate(1); return orderDateSP >= start && orderDateSP < startOfTomorrowSP; }
      if (period === 'lastmonth') {
        const start = new Date(startOfTodaySP);
        start.setUTCMonth(start.getUTCMonth() - 1);
        start.setUTCDate(1);
        const end = new Date(startOfTodaySP);
        end.setUTCDate(1);
        return orderDateSP >= start && orderDateSP < end;
      }
      if (period === 'custom') {
        const startStr = req.query.startDate;
        const endStr = req.query.endDate;
        if (startStr && endStr) {
          const [sY, sM, sD] = startStr.split('-').map(Number);
          const start = new Date(Date.UTC(sY, sM - 1, sD, 0, 0, 0, 0));
          const [eY, eM, eD] = endStr.split('-').map(Number);
          const end = new Date(Date.UTC(eY, eM - 1, eD, 23, 59, 59, 999));
          return orderDateSP >= start && orderDateSP <= end;
        }
        return true;
      }
      return true;
    };

    if (view === 'ltv') {
      const period = 'all';
      const normalizePersonKey = (v) => String(v == null ? '' : v).trim().replace(/^@+/, '').toLowerCase();
      const isTestUser = (ig, customerName) => {
        const key1 = normalizePersonKey(ig);
        const key2 = normalizePersonKey(customerName);
        return key1 === 'biel' || key1 === 'virginia' || key1 === 'pedro' || key2 === 'biel' || key2 === 'virginia' || key2 === 'pedro';
      };
      const normalizeInstaUser = (v) => {
        const raw = String(v == null ? '' : v).trim();
        if (!raw) return '';
        const cleaned = raw.replace(/^@+/, '').replace(/\/+$/, '').split('/').pop().trim();
        return cleaned.toLowerCase();
      };
      const onlyDigits = (v) => String(v == null ? '' : v).replace(/\D/g, '');
      const normalizeEmail = (v) => {
        const raw = String(v == null ? '' : v).trim().toLowerCase();
        if (!raw) return '';
        const ok = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(raw);
        return ok ? raw : '';
      };
      const pickInfoFromArray = (arr, key) => {
        try {
          if (!Array.isArray(arr)) return '';
          const it = arr.find(x => x && x.key === key);
          return it && typeof it.value !== 'undefined' ? String(it.value) : '';
        } catch (_) {
          return '';
        }
      };
      const resolvePhone = (o) => {
        try {
          const c = o && o.customer ? o.customer : null;
          const direct = c && (c.phone || c.phone_number || c.phoneNumber) ? String(c.phone || c.phone_number || c.phoneNumber) : '';
          if (direct) return direct;
          const mapPaid = (o && o.additionalInfoMapPaid) ? o.additionalInfoMapPaid : null;
          const map = (o && o.additionalInfoMap) ? o.additionalInfoMap : null;
          const mp = mapPaid && (mapPaid.phone || mapPaid.telefone || mapPaid.celular) ? String(mapPaid.phone || mapPaid.telefone || mapPaid.celular) : '';
          if (mp) return mp;
          const m = map && (map.phone || map.telefone || map.celular) ? String(map.phone || map.telefone || map.celular) : '';
          if (m) return m;
          const ap = pickInfoFromArray(o && o.additionalInfoPaid, 'phone') || pickInfoFromArray(o && o.additionalInfoPaid, 'telefone') || pickInfoFromArray(o && o.additionalInfoPaid, 'celular');
          if (ap) return ap;
          const a = pickInfoFromArray(o && o.additionalInfo, 'phone') || pickInfoFromArray(o && o.additionalInfo, 'telefone') || pickInfoFromArray(o && o.additionalInfo, 'celular');
          if (a) return a;
        } catch (_) {}
        return '';
      };
      const resolveInsta = (o) => {
        try {
          const mapPaid = (o && o.additionalInfoMapPaid) ? o.additionalInfoMapPaid : null;
          const map = (o && o.additionalInfoMap) ? o.additionalInfoMap : null;
          const mp = mapPaid && (mapPaid.instagram_username || mapPaid.instauser || mapPaid.username || mapPaid.perfil) ? String(mapPaid.instagram_username || mapPaid.instauser || mapPaid.username || mapPaid.perfil) : '';
          if (mp) return mp;
          const m = map && (map.instagram_username || map.instauser || map.username || map.perfil) ? String(map.instagram_username || map.instauser || map.username || map.perfil) : '';
          if (m) return m;
          const direct = o && (o.instagramUsername || o.instauser) ? String(o.instagramUsername || o.instauser) : '';
          if (direct) return direct;
          const ap = pickInfoFromArray(o && o.additionalInfoPaid, 'instagram_username') || pickInfoFromArray(o && o.additionalInfoPaid, 'instauser') || pickInfoFromArray(o && o.additionalInfoPaid, 'username');
          if (ap) return ap;
          const a = pickInfoFromArray(o && o.additionalInfo, 'instagram_username') || pickInfoFromArray(o && o.additionalInfo, 'instauser') || pickInfoFromArray(o && o.additionalInfo, 'username');
          if (a) return a;
        } catch (_) {}
        return '';
      };
      const resolveEmail = (o) => {
        try {
          const c = o && o.customer ? o.customer : null;
          const direct = c && c.email ? String(c.email) : '';
          if (direct) return direct;
          const mapPaid = (o && o.additionalInfoMapPaid) ? o.additionalInfoMapPaid : null;
          const map = (o && o.additionalInfoMap) ? o.additionalInfoMap : null;
          const mp = mapPaid && mapPaid.email ? String(mapPaid.email) : '';
          if (mp) return mp;
          const m = map && map.email ? String(map.email) : '';
          if (m) return m;
          const ap = pickInfoFromArray(o && o.additionalInfoPaid, 'email');
          if (ap) return ap;
          const a = pickInfoFromArray(o && o.additionalInfo, 'email');
          if (a) return a;
        } catch (_) {}
        return '';
      };
      const resolveCustomerName = (o) => {
        try {
          const c = o && o.customer ? o.customer : null;
          const direct = c && c.name ? String(c.name) : '';
          if (direct) return direct;
          const mapPaid = (o && o.additionalInfoMapPaid) ? o.additionalInfoMapPaid : null;
          const map = (o && o.additionalInfoMap) ? o.additionalInfoMap : null;
          const mp = mapPaid && (mapPaid.nome || mapPaid.name) ? String(mapPaid.nome || mapPaid.name) : '';
          if (mp) return mp;
          const m = map && (map.nome || map.name) ? String(map.nome || map.name) : '';
          if (m) return m;
          const ap = pickInfoFromArray(o && o.additionalInfoPaid, 'nome') || pickInfoFromArray(o && o.additionalInfoPaid, 'name');
          if (ap) return ap;
          const a = pickInfoFromArray(o && o.additionalInfo, 'nome') || pickInfoFromArray(o && o.additionalInfo, 'name');
          if (a) return a;
        } catch (_) {}
        return '';
      };
      const resolveTypeKey = (o) => {
        try {
          let type = o.tipo || o.tipoServico;
          const mapPaid = (o && o.additionalInfoMapPaid) ? o.additionalInfoMapPaid : null;
          const map = (o && o.additionalInfoMap) ? o.additionalInfoMap : null;
          if (!type && mapPaid && mapPaid.tipo_servico) type = mapPaid.tipo_servico;
          if (!type && map && map.tipo_servico) type = map.tipo_servico;
          if (!type) type = pickInfoFromArray(o && o.additionalInfoPaid, 'tipo_servico') || pickInfoFromArray(o && o.additionalInfo, 'tipo_servico');
          const cat = (() => {
            const c1 = mapPaid && mapPaid.categoria_servico ? String(mapPaid.categoria_servico) : '';
            if (c1) return c1;
            const c2 = map && map.categoria_servico ? String(map.categoria_servico) : '';
            if (c2) return c2;
            return pickInfoFromArray(o && o.additionalInfoPaid, 'categoria_servico') || pickInfoFromArray(o && o.additionalInfo, 'categoria_servico');
          })();
          const categoryRaw = String(cat || '').toLowerCase().trim();
          const t = String(type || '').toLowerCase().trim();
          const category = (() => {
            if (categoryRaw.includes('segu')) return 'seguidores';
            if (categoryRaw.includes('curti')) return 'curtidas';
            return categoryRaw;
          })();
          if (category === 'curtidas' && t === 'mistos') return 'curtidas_mistos';
          if (category === 'curtidas' && t === 'organicos') return 'curtidas_organicas';
          if (category === 'seguidores' && t === 'mistos') return 'seguidores_mistos';
          if (category === 'seguidores' && t === 'brasileiros') return 'seguidores_brasileiros';
          if (category === 'seguidores' && t === 'organicos') return 'seguidores_organicos';
          if (!category && (t === 'mistos' || t === 'organicos' || t === 'brasileiros')) return `${t}_sem_categoria`;
          return t || '-';
        } catch (_) {
          return '-';
        }
      };
      const resolveOrderBumpsRaw = (o) => {
        try {
          const mapPaid = (o && o.additionalInfoMapPaid) ? o.additionalInfoMapPaid : null;
          const map = (o && o.additionalInfoMap) ? o.additionalInfoMap : null;
          if (mapPaid && typeof mapPaid.order_bumps !== 'undefined') return String(mapPaid.order_bumps || '').trim();
          if (map && typeof map.order_bumps !== 'undefined') return String(map.order_bumps || '').trim();
          const ap = pickInfoFromArray(o && o.additionalInfoPaid, 'order_bumps');
          if (ap) return String(ap || '').trim();
          const a = pickInfoFromArray(o && o.additionalInfo, 'order_bumps');
          if (a) return String(a || '').trim();
        } catch (_) {}
        return '';
      };
      const parseBumps = (raw) => {
        const text = String(raw || '');
        const parts = text.split(';').map(p => p.trim()).filter(Boolean);
        const res = { views: 0, likes: 0, comments: 0, upgrade: 0 };
        for (const p of parts) {
          const [kRaw, vRaw] = p.split(':');
          const k = String(kRaw || '').toLowerCase().trim();
          const v = Number(String(vRaw || '').replace(/[^\d]/g, '')) || 0;
          if (k === 'views') res.views = v;
          else if (k === 'likes') res.likes = v;
          else if (k === 'comments') res.comments = v;
          else if (k === 'upgrade') res.upgrade = v || 1;
        }
        return res;
      };
      const toMoney = (v) => {
        const raw = String(v == null ? '' : v).trim();
        if (!raw) return 0;
        let s = raw.replace(/[^\d,.\-]/g, '');
        const hasComma = s.includes(',');
        const hasDot = s.includes('.');
        if (hasComma && hasDot) {
          if (s.lastIndexOf(',') > s.lastIndexOf('.')) s = s.replace(/\./g, '').replace(/,/g, '.');
          else s = s.replace(/,/g, '');
        } else if (hasComma && !hasDot) {
          s = s.replace(/,/g, '.');
        }
        const n = Number(s);
        return Number.isFinite(n) ? n : 0;
      };

      const cursor = col.find(paidQuery, {
        projection: {
          _id: 1,
          createdAt: 1,
          paidAt: 1,
          woovi: 1,
          valueCents: 1,
          customer: 1,
          instagramUsername: 1,
          instauser: 1,
          tipo: 1,
          tipoServico: 1,
          additionalInfoMapPaid: 1,
          additionalInfoMap: 1,
          additionalInfoPaid: 1,
          additionalInfo: 1
        }
      });

      const customerAgg = new Map();
      const serviceAgg = new Map();
      let unknownCustomerOrders = 0;
      let consideredOrders = 0;
      let bumpOnlyOrders = 0;

      for await (const o of cursor) {
        const ig = normalizeInstaUser(resolveInsta(o));
        const customerName = String(resolveCustomerName(o) || '').trim();
        if (isTestUser(ig, customerName)) continue;

        consideredOrders += 1;

        const phoneDigits = onlyDigits(resolvePhone(o));
        const email = normalizeEmail(resolveEmail(o));
        const customerKey = ig ? `ig:${ig}` : (phoneDigits ? `ph:${phoneDigits}` : '');
        if (!customerKey) {
          unknownCustomerOrders += 1;
        } else {
          const username = ig ? (`@${ig}`) : (customerName ? customerName : '');
          const label = username || (phoneDigits ? (`+${phoneDigits}`) : '-');
          const cur = customerAgg.get(customerKey) || { orders: 0, spend: 0, label, username, phone: phoneDigits ? (`+${phoneDigits}`) : '', email };
          cur.orders += 1;
          if (!cur.username && username) cur.username = username;
          if (!cur.phone && phoneDigits) cur.phone = `+${phoneDigits}`;
          if (!cur.email && email) cur.email = email;

          let revenue = 0;
          if (o.valueCents) revenue = Number(o.valueCents) / 100;
          else if (o.woovi && o.woovi.paymentMethods && o.woovi.paymentMethods.pix && o.woovi.paymentMethods.pix.value) revenue = Number(o.woovi.paymentMethods.pix.value) / 100;
          const bumpTotalRaw = (o.additionalInfoMapPaid && o.additionalInfoMapPaid.order_bumps_total) ? o.additionalInfoMapPaid.order_bumps_total
            : ((o.additionalInfoMap && o.additionalInfoMap.order_bumps_total) ? o.additionalInfoMap.order_bumps_total
              : (pickInfoFromArray(o.additionalInfoPaid, 'order_bumps_total') || pickInfoFromArray(o.additionalInfo, 'order_bumps_total')));
          const bumpRevenue = toMoney(bumpTotalRaw);
          const totalPaid = (!o.valueCents && bumpRevenue) ? (Number(revenue || 0) + Number(bumpRevenue || 0)) : Number(revenue || 0);
          cur.spend += totalPaid;
          customerAgg.set(customerKey, cur);
        }

        const typeKey = resolveTypeKey(o);
        const bumpsRaw = resolveOrderBumpsRaw(o);
        const bumps = parseBumps(bumpsRaw);
        const hasBumps = (bumps.views || bumps.likes || bumps.comments || bumps.upgrade) ? true : false;
        const typeStr = String(typeKey || '').toLowerCase().trim();
        const typeIsEmpty = !typeStr || typeStr === '-' || typeStr === 'null' || typeStr === 'undefined';
        const typeIsBump = typeStr.includes('orderbump') || typeStr.includes('order_bump') || typeStr === 'bump';
        const bumpOnly = hasBumps && (typeIsEmpty || typeIsBump);
        if (bumpOnly) bumpOnlyOrders += 1;
        if (!bumpOnly) serviceAgg.set(typeKey, (serviceAgg.get(typeKey) || 0) + 1);
      }

      const totalCustomers = customerAgg.size;
      let repeatCustomers = 0;
      for (const v of customerAgg.values()) if (v && v.orders > 1) repeatCustomers += 1;
      const repeatCustomerPct = totalCustomers > 0 ? (repeatCustomers / totalCustomers) * 100 : 0;

      const topUsersByOrders = Array.from(customerAgg.values())
        .sort((a, b) => (b.orders - a.orders) || (b.spend - a.spend))
        .slice(0, 5);

      const topUsersBySpend = Array.from(customerAgg.values())
        .sort((a, b) => (b.spend - a.spend) || (b.orders - a.orders))
        .slice(0, 5);

      const prettyServiceLabel = (typeKey) => {
        const raw = String(typeKey == null ? '' : typeKey).trim();
        if (!raw || raw === '-' || raw === 'null' || raw === 'undefined') return '-';
        const key = raw.toLowerCase().trim();
        if (key === 'seguidores_organicos') return 'Seguidores orgânicos';
        if (key === 'seguidores_mistos') return 'Seguidores mistos';
        if (key === 'seguidores_brasileiros') return 'Seguidores brasileiros';
        if (key === 'curtidas_organicas') return 'Curtidas orgânicas';
        if (key === 'curtidas_mistos') return 'Curtidas mistos';
        if (key === 'mistos_sem_categoria') return 'Mistos (sem categoria)';
        if (key === 'organicos_sem_categoria') return 'Orgânicos (sem categoria)';
        if (key === 'brasileiros_sem_categoria') return 'Brasileiros (sem categoria)';
        const s = raw.replace(/_/g, ' ');
        return s.charAt(0).toUpperCase() + s.slice(1);
      };

      const serviceEntries = Array.from(serviceAgg.entries())
        .map(([type, count]) => ({ type, count }))
        .filter(it => it && it.type && it.type !== '-' && it.type !== 'null' && it.type !== 'undefined');
      serviceEntries.sort((a, b) => b.count - a.count);
      const topService = serviceEntries.length ? { type: prettyServiceLabel(serviceEntries[0].type), count: serviceEntries[0].count } : null;
      const colors = ['#2563eb', '#7c3aed', '#16a34a', '#f59e0b', '#dc2626', '#64748b', '#06b6d4', '#a855f7'];
      const top5Entries = serviceEntries.slice(0, 5);
      const totalForPie = top5Entries.reduce((acc, it) => acc + (it.count || 0), 0);
      const servicePie = top5Entries.map((it, idx) => ({
        label: prettyServiceLabel(it.type),
        count: it.count,
        color: colors[idx % colors.length],
        pct: totalForPie > 0 ? (it.count / totalForPie) * 100 : 0
      }));

      const customerRows = Array.from(customerAgg.values())
        .map(v => ({
          label: String(v && v.label ? v.label : ''),
          username: String(v && v.username ? v.username : ''),
          phone: String(v && v.phone ? v.phone : ''),
          email: String(v && v.email ? v.email : ''),
          orders: Number(v && v.orders ? v.orders : 0),
          spend: Number(v && v.spend ? v.spend : 0)
        }))
        .sort((a, b) => (b.orders - a.orders) || (b.spend - a.spend) || String(a.label).localeCompare(String(b.label)));

      return res.render('painel', {
        view: 'ltv',
        period,
        totalTransactions: consideredOrders,
        totalCustomers,
        repeatCustomers,
        repeatCustomerPct,
        topUsersByOrders,
        topUsersBySpend,
        topService,
        servicePie,
        unknownCustomerOrders,
        bumpOnlyOrders,
        customerRows
      });
    }

    const unknownProviderQuery = {
      $or: [
        { 'fama24h.status': 'unknown' },
        { 'fama24h.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fama24h.status': 'error' },
        { 'fama24h.error': { $exists: true, $ne: '' } },
        { 'fornecedor_social.status': 'unknown' },
        { 'fornecedor_social.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fornecedor_social.status': 'error' },
        { 'fornecedor_social.error': { $exists: true, $ne: '' } },
        { 'worldsmm_comments.status': 'unknown' },
        { 'worldsmm_comments.orderId': { $in: ['unknown', 'unknow'] } },
        { 'worldsmm_comments.status': 'error' },
        { 'worldsmm_comments.error': { $regex: 'timeout', $options: 'i' } },
        {
          $and: [
            { worldsmm_comments: { $exists: true } },
            {
              $or: [
                { 'worldsmm_comments.orderId': { $exists: false } },
                { 'worldsmm_comments.orderId': '' },
                { 'worldsmm_comments.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } } },
                { 'additionalInfoMap.order_bumps': { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } },
                { 'additionalInfoMapPaid.order_bumps': { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } }
              ]
            },
            { $or: [{ worldsmm_comments: { $exists: false } }, { 'worldsmm_comments.orderId': { $exists: false } }] }
          ]
        },
        { 'fama24h_views.status': 'unknown' },
        { 'fama24h_views.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fama24h_views.status': 'error' },
        { 'fama24h_views.error': { $exists: true } },
        {
          $and: [
            { fama24h_views: { $exists: true } },
            {
              $or: [
                { 'fama24h_views.orderId': { $exists: false } },
                { 'fama24h_views.orderId': '' },
                { 'fama24h_views.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } } },
                { 'additionalInfoMap.order_bumps': { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } },
                { 'additionalInfoMapPaid.order_bumps': { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } }
              ]
            },
            {
              $or: [
                { fama24h_views: { $exists: false } },
                { 'fama24h_views.orderId': { $exists: false } },
                { 'fama24h_views.orderId': '' },
                { 'fama24h_views.orderId': null }
              ]
            }
          ]
        },
        { 'fama24h_likes.status': 'unknown' },
        { 'fama24h_likes.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fama24h_likes.status': 'error' },
        { 'fama24h_likes.error': { $exists: true } },
        {
          $and: [
            { fama24h_likes: { $exists: true } },
            {
              $or: [
                { 'fama24h_likes.orderId': { $exists: false } },
                { 'fama24h_likes.orderId': '' },
                { 'fama24h_likes.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } } },
                { 'additionalInfoMap.order_bumps': { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } },
                { 'additionalInfoMapPaid.order_bumps': { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } }
              ]
            },
            {
              $or: [
                { fama24h_likes: { $exists: false } },
                { 'fama24h_likes.orderId': { $exists: false } },
                { 'fama24h_likes.orderId': '' },
                { 'fama24h_likes.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { tipo: { $regex: '^(seguidores|curtidas|visualizacoes)', $options: 'i' } },
                { tipoServico: { $regex: '^(seguidores|curtidas|visualizacoes)', $options: 'i' } },
                { additionalInfo: { $elemMatch: { key: 'tipo_servico', value: { $regex: '^(seguidores|curtidas|visualizacoes)', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'tipo_servico', value: { $regex: '^(seguidores|curtidas|visualizacoes)', $options: 'i' } } } },
                { 'additionalInfoMap.tipo_servico': { $regex: '^(seguidores|curtidas|visualizacoes)', $options: 'i' } },
                { 'additionalInfoMapPaid.tipo_servico': { $regex: '^(seguidores|curtidas|visualizacoes)', $options: 'i' } }
              ]
            },
            {
              $or: [
                { fama24h: { $exists: false } },
                { 'fama24h.orderId': { $exists: false } },
                { 'fama24h.orderId': '' },
                { 'fama24h.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { tipo: { $regex: '^seguidores_organicos', $options: 'i' } },
                { tipoServico: { $regex: '^seguidores_organicos', $options: 'i' } },
                { additionalInfo: { $elemMatch: { key: 'tipo_servico', value: { $regex: '^seguidores_organicos', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'tipo_servico', value: { $regex: '^seguidores_organicos', $options: 'i' } } } },
                { 'additionalInfoMap.tipo_servico': { $regex: '^seguidores_organicos', $options: 'i' } },
                { 'additionalInfoMapPaid.tipo_servico': { $regex: '^seguidores_organicos', $options: 'i' } }
              ]
            },
            {
              $or: [
                { fornecedor_social: { $exists: false } },
                { 'fornecedor_social.orderId': { $exists: false } },
                { 'fornecedor_social.orderId': '' },
                { 'fornecedor_social.orderId': null }
              ]
            }
          ]
        }
      ]
    };

    const query = view === 'unknown_orderid'
      ? { $and: [paidQuery, unknownProviderQuery] }
      : paidQuery;

    const orders = await col.find(query).sort({ createdAt: -1 }).limit(2000).toArray();

    let costSettingsDoc = await settingsCol.findOne({ _id: 'cost_settings' });
    const costSettings = Object.assign({}, DEFAULT_COST_SETTINGS, (costSettingsDoc && costSettingsDoc.values) || {});

    // Filter logic
    const periodDefault = view === 'unknown_orderid' ? 'all' : 'today';
    const period = req.query.period || periodDefault;
    const ignoreBumps = String(req.query.ignoreBumps || '') === '1';
    const toggleIgnoreBumpsUrl = (() => {
      try {
        const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'http');
        const host = String(req.get('host') || 'localhost');
        const u = new URL(String(req.originalUrl || '/painel'), `${proto}://${host}`);
        if (ignoreBumps) u.searchParams.delete('ignoreBumps');
        else u.searchParams.set('ignoreBumps', '1');
        return u.pathname + (u.search || '');
      } catch (_) {
        return ignoreBumps ? '/painel' : '/painel?ignoreBumps=1';
      }
    })();
    const startOfTodayUtc = new Date(Date.UTC(nowSP.getUTCFullYear(), nowSP.getUTCMonth(), nowSP.getUTCDate(), 3, 0, 0, 0));
    const startOfTomorrowUtc = (() => { const d = new Date(startOfTodayUtc); d.setUTCDate(d.getUTCDate() + 1); return d; })();

    let filteredOrders = orders.filter(o => {
      const dateStr = o.createdAt || o.woovi?.paidAt || o.paidAt;
      if (!dateStr) return false;
      
      const orderDateUTC = new Date(dateStr);
      const orderDateSP = toSP(orderDateUTC);

      if (period === 'all') {
        return true;
      } else if (period === 'today') {
        return orderDateSP >= startOfTodaySP;
      } else if (period === 'last3days') {
        const start = new Date(startOfTodaySP);
        start.setUTCDate(start.getUTCDate() - 2); 
        return orderDateSP >= start;
      } else if (period === 'last7days') {
        const start = new Date(startOfTodaySP);
        start.setUTCDate(start.getUTCDate() - 6);
        return orderDateSP >= start;
      } else if (period === 'thismonth') {
        const start = new Date(startOfTodaySP);
        start.setUTCDate(1);
        return orderDateSP >= start;
      } else if (period === 'lastmonth') {
        const start = new Date(startOfTodaySP);
        start.setUTCMonth(start.getUTCMonth() - 1);
        start.setUTCDate(1);
        const end = new Date(startOfTodaySP);
        end.setUTCDate(1);
        return orderDateSP >= start && orderDateSP < end;
      } else if (period === 'custom') {
        const startStr = req.query.startDate;
        const endStr = req.query.endDate;
        if (startStr && endStr) {
          const [sY, sM, sD] = startStr.split('-').map(Number);
          const start = new Date(Date.UTC(sY, sM - 1, sD, 0, 0, 0, 0));
          
          const [eY, eM, eD] = endStr.split('-').map(Number);
          const end = new Date(Date.UTC(eY, eM - 1, eD, 23, 59, 59, 999));
          
          return orderDateSP >= start && orderDateSP <= end;
        }
        return true;
      }
      return true;
    });

    if (view === 'unknown_orderid') {
      const resolveQty = (o) => {
        if (o.quantidade) return Number(o.quantidade);
        if (o.qtd) return Number(o.qtd);
        if (o.additionalInfoPaid) {
          const qItem = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid.find(i => i && i.key === 'quantidade') : null;
          if (qItem) return Number(qItem.value);
        }
        if (o.additionalInfoMap && o.additionalInfoMap.quantidade) return Number(o.additionalInfoMap.quantidade);
        return 0;
      };

      const resolveOrderBumps = (o) => {
        try {
          if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid.order_bumps === 'string') return o.additionalInfoMapPaid.order_bumps;
          const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const paidItem = arrPaid.find(i => i && i.key === 'order_bumps');
          if (paidItem && typeof paidItem.value === 'string') return paidItem.value;
          if (o.additionalInfoMap && typeof o.additionalInfoMap.order_bumps === 'string') return o.additionalInfoMap.order_bumps;
          const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
          const item = arr.find(i => i && i.key === 'order_bumps');
          if (item && typeof item.value === 'string') return item.value;
        } catch (_) {}
        return '';
      };

      const parseBumps = (raw) => {
        const text = String(raw || '');
        const parts = text.split(';').map(p => p.trim()).filter(Boolean);
        const res = { views: 0, likes: 0, comments: 0, upgrade: 0 };
        for (const p of parts) {
          const [kRaw, vRaw] = p.split(':');
          const k = String(kRaw || '').toLowerCase().trim();
          const v = Number(String(vRaw || '').replace(/[^\d]/g, '')) || 0;
          if (k === 'views') res.views = v;
          else if (k === 'likes') res.likes = v;
          else if (k === 'comments') res.comments = v;
          else if (k === 'upgrade') res.upgrade = v || 1;
        }
        return res;
      };

      const isUnknownToken = (v) => {
        const s = String(v || '').toLowerCase().trim();
        return s === 'unknown' || s === 'unknow';
      };

      const isUnknownForFamaBump = (subdoc) => {
        if (!subdoc) return true;
        const st = (typeof subdoc.status !== 'undefined') ? String(subdoc.status) : '';
        const oid = (typeof subdoc.orderId !== 'undefined') ? String(subdoc.orderId) : '';
        const err = (typeof subdoc.error !== 'undefined') ? String(subdoc.error) : '';
        const stLower = st.toLowerCase();
        const errLower = err.toLowerCase();
        if (!String(oid || '').trim()) return true;
        return isUnknownToken(st) || isUnknownToken(oid) || stLower === 'error' || errLower.includes('timeout') || (!!err && String(err || '').trim() !== '');
      };

      const getUpgradeAddQtd = (tipo, base) => {
        try {
          const t0 = String(tipo || '').toLowerCase().trim();
          const t = t0.startsWith('seguidores_') ? t0.replace(/^seguidores_/, '') : t0;
          const b = Number(base) || 0;
          if (!b) return 0;
          if (t === 'organicos' && b === 50) return 50;
          if ((t === 'brasileiros' || t === 'organicos') && b === 1000) return 1000;
          const upsellTargets = { 50: 150, 150: 300, 500: 700, 1000: 2000, 3000: 4000, 5000: 7500, 10000: 15000 };
          const target = upsellTargets[b];
          if (!target) return 0;
          return Number(target) - b;
        } catch (_) {
          return 0;
        }
      };

      const resolveQtyWithUpgrade = (o) => {
        const baseQty = resolveQty(o);
        if (!baseQty) return baseQty;
        const bumpsStr = String(resolveOrderBumps(o) || '').toLowerCase();
        const m = bumpsStr.match(/(?:^|;)\s*upgrade\s*:\s*(\d+)/i);
        const upgradeQty = m ? Number(m[1]) : 0;
        if (!upgradeQty) return baseQty;
        const category = String(resolveCategory(o) || '').toLowerCase();
        const type = String(resolveType(o) || '').toLowerCase();
        const serviceKey = String(resolveServiceKey(o) || '').toLowerCase();
        const tipoRaw = String(o && (o.tipo || o.tipoServico) ? (o.tipo || o.tipoServico) : '').toLowerCase();
        const isFollowers =
          category.includes('seguidores') ||
          type.includes('seguidores') ||
          serviceKey.includes('seguidores') ||
          tipoRaw.includes('seguidores');
        if (!isFollowers) return baseQty;
        const tipo = resolveType(o);
        const add = getUpgradeAddQtd(tipo, baseQty);
        return baseQty + add;
      };

      const resolvePhone = (o) => {
        try {
          const phoneFromCustomer = o && o.customer && (o.customer.phone || o.customer.telefone || o.customer.whatsapp);
          if (phoneFromCustomer) return String(phoneFromCustomer);

          const phoneFromMap = o && o.additionalInfoMap && (o.additionalInfoMap.phone || o.additionalInfoMap.telefone || o.additionalInfoMap.whatsapp);
          if (phoneFromMap) return String(phoneFromMap);

          const direct = o && (o.phone || o.telefone || o.whatsapp || o.customerPhone);
          if (direct) return String(direct);

          if (o && o.additionalInfoPaid) {
            const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
            const pItem = arr.find(i => i && (i.key === 'phone' || i.key === 'telefone' || i.key === 'whatsapp' || i.key === 'celular'));
            if (pItem && typeof pItem.value !== 'undefined') return String(pItem.value);
          }
        } catch (_) {}
        return '';
      };

      const resolveType = (o) => {
        let type = o.tipo || o.tipoServico;
        if (!type && o.additionalInfoPaid) {
          const tItem = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid.find(i => i && i.key === 'tipo_servico') : null;
          if (tItem) type = tItem.value;
        }
        if (!type && o.additionalInfoMap && o.additionalInfoMap.tipo_servico) type = o.additionalInfoMap.tipo_servico;
        return String(type || '');
      };

      const resolveUser = (o) => {
        if (o.instaUser) return String(o.instaUser);
        if (o.instauser) return String(o.instauser);
        if (o.instagramUser) return String(o.instagramUser);
        if (o.additionalInfoMap && (o.additionalInfoMap.instauser || o.additionalInfoMap.instaUser)) return String(o.additionalInfoMap.instauser || o.additionalInfoMap.instaUser);
        if (o.additionalInfoPaid) {
          const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const uItem = arr.find(i => i && (i.key === 'instauser' || i.key === 'instaUser' || i.key === 'instagram_username' || i.key === 'user'));
          if (uItem && typeof uItem.value !== 'undefined') return String(uItem.value);
        }
        return '';
      };

      const resolveCategory = (o) => {
        let category = '';
        if (o.additionalInfoPaid) {
          const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const cItem = arr.find(i => i && i.key === 'categoria_servico');
          if (cItem && typeof cItem.value === 'string') category = cItem.value;
        }
        if (!category && o.additionalInfoMap && typeof o.additionalInfoMap.categoria_servico === 'string') {
          category = o.additionalInfoMap.categoria_servico;
        }
        return String(category || '');
      };

      const resolveServiceKey = (o) => {
        let category = String(resolveCategory(o) || '').toLowerCase().trim();
        let type = String(resolveType(o) || '').toLowerCase().trim();

        if (!category) {
          if (type.startsWith('curtidas_')) {
            category = 'curtidas';
            type = type.replace(/^curtidas_/, '');
          } else if (type.startsWith('seguidores_')) {
            category = 'seguidores';
            type = type.replace(/^seguidores_/, '');
          } else if (type.startsWith('visualizacoes_')) {
            category = 'visualizacoes';
            type = type.replace(/^visualizacoes_/, '');
          }
        }

        if (type === 'visualizacoes_reels') {
          category = 'visualizacoes';
          type = 'reels';
        }

        if (category === 'curtidas' && type === 'mistos') return 'curtidas_mistos';
        if (category === 'seguidores' && type === 'mistos') return 'seguidores_mistos';
        if (category === 'curtidas' && type === 'organicos') return 'curtidas_organicos';
        if (category === 'seguidores' && type === 'organicos') return 'seguidores_organicos';

        if (category && type) return `${category}_${type}`;
        return type || category || '';
      };

      const resolveServiceLabel = (o) => {
        const key = resolveServiceKey(o);
        return String(key || '').replace(/_/g, ' ').trim();
      };

      const extractInfoAny = (o, key) => {
        try {
          if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
          if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return o.additionalInfoMap[key];
          const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const itemPaid = arrPaid.find(i => i && i.key === key);
          if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
          const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
          const item = arr.find(i => i && i.key === key);
          if (item && typeof item.value !== 'undefined') return item.value;
        } catch (_) {}
        return '';
      };

      const normalizeUrl = (u) => {
        const s0 = String(u || '').trim();
        if (!s0) return '';
        if (/^https?:\/\//i.test(s0)) return s0;
        if (/^www\./i.test(s0)) return `https://${s0}`;
        if (/instagram\.com\//i.test(s0)) return `https://${s0.replace(/^\/+/, '')}`;
        return s0;
      };

      const sanitizeInstagramPostLink = (u) => {
        const s0 = normalizeUrl(u);
        let v = String(s0 || '').trim();
        if (!v) return '';
        v = v.split('#')[0].split('?')[0];
        const m = v.match(/^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/([A-Za-z0-9_-]+)\/?$/i);
        if (!m) return '';
        const kind = String(m[2] || '').toLowerCase();
        const code = String(m[3] || '');
        if (!kind || !code) return '';
        return `https://www.instagram.com/${kind}/${code}/`;
      };

      const resolvePostLinkViews = (o) => {
        const sessionLink = sessionSelectedFor && sessionSelectedFor.views && sessionSelectedFor.views.link ? String(sessionSelectedFor.views.link) : '';
        const candidates = [
          sessionLink,
          o?.fama24h_views?.requestPayload?.link,
          extractInfoAny(o, 'orderbump_post_views'),
          extractInfoAny(o, 'post_link')
        ];
        for (const c of candidates) {
          const url = sanitizeInstagramPostLink(c);
          if (url) return url;
        }
        return '';
      };

      const resolvePostLinkLikes = (o) => {
        const sessionLink = sessionSelectedFor && sessionSelectedFor.likes && sessionSelectedFor.likes.link ? String(sessionSelectedFor.likes.link) : '';
        const candidates = [
          sessionLink,
          o?.fama24h_likes?.requestPayload?.link,
          extractInfoAny(o, 'orderbump_post_likes'),
          extractInfoAny(o, 'post_link')
        ];
        for (const c of candidates) {
          const url = sanitizeInstagramPostLink(c);
          if (url) return url;
        }
        return '';
      };

      const resolvePostLinkComments = (o) => {
        const sessionLink = sessionSelectedFor && sessionSelectedFor.comments && sessionSelectedFor.comments.link ? String(sessionSelectedFor.comments.link) : '';
        const candidates = [
          sessionLink,
          o?.worldsmm_comments?.requestPayload?.link,
          extractInfoAny(o, 'orderbump_post_comments'),
          extractInfoAny(o, 'post_link')
        ];
        for (const c of candidates) {
          const url = sanitizeInstagramPostLink(c);
          if (url) return url;
        }
        return '';
      };

      const normalizeUsernameKey = (u) => {
        const s0 = String(u || '').trim();
        if (!s0) return '';
        const s1 = s0.startsWith('@') ? s0.slice(1) : s0;
        return s1.toLowerCase().trim();
      };

      const resolveIsPrivate = (o) => {
        try {
          if (o && o.isPrivate === true) return true;
          if (o && o.profilePrivacy && o.profilePrivacy.isPrivate === true) return true;
          if (o && o.additionalInfoMap && (o.additionalInfoMap.isPrivate === true || o.additionalInfoMap.isPrivate === 'true')) return true;
          if (o && o.additionalInfoMapPaid && (o.additionalInfoMapPaid.isPrivate === true || o.additionalInfoMapPaid.isPrivate === 'true')) return true;
          const arrPaid = Array.isArray(o && o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const iPaid = arrPaid.find(i => i && String(i.key || '').trim().toLowerCase() === 'isprivate');
          if (iPaid && String(iPaid.value || '').trim().toLowerCase() === 'true') return true;
          const arr = Array.isArray(o && o.additionalInfo) ? o.additionalInfo : [];
          const i = arr.find(i => i && String(i.key || '').trim().toLowerCase() === 'isprivate');
          if (i && String(i.value || '').trim().toLowerCase() === 'true') return true;
        } catch (_) {}
        return false;
      };

      const normalizePhoneKey = (p) => String(p || '').replace(/\D/g, '');

      const privateKeySetUsers = new Set();
      const privateKeySetPhones = new Set();
      try {
        const privateQuery = {
          $and: [
            paidQuery,
            {
              $or: [
                { isPrivate: true },
                { 'profilePrivacy.isPrivate': true },
                { 'additionalInfoMap.isPrivate': true },
                { 'additionalInfoMap.isPrivate': 'true' },
                { 'additionalInfoMapPaid.isPrivate': true },
                { 'additionalInfoMapPaid.isPrivate': 'true' }
              ]
            }
          ]
        };
        const privateOrders = await col.find(privateQuery, {
          projection: { instauser: 1, instaUser: 1, instagramUsername: 1, instagramUser: 1, additionalInfoPaid: 1, additionalInfo: 1, additionalInfoMap: 1, additionalInfoMapPaid: 1, customer: 1, phone: 1, telefone: 1, whatsapp: 1, customerPhone: 1 }
        }).sort({ createdAt: -1 }).limit(5000).toArray();
        for (const po of privateOrders) {
          const uKey = normalizeUsernameKey(resolveUser(po));
          if (uKey) privateKeySetUsers.add(uKey);
          const pKey = normalizePhoneKey(resolvePhone(po));
          if (pKey) privateKeySetPhones.add(pKey);
        }
      } catch (_) {}

      let filteredUnknownOrders = filteredOrders;
      const searchPhone = String(req.query.searchPhone || '').trim();
      if (searchPhone) {
        const needleDigits = searchPhone.replace(/\D/g, '');
        filteredUnknownOrders = filteredUnknownOrders.filter(o => {
          const phone = resolvePhone(o);
          if (!phone) return false;
          if (needleDigits) return String(phone).replace(/\D/g, '').includes(needleDigits);
          return String(phone).toLowerCase().includes(searchPhone.toLowerCase());
        });
      }

      const hasWorldsmmComments = (o) => {
        try {
          const bumps = parseBumps(resolveOrderBumps(o));
          if (bumps.comments > 0) return true;
        } catch (_) {}
        return !!(o && o.worldsmm_comments && (typeof o.worldsmm_comments.status !== 'undefined' || typeof o.worldsmm_comments.orderId !== 'undefined'));
      };

      const serviceFilter = String(req.query.service || '').toLowerCase().trim();
      const issueFilter = String(req.query.issue || '').toLowerCase().trim();
      if (serviceFilter && serviceFilter !== 'all') {
        if (serviceFilter === 'comentarios') {
          filteredUnknownOrders = filteredUnknownOrders.filter(o => hasWorldsmmComments(o));
        } else {
          filteredUnknownOrders = filteredUnknownOrders.filter(o => resolveServiceKey(o) === serviceFilter);
        }
      }

      const expectedProviderFromServiceKey = (serviceKey) => {
        const k = String(serviceKey || '').toLowerCase().trim();
        if (!k) return '';
        if (k === 'seguidores_organicos') return 'fornecedor_social';
        if (k.startsWith('seguidores')) return 'fama24h';
        if (k.startsWith('curtidas')) return 'fama24h';
        if (k.startsWith('visualizacoes')) return 'fama24h';
        return '';
      };

      const isUnknownForProvider = (o, provider) => {
        if (provider === 'fama24h') {
          const doc = o && o.fama24h ? o.fama24h : null;
          if (!doc) return true;
          const st = typeof doc.status !== 'undefined' ? doc.status : '';
          const oid = typeof doc.orderId !== 'undefined' ? doc.orderId : '';
          const oidStr = String(oid || '').trim().toLowerCase();
          if (!oidStr) return true;
          if (oidStr === 'null' || oidStr === 'undefined') return true;
          return isUnknownToken(st) || isUnknownToken(oid);
        }
        if (provider === 'fornecedor_social') {
          const doc = o && o.fornecedor_social ? o.fornecedor_social : null;
          if (!doc) return true;
          const st = typeof doc.status !== 'undefined' ? doc.status : '';
          const oid = typeof doc.orderId !== 'undefined' ? doc.orderId : '';
          const oidStr = String(oid || '').trim().toLowerCase();
          if (!oidStr) return true;
          if (oidStr === 'null' || oidStr === 'undefined') return true;
          return isUnknownToken(st) || isUnknownToken(oid);
        }
        return false;
      };

      const hasValidOrderId = (v) => {
        const s = String(v || '').trim();
        if (!s) return false;
        return !isUnknownToken(s);
      };

      const isErrorForProvider = (o, provider) => {
        const doc = (o && o[provider]) ? o[provider] : null;
        if (!doc) return false;
        const stLower = String(doc.status || '').toLowerCase().trim();
        const err = (typeof doc.error !== 'undefined') ? String(doc.error || '') : '';
        if (stLower === 'error') return true;
        if (err && err.trim() !== '') return true;
        return false;
      };

      const classifyFamaBumpIssue = (subdoc) => {
        const stLower = subdoc && typeof subdoc.status !== 'undefined' ? String(subdoc.status || '').toLowerCase().trim() : '';
        const oid = subdoc && typeof subdoc.orderId !== 'undefined' ? String(subdoc.orderId || '') : '';
        const err = subdoc && typeof subdoc.error !== 'undefined' ? String(subdoc.error || '') : '';
        const errLower = err.toLowerCase();
        const hasId = hasValidOrderId(oid);
        if (hasId && (stLower === 'created' || stLower === 'processing' || stLower === 'pending' || stLower === 'completed')) return 'ok';
        if (isUnknownToken(stLower) || isUnknownToken(oid)) return 'unknown';
        if (stLower === 'error') return 'error';
        if (err && err.trim() !== '') return 'error';
        if (!hasId) return 'unknown';
        return 'unknown';
      };

      const classifyWorldsmmCommentsIssue = (o) => {
        const doc = o && o.worldsmm_comments ? o.worldsmm_comments : null;
        if (!doc) return 'unknown';
        const stLower = typeof doc.status !== 'undefined' ? String(doc.status || '').toLowerCase().trim() : '';
        const oid = typeof doc.orderId !== 'undefined' ? String(doc.orderId || '') : '';
        const err = typeof doc.error !== 'undefined' ? String(doc.error || '') : '';
        const errLower = err.toLowerCase();
        const hasId = hasValidOrderId(oid);
        if (hasId && (stLower === 'created' || stLower === 'processing' || stLower === 'pending' || stLower === 'completed')) return 'ok';
        if (isUnknownToken(stLower) || isUnknownToken(oid)) return 'unknown';
        if (stLower === 'error') return 'error';
        if (!hasId && errLower.includes('timeout')) return 'error';
        if (!hasId && err && err.trim() !== '') return 'error';
        if (!hasId) return 'unknown';
        return 'unknown';
      };

      filteredUnknownOrders = filteredUnknownOrders.filter(o => {
        const serviceKey = resolveServiceKey(o);
        const expectedMain = expectedProviderFromServiceKey(serviceKey);
        const bumps = parseBumps(resolveOrderBumps(o));
        const wantsViewsBump = bumps.views > 0 || !!(o && o.fama24h_views);
        const wantsLikesBump = bumps.likes > 0 || !!(o && o.fama24h_likes);
        const wantsCommentsBump = bumps.comments > 0 || !!(o && o.worldsmm_comments);
        const mainUnknown = expectedMain ? isUnknownForProvider(o, expectedMain) : (isUnknownForProvider(o, 'fama24h') || isUnknownForProvider(o, 'fornecedor_social'));
        const mainError = expectedMain ? isErrorForProvider(o, expectedMain) : (isErrorForProvider(o, 'fama24h') || isErrorForProvider(o, 'fornecedor_social'));

        const bumpViewsIssue = wantsViewsBump ? classifyFamaBumpIssue(o && o.fama24h_views) : 'ok';
        const bumpLikesIssue = wantsLikesBump ? classifyFamaBumpIssue(o && o.fama24h_likes) : 'ok';
        const bumpCommentsIssue = wantsCommentsBump ? classifyWorldsmmCommentsIssue(o) : 'ok';

        const anyUnknown = mainUnknown || bumpViewsIssue === 'unknown' || bumpLikesIssue === 'unknown' || bumpCommentsIssue === 'unknown';
        const anyError = mainError || bumpViewsIssue === 'error' || bumpLikesIssue === 'error' || bumpCommentsIssue === 'error';

        if (issueFilter === 'unknown') {
          if (serviceFilter === 'comentarios') return wantsCommentsBump && bumpCommentsIssue === 'unknown';
          if (serviceFilter && serviceFilter !== 'all') return mainUnknown;
          return anyUnknown;
        }
        if (issueFilter === 'error') {
          if (serviceFilter === 'comentarios') return wantsCommentsBump && bumpCommentsIssue === 'error';
          if (serviceFilter && serviceFilter !== 'all') return mainError;
          return anyError;
        }

        return anyUnknown || anyError;
      });

      const seenPrivates = new Set();
      filteredUnknownOrders = filteredUnknownOrders.filter(o => {
        if (!(o && o.isPrivate)) return true;
        const key = normalizeUsernameKey(resolveUser(o));
        if (!key) return true;
        if (seenPrivates.has(key)) return false;
        seenPrivates.add(key);
        return true;
      });

      const unknownOrders = filteredUnknownOrders.map(o => ({
        orderBumps: resolveOrderBumps(o),
        _id: o._id,
        createdAt: o.createdAt || o.woovi?.paidAt || o.paidAt,
        instaUser: resolveUser(o),
        phone: resolvePhone(o),
        postLinkViews: resolvePostLinkViews(o),
        postLinkLikes: resolvePostLinkLikes(o),
        postLinkComments: resolvePostLinkComments(o),
        type: resolveType(o),
        serviceLabel: resolveServiceLabel(o),
        serviceKey: resolveServiceKey(o),
        expectedMainProvider: expectedProviderFromServiceKey(resolveServiceKey(o)) || '',
        needsWorldsmmComments: (() => { const b = parseBumps(resolveOrderBumps(o)); const wants = b.comments > 0 || !!(o && o.worldsmm_comments); if (!wants) return false; return classifyWorldsmmCommentsIssue(o) !== 'ok'; })(),
        needsFamaViewsBump: (() => { const b = parseBumps(resolveOrderBumps(o)); const wants = b.views > 0 || !!(o && o.fama24h_views); return wants ? classifyFamaBumpIssue(o && o.fama24h_views) !== 'ok' : false; })(),
        needsFamaLikesBump: (() => { const b = parseBumps(resolveOrderBumps(o)); const wants = b.likes > 0 || !!(o && o.fama24h_likes); return wants ? classifyFamaBumpIssue(o && o.fama24h_likes) !== 'ok' : false; })(),
        qty: resolveQtyWithUpgrade(o),
        isPrivate: (() => {
          const byDoc = resolveIsPrivate(o);
          if (byDoc) return true;
          const uKey = normalizeUsernameKey(resolveUser(o));
          if (uKey && privateKeySetUsers.has(uKey)) return true;
          const pKey = normalizePhoneKey(resolvePhone(o));
          if (pKey && privateKeySetPhones.has(pKey)) return true;
          return false;
        })(),
        famaOrderId: (o.fama24h && typeof o.fama24h.orderId !== 'undefined') ? String(o.fama24h.orderId) : '',
        famaStatus: (o.fama24h && o.fama24h.status) ? String(o.fama24h.status) : '',
        fsOrderId: (o.fornecedor_social && typeof o.fornecedor_social.orderId !== 'undefined') ? String(o.fornecedor_social.orderId) : '',
        fsStatus: (o.fornecedor_social && o.fornecedor_social.status) ? String(o.fornecedor_social.status) : '',
        famaViewsOrderId: (o.fama24h_views && typeof o.fama24h_views.orderId !== 'undefined') ? String(o.fama24h_views.orderId) : '',
        famaViewsStatus: (o.fama24h_views && typeof o.fama24h_views.status !== 'undefined') ? String(o.fama24h_views.status) : '',
        famaViewsQty: (o.fama24h_views && o.fama24h_views.requestPayload && typeof o.fama24h_views.requestPayload.quantity !== 'undefined') ? String(o.fama24h_views.requestPayload.quantity) : String(parseBumps(resolveOrderBumps(o)).views || ''),
        famaViewsError: (o.fama24h_views && typeof o.fama24h_views.error !== 'undefined') ? String(o.fama24h_views.error) : '',
        famaLikesOrderId: (o.fama24h_likes && typeof o.fama24h_likes.orderId !== 'undefined') ? String(o.fama24h_likes.orderId) : '',
        famaLikesStatus: (o.fama24h_likes && typeof o.fama24h_likes.status !== 'undefined') ? String(o.fama24h_likes.status) : '',
        famaLikesQty: (o.fama24h_likes && o.fama24h_likes.requestPayload && typeof o.fama24h_likes.requestPayload.quantity !== 'undefined') ? String(o.fama24h_likes.requestPayload.quantity) : String(parseBumps(resolveOrderBumps(o)).likes || ''),
        famaLikesError: (o.fama24h_likes && typeof o.fama24h_likes.error !== 'undefined') ? String(o.fama24h_likes.error) : '',
        worldsmmCommentsOrderId: (o.worldsmm_comments && typeof o.worldsmm_comments.orderId !== 'undefined') ? String(o.worldsmm_comments.orderId) : '',
        worldsmmCommentsStatus: (() => { const b = parseBumps(resolveOrderBumps(o)); const wants = b.comments > 0 || !!(o && o.worldsmm_comments); if (!wants) return ''; const st = (o.worldsmm_comments && typeof o.worldsmm_comments.status !== 'undefined') ? String(o.worldsmm_comments.status) : ''; return st || 'unknown'; })(),
        worldsmmCommentsError: (o.worldsmm_comments && typeof o.worldsmm_comments.error !== 'undefined') ? String(o.worldsmm_comments.error) : '',
        worldsmmCommentsQty: (() => {
          const b = parseBumps(resolveOrderBumps(o));
          const wants = b.comments > 0 || !!(o && o.worldsmm_comments);
          if (!wants) return '';
          if (o.worldsmm_comments && o.worldsmm_comments.requestPayload && typeof o.worldsmm_comments.requestPayload.quantity !== 'undefined') {
            return String(o.worldsmm_comments.requestPayload.quantity);
          }
          return String(b.comments || '');
        })()
      }));

      const openUnknownModal = String(req.query.openUnknownModal || '').trim();
      const openUnknownDefaultProviderRaw = String(req.query.defaultProvider || '').trim();
      const openUnknownLockProvider = String(req.query.lockProvider || '').trim() === '1' ? '1' : '';
      const allowedProviders = new Set(['fama24h', 'fama24h_views', 'fama24h_likes', 'fornecedor_social', 'worldsmm_comments']);
      const openUnknownDefaultProvider = allowedProviders.has(openUnknownDefaultProviderRaw) ? openUnknownDefaultProviderRaw : '';

      return res.render('painel', {
        view,
        period,
        unknownOrders,
        totalUnknown: unknownOrders.length,
        costSettings,
        openUnknownModal,
        openUnknownDefaultProvider,
        openUnknownLockProvider
      });
    }

    const paidOrdersToday = orders.filter(o => {
      const dateStr = o.woovi?.paidAt || o.paidAt || o.createdAt || o.criado;
      if (!dateStr) return false;
      const orderDateUTC = new Date(dateStr);
      const orderDateSP = toSP(orderDateUTC);
      return orderDateSP >= startOfTodaySP && orderDateSP < startOfTomorrowSP;
    }).length;

    const countValidatedUsersInRange = async (vu, start, endExclusive) => {
      const hasStart = !!start;
      const hasEnd = !!endExclusive;
      const startIso = hasStart ? start.toISOString() : null;
      const endIso = hasEnd ? endExclusive.toISOString() : null;
      const stringRange = {};
      if (startIso) stringRange.$gte = startIso;
      if (endIso) stringRange.$lt = endIso;
      const dateRange = {};
      if (start) dateRange.$gte = start;
      if (endExclusive) dateRange.$lt = endExclusive;
      const or = [
        { checkedAt: Object.assign({ $type: 'string' }, stringRange) },
        { checkedAt: Object.assign({ $type: 'date' }, dateRange) },
        { lastTrackAt: Object.assign({ $type: 'string' }, stringRange) },
        { lastTrackAt: Object.assign({ $type: 'date' }, dateRange) }
      ];
      return vu.countDocuments({ $or: or });
    };

    let validatedProfilesToday = 0;
    try {
      const vu = await getCollection('validated_insta_users');
      validatedProfilesToday = await countValidatedUsersInRange(vu, startOfTodayUtc, startOfTomorrowUtc);
    } catch (_) {
      validatedProfilesToday = 0;
    }

    const getPeriodRange = () => {
      if (period === 'today') {
        return { start: startOfTodayUtc, endExclusive: startOfTomorrowUtc };
      }
      if (period === 'last3days') {
        const start = new Date(startOfTodayUtc);
        start.setUTCDate(start.getUTCDate() - 2);
        return { start, endExclusive: startOfTomorrowUtc };
      }
      if (period === 'last7days') {
        const start = new Date(startOfTodayUtc);
        start.setUTCDate(start.getUTCDate() - 6);
        return { start, endExclusive: startOfTomorrowUtc };
      }
      if (period === 'thismonth') {
        const start = new Date(Date.UTC(nowSP.getUTCFullYear(), nowSP.getUTCMonth(), 1, 3, 0, 0, 0));
        return { start, endExclusive: startOfTomorrowUtc };
      }
      if (period === 'lastmonth') {
        const endExclusive = new Date(Date.UTC(nowSP.getUTCFullYear(), nowSP.getUTCMonth(), 1, 3, 0, 0, 0));
        const start = new Date(endExclusive);
        start.setUTCMonth(start.getUTCMonth() - 1);
        return { start, endExclusive };
      }
      if (period === 'custom') {
        const startStr = req.query.startDate;
        const endStr = req.query.endDate;
        if (startStr && endStr) {
          const [sY, sM, sD] = startStr.split('-').map(Number);
          const start = new Date(Date.UTC(sY, sM - 1, sD, 3, 0, 0, 0));
          const [eY, eM, eD] = endStr.split('-').map(Number);
          const endExclusive = new Date(Date.UTC(eY, eM - 1, eD + 1, 3, 0, 0, 0));
          return { start, endExclusive };
        }
      }
      return { start: null, endExclusive: null };
    };

    let validatedProfilesPeriod = 0;
    try {
      const { start, endExclusive } = getPeriodRange();
      const vu = await getCollection('validated_insta_users');
      if (!start && !endExclusive) {
        validatedProfilesPeriod = await vu.countDocuments({
          $or: [
            { checkedAt: { $exists: true, $ne: null } },
            { lastTrackAt: { $exists: true, $ne: null } }
          ]
        });
      } else {
        validatedProfilesPeriod = await countValidatedUsersInRange(vu, start, endExclusive);
      }
    } catch (_) {
      validatedProfilesPeriod = 0;
    }

    const paidOverValidatedTodayPct = validatedProfilesToday > 0 ? (paidOrdersToday / validatedProfilesToday) * 100 : 0;
    const paidOrdersPeriod = filteredOrders.length;
    const paidOverValidatedPeriodPct = validatedProfilesPeriod > 0 ? (paidOrdersPeriod / validatedProfilesPeriod) * 100 : 0;

    // Cost calculation
    let totalCost = 0;
    let totalRevenue = 0;
    let totalBumpRevenue = 0;
    const report = filteredOrders.map(o => {
      const extractInfo = (key) => {
        try {
          if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
          if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return o.additionalInfoMap[key];
          const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const itemPaid = arrPaid.find(i => i && i.key === key);
          if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
          const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
          const item = arr.find(i => i && i.key === key);
          if (item && typeof item.value !== 'undefined') return item.value;
        } catch (_) {}
        return '';
      };

      const toNumber = (v) => {
        const n = Number(String(v || '').replace(/[^\d.]/g, ''));
        return Number.isFinite(n) ? n : 0;
      };

      const toMoney = (v) => {
        const raw = String(v == null ? '' : v).trim();
        if (!raw) return 0;
        let s = raw.replace(/[^\d,.\-]/g, '');
        const hasComma = s.includes(',');
        const hasDot = s.includes('.');
        if (hasComma && hasDot) {
          if (s.lastIndexOf(',') > s.lastIndexOf('.')) {
            s = s.replace(/\./g, '').replace(/,/g, '.');
          } else {
            s = s.replace(/,/g, '');
          }
        } else if (hasComma && !hasDot) {
          s = s.replace(/,/g, '.');
        }
        const n = Number(s);
        return Number.isFinite(n) ? n : 0;
      };

      // Determine quantity
      let qty = 0;
      if (o.quantidade) qty = Number(o.quantidade);
      else if (o.qtd) qty = Number(o.qtd);
      else if (o.additionalInfoPaid) {
         const qItem = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid.find(i => i.key === 'quantidade') : null;
         if (qItem) qty = Number(qItem.value);
      } else if (o.additionalInfoMap && o.additionalInfoMap.quantidade) {
          qty = Number(o.additionalInfoMap.quantidade);
      }

      // Determine type
      let type = o.tipo || o.tipoServico;
      if (!type && o.additionalInfoPaid) {
         const tItem = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid.find(i => i && i.key === 'tipo_servico') : null;
         if (tItem) type = tItem.value;
      } else if (!type && o.additionalInfoMap && o.additionalInfoMap.tipo_servico) {
          type = o.additionalInfoMap.tipo_servico;
      }
      type = String(type || '').toLowerCase();

      // Determine category (seguidores/curtidas/visualizacoes/etc)
      let category = '';
      if (o.additionalInfoPaid) {
        const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const cItem = arr.find(i => i && i.key === 'categoria_servico');
        if (cItem && typeof cItem.value === 'string') category = cItem.value;
      }
      if (!category && o.additionalInfoMap && typeof o.additionalInfoMap.categoria_servico === 'string') {
        category = o.additionalInfoMap.categoria_servico;
      }
      category = String(category || '').toLowerCase();

      // Normalize type for report when using new checkout (categoria_servico + tipo_servico)
      let typeForCost = type;
      if (category === 'curtidas' && type === 'mistos') typeForCost = 'curtidas_mistos';
      else if (category === 'curtidas' && type === 'organicos') typeForCost = 'curtidas_organicas';
      else if (category === 'seguidores' && type === 'mistos') typeForCost = 'seguidores_mistos';
      else if (category === 'seguidores' && type === 'brasileiros') typeForCost = 'seguidores_brasileiros';

      // Determine cost per 1000
      let costPer1000 = 0;
      if (typeForCost.includes('curtidas') && typeForCost.includes('mistos')) costPer1000 = costSettings.curtidas_mistos;
      else if (category === 'curtidas' && typeForCost.includes('organicos')) costPer1000 = costSettings.curtidas;
      else if (typeForCost.includes('mistos')) costPer1000 = costSettings.seguidores_mistos;
      else if (typeForCost.includes('brasileiros') && !typeForCost.includes('curtidas') && !typeForCost.includes('comentarios') && !typeForCost.includes('visualiza')) costPer1000 = costSettings.seguidores_brasileiros;
      else if (typeForCost.includes('organicos')) costPer1000 = costSettings.seguidores_organicos;
      else if (typeForCost.includes('curtidas')) costPer1000 = costSettings.curtidas;
      else if (typeForCost.includes('comentarios')) costPer1000 = costSettings.comentarios;
      else if (typeForCost.includes('visualiza') || typeForCost.includes('views')) costPer1000 = costSettings.visualizacoes;

      const serviceCost = (qty / 1000) * costPer1000;
      const bumpStrRaw = extractInfo('order_bumps');
      const bumpTotalRaw = extractInfo('order_bumps_total');
      const bumpRevenue = toMoney(bumpTotalRaw);
      totalBumpRevenue += bumpRevenue;

      const parseBumps = (raw) => {
        const text = String(raw || '').toLowerCase();
        const get = (name) => {
          const m = text.match(new RegExp(`${name}\\s*:\\s*(\\d+)`, 'i'));
          return m ? Number(m[1]) : 0;
        };
        return {
          views: get('views'),
          likes: get('likes'),
          comments: get('comments'),
          upgrade: get('upgrade')
        };
      };

      const bumpsFromStr = parseBumps(bumpStrRaw);
      const bumpViewsQty = toNumber((o && o.fama24h_views && o.fama24h_views.requestPayload && o.fama24h_views.requestPayload.quantity) || bumpsFromStr.views);
      const bumpLikesQty = toNumber((o && o.fama24h_likes && o.fama24h_likes.requestPayload && o.fama24h_likes.requestPayload.quantity) || bumpsFromStr.likes);
      const bumpCommentsQty = toNumber((o && o.worldsmm_comments && o.worldsmm_comments.requestPayload && o.worldsmm_comments.requestPayload.quantity) || bumpsFromStr.comments);

      const getUpgradeExtraQty = (tipo, base) => {
        try {
          const t0 = String(tipo || '').toLowerCase().trim();
          const b = Number(base) || 0;
          if (!b) return 0;

          const isViewsType =
            t0 === 'visualizacoes_reels' ||
            t0 === 'views' ||
            t0 === 'reels' ||
            t0.includes('visualizacoes') ||
            t0.includes('visualiza');

          if (isViewsType) {
            const viewsMap = {
              1000: 2500,
              5000: 10000,
              25000: 50000,
              100000: 150000,
              200000: 250000,
              500000: 1000000
            };
            const target = viewsMap[b];
            return target ? (Number(target) - b) : 0;
          }

          const isFollowersType = t0.startsWith('seguidores') || t0 === 'mistos' || t0 === 'brasileiros' || t0 === 'organicos';
          const isLikesType = t0.startsWith('curtidas') || t0.includes('curtidas');
          if (!isFollowersType && !isLikesType) return 0;

          const t = t0.startsWith('seguidores_')
            ? t0.replace(/^seguidores_/, '')
            : (t0.startsWith('seguidores') ? t0.replace(/^seguidores/, '').replace(/^_/, '') : t0);

          if (isFollowersType) {
            if (t === 'organicos' && (b === 50 || b === 100)) return 50;
            if ((t === 'brasileiros' || t === 'organicos') && (b === 1000 || b === 2000)) return 1000;
          }

          const map = {
            50: 150,
            150: 300,
            300: 500,
            500: 700,
            700: 1000,
            1000: 2000,
            1200: 2000,
            2000: 3000,
            3000: 4000,
            4000: 5000,
            5000: 7500,
            7500: 10000,
            10000: 15000
          };
          const target = map[b];
          return target ? (Number(target) - b) : 0;
        } catch (_) {
          return 0;
        }
      };

      const upgradeExtraQty = bumpsFromStr.upgrade ? getUpgradeExtraQty(typeForCost, qty) : 0;
      const upgradeCost = (upgradeExtraQty / 1000) * Number(costPer1000 || 0);
      const bumpCommentsCost = bumpCommentsQty * Number(costSettings.comentarios || 0);

      const bumpCostRaw =
        ((bumpViewsQty / 1000) * Number(costSettings.visualizacoes || 0)) +
        ((bumpLikesQty / 1000) * Number(costSettings.curtidas || 0)) +
        bumpCommentsCost +
        upgradeCost;
      const bumpCost = ignoreBumps ? 0 : bumpCostRaw;

      const totalItemCost = 0.85 + serviceCost + bumpCost;
      totalCost += totalItemCost;

      // Revenue calculation
      let revenue = 0;
      if (o.valueCents) {
          revenue = Number(o.valueCents) / 100;
      } else if (o.woovi && o.woovi.paymentMethods && o.woovi.paymentMethods.pix && o.woovi.paymentMethods.pix.value) {
          revenue = Number(o.woovi.paymentMethods.pix.value) / 100;
      }
      totalRevenue += revenue;

      let igUser = '';
      try {
        igUser = String(extractInfo('instagram_username') || o.instagramUsername || o.instauser || '').trim();
      } catch (_) {
        igUser = '';
      }
      igUser = igUser.replace(/^@+/, '').trim();

      let phone = '';
      try {
        phone = String((o && o.customer && o.customer.phone) ? o.customer.phone : '').trim();
      } catch (_) {
        phone = '';
      }
      if (!phone) {
        try {
          phone = String(extractInfo('phone') || '').trim();
        } catch (_) {
          phone = '';
        }
      }

      const customerKey = igUser ? (`ig:${igUser.toLowerCase()}`) : (phone ? (`ph:${String(phone).replace(/\D/g, '')}`) : '');
      const customerLabel = igUser ? (`@${igUser}`) : (phone || '-');
      const totalPaid = (!o.valueCents && bumpRevenue) ? (Number(revenue || 0) + Number(bumpRevenue || 0)) : Number(revenue || 0);

      return {
        _id: o._id,
        createdAt: o.createdAt,
        type: typeForCost,
        qty,
        costPer1000,
        cost: totalItemCost,
        bumpCost,
        orderBumps: String(bumpStrRaw || ''),
        orderBumpsTotal: String(bumpTotalRaw || ''),
        bumpRevenue,
        revenue,
        totalPaid,
        customerKey,
        customerLabel,
        instagramUsername: igUser,
        phone
      };
    });

    const customerAgg = new Map();
    const serviceAgg = new Map();
    for (const r of report) {
      const k = String(r && r.customerKey ? r.customerKey : '').trim();
      if (k) {
        const cur = customerAgg.get(k) || { orders: 0, spend: 0, label: String(r.customerLabel || k) };
        cur.orders += 1;
        cur.spend += Number(r.totalPaid || 0);
        customerAgg.set(k, cur);
      }
      const t = String(r && r.type ? r.type : '').trim();
      if (t) serviceAgg.set(t, (serviceAgg.get(t) || 0) + 1);
    }

    const totalCustomers = customerAgg.size;
    let repeatCustomers = 0;
    for (const v of customerAgg.values()) {
      if (v && v.orders > 1) repeatCustomers += 1;
    }
    const repeatCustomerPct = totalCustomers > 0 ? (repeatCustomers / totalCustomers) * 100 : 0;

    const topUsersByOrders = Array.from(customerAgg.values())
      .sort((a, b) => (b.orders - a.orders) || (b.spend - a.spend))
      .slice(0, 5);

    const topUsersBySpend = Array.from(customerAgg.values())
      .sort((a, b) => (b.spend - a.spend) || (b.orders - a.orders))
      .slice(0, 5);

    const serviceEntries = Array.from(serviceAgg.entries()).map(([type, count]) => ({ type, count }));
    serviceEntries.sort((a, b) => b.count - a.count);
    const topService = serviceEntries.length ? serviceEntries[0] : null;
    const totalOrdersForPie = serviceEntries.reduce((acc, it) => acc + (it.count || 0), 0);
    const colors = ['#2563eb', '#7c3aed', '#16a34a', '#f59e0b', '#dc2626', '#64748b', '#06b6d4', '#a855f7'];
    const pieBase = serviceEntries.slice(0, 5).map((it, idx) => ({
      label: it.type,
      count: it.count,
      color: colors[idx % colors.length],
      pct: totalOrdersForPie > 0 ? (it.count / totalOrdersForPie) * 100 : 0
    }));
    const otherCount = serviceEntries.slice(5).reduce((acc, it) => acc + (it.count || 0), 0);
    const servicePie = otherCount > 0 ? pieBase.concat([{
      label: 'outros',
      count: otherCount,
      color: colors[5 % colors.length],
      pct: totalOrdersForPie > 0 ? (otherCount / totalOrdersForPie) * 100 : 0
    }]) : pieBase;

    const ignoreBumpRevenue = String(req.query.ignoreBumpRevenue || '') === '1';
    const revenueWithoutBumps = Math.max(0, Number(totalRevenue || 0) - Number(totalBumpRevenue || 0));
    const revenueShown = ignoreBumpRevenue ? revenueWithoutBumps : totalRevenue;
    const costOverRevenuePct = revenueShown > 0 ? (Number(totalCost || 0) / Number(revenueShown || 0)) * 100 : 0;
    const bumpRevenuePctOfTotal = totalRevenue > 0 ? (Number(totalBumpRevenue || 0) / Number(totalRevenue || 0)) * 100 : 0;
    const toggleIgnoreBumpRevenueUrl = (() => {
      try {
        const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'http');
        const host = String(req.get('host') || 'localhost');
        const u = new URL(String(req.originalUrl || '/painel'), `${proto}://${host}`);
        if (ignoreBumpRevenue) u.searchParams.delete('ignoreBumpRevenue');
        else u.searchParams.set('ignoreBumpRevenue', '1');
        return u.pathname + (u.search || '');
      } catch (_) {
        return ignoreBumpRevenue ? '/painel' : '/painel?ignoreBumpRevenue=1';
      }
    })();

    res.render('painel', { view, orders: report, totalCost, totalRevenue, revenueShown, totalBumpRevenue, revenueWithoutBumps, ignoreBumpRevenue, bumpRevenuePctOfTotal, costOverRevenuePct, toggleIgnoreBumpRevenueUrl, period, totalTransactions: filteredOrders.length, costSettings, validatedProfilesToday, validatedProfilesPeriod, paidOrdersToday, paidOverValidatedTodayPct, paidOverValidatedPeriodPct, ignoreBumps, toggleIgnoreBumpsUrl, repeatCustomerPct, repeatCustomers, totalCustomers, topUsersByOrders, topUsersBySpend, topService, servicePie });
  } catch (e) {
    res.status(500).send(e.toString());
  }
});

app.get('/painel/unknown_orderid/export', requireAdmin, async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const sessionSelectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};

    const paidQuery = {
      $or: [
        { status: 'pago' },
        { 'woovi.status': 'pago' }
      ]
    };

    const unknownProviderQuery = {
      $or: [
        { 'fama24h.status': 'unknown' },
        { 'fama24h.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fama24h.status': 'error' },
        { 'fama24h.error': { $exists: true, $ne: '' } },
        { 'fornecedor_social.status': 'unknown' },
        { 'fornecedor_social.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fornecedor_social.status': 'error' },
        { 'fornecedor_social.error': { $exists: true, $ne: '' } },
        { 'worldsmm_comments.status': 'unknown' },
        { 'worldsmm_comments.orderId': { $in: ['unknown', 'unknow'] } },
        { 'worldsmm_comments.status': 'error' },
        { 'worldsmm_comments.error': { $regex: 'timeout', $options: 'i' } },
        {
          $and: [
            { worldsmm_comments: { $exists: true } },
            {
              $or: [
                { 'worldsmm_comments.orderId': { $exists: false } },
                { 'worldsmm_comments.orderId': '' },
                { 'worldsmm_comments.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } } } },
                { 'additionalInfoMap.order_bumps': { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } },
                { 'additionalInfoMapPaid.order_bumps': { $regex: 'comments\\s*:\\s*[1-9]', $options: 'i' } }
              ]
            },
            { $or: [{ worldsmm_comments: { $exists: false } }, { 'worldsmm_comments.orderId': { $exists: false } }] }
          ]
        },
        { 'fama24h_views.status': 'unknown' },
        { 'fama24h_views.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fama24h_views.status': 'error' },
        { 'fama24h_views.error': { $exists: true } },
        {
          $and: [
            { fama24h_views: { $exists: true } },
            {
              $or: [
                { 'fama24h_views.orderId': { $exists: false } },
                { 'fama24h_views.orderId': '' },
                { 'fama24h_views.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } } } },
                { 'additionalInfoMap.order_bumps': { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } },
                { 'additionalInfoMapPaid.order_bumps': { $regex: 'views\\s*:\\s*[1-9]', $options: 'i' } }
              ]
            },
            {
              $or: [
                { fama24h_views: { $exists: false } },
                { 'fama24h_views.orderId': { $exists: false } },
                { 'fama24h_views.orderId': '' },
                { 'fama24h_views.orderId': null }
              ]
            }
          ]
        },
        { 'fama24h_likes.status': 'unknown' },
        { 'fama24h_likes.orderId': { $in: ['unknown', 'unknow'] } },
        { 'fama24h_likes.status': 'error' },
        { 'fama24h_likes.error': { $exists: true } },
        {
          $and: [
            { fama24h_likes: { $exists: true } },
            {
              $or: [
                { 'fama24h_likes.orderId': { $exists: false } },
                { 'fama24h_likes.orderId': '' },
                { 'fama24h_likes.orderId': null }
              ]
            }
          ]
        },
        {
          $and: [
            {
              $or: [
                { additionalInfo: { $elemMatch: { key: 'order_bumps', value: { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } } },
                { additionalInfoPaid: { $elemMatch: { key: 'order_bumps', value: { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } } } },
                { 'additionalInfoMap.order_bumps': { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } },
                { 'additionalInfoMapPaid.order_bumps': { $regex: 'likes\\s*:\\s*[1-9]', $options: 'i' } }
              ]
            },
            {
              $or: [
                { fama24h_likes: { $exists: false } },
                { 'fama24h_likes.orderId': { $exists: false } },
                { 'fama24h_likes.orderId': '' },
                { 'fama24h_likes.orderId': null }
              ]
            }
          ]
        }
      ]
    };

    const query = { $and: [paidQuery, unknownProviderQuery] };
    const orders = await col.find(query).sort({ createdAt: -1 }).limit(5000).toArray();

    const toSP = (d) => new Date(d.getTime() - 3 * 60 * 60 * 1000);
    const nowUTC = new Date();
    const nowSP = toSP(nowUTC);
    const startOfTodaySP = new Date(Date.UTC(nowSP.getUTCFullYear(), nowSP.getUTCMonth(), nowSP.getUTCDate(), 0, 0, 0, 0));

    const periodDefault = 'all';
    const period = String(req.query.period || periodDefault);
    const startOfTomorrowSP = (() => { const d = new Date(startOfTodaySP); d.setUTCDate(d.getUTCDate() + 1); return d; })();

    const inPeriod = (o) => {
      const dateStr = o.createdAt || o.woovi?.paidAt || o.paidAt;
      if (!dateStr) return false;
      const orderDateUTC = new Date(dateStr);
      const orderDateSP = toSP(orderDateUTC);
      if (period === 'all') return true;
      if (period === 'today') return orderDateSP >= startOfTodaySP;
      if (period === 'last3days') { const s = new Date(startOfTodaySP); s.setUTCDate(s.getUTCDate() - 2); return orderDateSP >= s; }
      if (period === 'last7days') { const s = new Date(startOfTodaySP); s.setUTCDate(s.getUTCDate() - 6); return orderDateSP >= s; }
      if (period === 'thismonth') { const s = new Date(startOfTodaySP); s.setUTCDate(1); return orderDateSP >= s; }
      if (period === 'lastmonth') {
        const s = new Date(startOfTodaySP); s.setUTCMonth(s.getUTCMonth() - 1); s.setUTCDate(1);
        const e = new Date(startOfTodaySP); e.setUTCDate(1);
        return orderDateSP >= s && orderDateSP < e;
      }
      if (period === 'custom') {
        const startStr = req.query.startDate;
        const endStr = req.query.endDate;
        if (startStr && endStr) {
          const [sY, sM, sD] = String(startStr).split('-').map(Number);
          const s = new Date(Date.UTC(sY, sM - 1, sD, 0, 0, 0, 0));
          const [eY, eM, eD] = String(endStr).split('-').map(Number);
          const e = new Date(Date.UTC(eY, eM - 1, eD, 23, 59, 59, 999));
          return orderDateSP >= s && orderDateSP <= e;
        }
        return true;
      }
      return true;
    };

    const extractInfoAny = (o, key) => {
      try {
        if (o.additionalInfoMapPaid && typeof o.additionalInfoMapPaid[key] !== 'undefined') return o.additionalInfoMapPaid[key];
        if (o.additionalInfoMap && typeof o.additionalInfoMap[key] !== 'undefined') return o.additionalInfoMap[key];
        const arrPaid = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
        const itemPaid = arrPaid.find(i => i && i.key === key);
        if (itemPaid && typeof itemPaid.value !== 'undefined') return itemPaid.value;
        const arr = Array.isArray(o.additionalInfo) ? o.additionalInfo : [];
        const item = arr.find(i => i && i.key === key);
        if (item && typeof item.value !== 'undefined') return item.value;
      } catch (_) {}
      return '';
    };

    const normalizeUrl = (u) => {
      const s0 = String(u || '').trim();
      if (!s0) return '';
      if (/^https?:\/\//i.test(s0)) return s0;
      if (/^www\./i.test(s0)) return `https://${s0}`;
      if (/instagram\.com\//i.test(s0)) return `https://${s0.replace(/^\/+/, '')}`;
      return s0;
    };

    const sanitizeInstagramPostLink = (u) => {
      const s0 = normalizeUrl(u);
      let v = String(s0 || '').trim();
      if (!v) return '';
      v = v.split('#')[0].split('?')[0];
      const m = v.match(/^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/([A-Za-z0-9_-]+)\/?$/i);
      if (!m) return '';
      const kind = String(m[2] || '').toLowerCase();
      const code = String(m[3] || '');
      if (!kind || !code) return '';
      return `https://www.instagram.com/${kind}/${code}/`;
    };

    const resolvePhone = (o) => {
      try {
        const phoneFromCustomer = o && o.customer && (o.customer.phone || o.customer.telefone || o.customer.whatsapp);
        if (phoneFromCustomer) return String(phoneFromCustomer);
        const phoneFromMap = o && o.additionalInfoMap && (o.additionalInfoMap.phone || o.additionalInfoMap.telefone || o.additionalInfoMap.whatsapp);
        if (phoneFromMap) return String(phoneFromMap);
        const direct = o && (o.phone || o.telefone || o.whatsapp || o.customerPhone);
        if (direct) return String(direct);
        if (o && o.additionalInfoPaid) {
          const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const pItem = arr.find(i => i && (i.key === 'phone' || i.key === 'telefone' || i.key === 'whatsapp' || i.key === 'celular'));
          if (pItem && typeof pItem.value !== 'undefined') return String(pItem.value);
        }
      } catch (_) {}
      return '';
    };

    const resolveUser = (o) => {
      try {
        if (o.instaUser) return String(o.instaUser);
        if (o.instauser) return String(o.instauser);
        if (o.instagramUser) return String(o.instagramUser);
        if (o.additionalInfoMap && (o.additionalInfoMap.instauser || o.additionalInfoMap.instaUser)) return String(o.additionalInfoMap.instauser || o.additionalInfoMap.instaUser);
        if (o.additionalInfoPaid) {
          const arr = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid : [];
          const uItem = arr.find(i => i && (i.key === 'instauser' || i.key === 'instaUser' || i.key === 'instagram_username' || i.key === 'user'));
          if (uItem && typeof uItem.value !== 'undefined') return String(uItem.value);
        }
      } catch (_) {}
      return '';
    };

    const resolveQtyWithUpgrade = (o) => {
      try {
        const baseQty = Number(o.quantidade || o.qtd || extractInfoAny(o, 'quantidade') || 0) || 0;
        if (!baseQty) return baseQty;
        const bumpsStr = String(extractInfoAny(o, 'order_bumps') || '').toLowerCase();
        const m = bumpsStr.match(/(?:^|;)\s*upgrade\s*:\s*(\d+)/i);
        const upgradeQty = m ? Number(m[1]) : 0;
        if (!upgradeQty) return baseQty;
        const category = String(extractInfoAny(o, 'categoria_servico') || (o.additionalInfoMap || {}).categoria_servico || '').toLowerCase().trim();
        let type = String(o.tipo || o.tipoServico || extractInfoAny(o, 'tipo_servico') || '').toLowerCase().trim();
        const t = type.startsWith('seguidores_') ? type.replace(/^seguidores_/, '') : type;
        if (category !== 'seguidores' && !type.startsWith('seguidores')) return baseQty;
        const b = Number(baseQty) || 0;
        if (!b) return baseQty;
        if (t === 'organicos' && b === 50) return 100;
        if ((t === 'brasileiros' || t === 'organicos') && b === 1000) return 2000;
        const upsellTargets = { 50: 150, 150: 300, 500: 700, 1000: 2000, 3000: 4000, 5000: 7500, 10000: 15000 };
        const target = upsellTargets[b];
        if (!target) return baseQty;
        return baseQty + (target - b);
      } catch (_) {
        return Number(o.quantidade || o.qtd || extractInfoAny(o, 'quantidade') || 0) || 0;
      }
    };

    const resolveServiceKey = (o) => {
      let category = String(extractInfoAny(o, 'categoria_servico') || (o.additionalInfoMap || {}).categoria_servico || '').toLowerCase().trim();
      let type = String(o.tipo || o.tipoServico || extractInfoAny(o, 'tipo_servico') || '').toLowerCase().trim();
      if (!category) {
        if (type.startsWith('curtidas_')) { category = 'curtidas'; type = type.replace(/^curtidas_/, ''); }
        else if (type.startsWith('seguidores_')) { category = 'seguidores'; type = type.replace(/^seguidores_/, ''); }
        else if (type.startsWith('visualizacoes_')) { category = 'visualizacoes'; type = type.replace(/^visualizacoes_/, ''); }
      }
      if (type === 'visualizacoes_reels') { category = 'visualizacoes'; type = 'reels'; }
      if (category === 'curtidas' && type === 'mistos') return 'curtidas_mistos';
      if (category === 'seguidores' && type === 'mistos') return 'seguidores_mistos';
      if (category === 'curtidas' && type === 'organicos') return 'curtidas_organicos';
      if (category === 'seguidores' && type === 'organicos') return 'seguidores_organicos';
      if (category && type) return `${category}_${type}`;
      return type || category || '';
    };

    const resolveServiceLabel = (o) => String(resolveServiceKey(o) || '').replace(/_/g, ' ').trim();

    const resolvePostLinkViews = (o) => {
      const sessionLink = sessionSelectedFor && sessionSelectedFor.views && sessionSelectedFor.views.link ? String(sessionSelectedFor.views.link) : '';
      const candidates = [
        sessionLink,
        o?.fama24h_views?.requestPayload?.link,
        extractInfoAny(o, 'orderbump_post_views'),
        extractInfoAny(o, 'post_link')
      ];
      for (const c of candidates) {
        const url = sanitizeInstagramPostLink(c);
        if (url) return url;
      }
      return '';
    };

    const resolvePostLinkLikes = (o) => {
      const sessionLink = sessionSelectedFor && sessionSelectedFor.likes && sessionSelectedFor.likes.link ? String(sessionSelectedFor.likes.link) : '';
      const candidates = [
        sessionLink,
        o?.fama24h_likes?.requestPayload?.link,
        extractInfoAny(o, 'orderbump_post_likes'),
        extractInfoAny(o, 'post_link')
      ];
      for (const c of candidates) {
        const url = sanitizeInstagramPostLink(c);
        if (url) return url;
      }
      return '';
    };

    const resolvePostLinkComments = (o) => {
      const sessionLink = sessionSelectedFor && sessionSelectedFor.comments && sessionSelectedFor.comments.link ? String(sessionSelectedFor.comments.link) : '';
      const candidates = [
        sessionLink,
        o?.worldsmm_comments?.requestPayload?.link,
        extractInfoAny(o, 'orderbump_post_comments'),
        extractInfoAny(o, 'post_link')
      ];
      for (const c of candidates) {
        const url = sanitizeInstagramPostLink(c);
        if (url) return url;
      }
      return '';
    };

    let filtered = orders.filter(inPeriod);
    const issueFilter = String(req.query.issue || '').toLowerCase().trim();

    const searchPhone = String(req.query.searchPhone || '').trim();
    if (searchPhone) {
      const needleDigits = searchPhone.replace(/\D/g, '');
      filtered = filtered.filter(o => {
        const phone = resolvePhone(o);
        if (!phone) return false;
        if (needleDigits) return String(phone).replace(/\D/g, '').includes(needleDigits);
        return String(phone).toLowerCase().includes(searchPhone.toLowerCase());
      });
    }

    const serviceFilter = String(req.query.service || '').toLowerCase().trim();
    if (serviceFilter && serviceFilter !== 'all') {
      if (serviceFilter === 'comentarios') {
        filtered = filtered.filter(o => !!(o && o.worldsmm_comments && (typeof o.worldsmm_comments.status !== 'undefined' || typeof o.worldsmm_comments.orderId !== 'undefined')));
      } else {
        filtered = filtered.filter(o => resolveServiceKey(o) === serviceFilter);
      }
    }

    const isUnknownToken = (v) => {
      const s = String(v || '').toLowerCase().trim();
      return s === 'unknown' || s === 'unknow' || s === 'null';
    };

    const hasValidOrderId = (v) => {
      const s = String(v || '').trim();
      if (!s) return false;
      return !isUnknownToken(s);
    };

    const isUnknownForProvider = (o, provider) => {
      const doc = (o && o[provider]) ? o[provider] : null;
      if (!doc) return true;
      const st = typeof doc.status !== 'undefined' ? doc.status : '';
      const oid = typeof doc.orderId !== 'undefined' ? doc.orderId : '';
      const oidStr = String(oid || '').trim().toLowerCase();
      if (!oidStr) return true;
      if (oidStr === 'null' || oidStr === 'undefined') return true;
      return isUnknownToken(st) || isUnknownToken(oid);
    };

    const isErrorForProvider = (o, provider) => {
      const doc = (o && o[provider]) ? o[provider] : null;
      if (!doc) return false;
      const stLower = String(doc.status || '').toLowerCase().trim();
      const err = (typeof doc.error !== 'undefined') ? String(doc.error || '') : '';
      if (stLower === 'error') return true;
      if (err && err.trim() !== '') return true;
      return false;
    };

    const classifyFamaBumpIssue = (subdoc) => {
      const stLower = subdoc && typeof subdoc.status !== 'undefined' ? String(subdoc.status || '').toLowerCase().trim() : '';
      const oid = subdoc && typeof subdoc.orderId !== 'undefined' ? String(subdoc.orderId || '') : '';
      const err = subdoc && typeof subdoc.error !== 'undefined' ? String(subdoc.error || '') : '';
      const errLower = err.toLowerCase();
      const hasId = hasValidOrderId(oid);
      if (hasId && (stLower === 'created' || stLower === 'processing' || stLower === 'pending' || stLower === 'completed')) return 'ok';
      if (isUnknownToken(stLower) || isUnknownToken(oid)) return 'unknown';
      if (stLower === 'error') return 'error';
      if (err && err.trim() !== '') return 'error';
      if (!hasId) return 'unknown';
      return 'unknown';
    };

    const classifyWorldsmmCommentsIssue = (o) => {
      const doc = o && o.worldsmm_comments ? o.worldsmm_comments : null;
      if (!doc) return 'unknown';
      const stLower = typeof doc.status !== 'undefined' ? String(doc.status || '').toLowerCase().trim() : '';
      const oid = typeof doc.orderId !== 'undefined' ? String(doc.orderId || '') : '';
      const err = typeof doc.error !== 'undefined' ? String(doc.error || '') : '';
      const errLower = err.toLowerCase();
      const hasId = hasValidOrderId(oid);
      if (hasId && (stLower === 'created' || stLower === 'processing' || stLower === 'pending' || stLower === 'completed')) return 'ok';
      if (isUnknownToken(stLower) || isUnknownToken(oid)) return 'unknown';
      if (stLower === 'error') return 'error';
      if (!hasId && errLower.includes('timeout')) return 'error';
      if (!hasId && err && err.trim() !== '') return 'error';
      if (!hasId) return 'unknown';
      return 'unknown';
    };

    const expectedProviderFromServiceKey = (serviceKey) => {
      const k = String(serviceKey || '').toLowerCase().trim();
      if (!k) return '';
      if (k === 'seguidores_organicos') return 'fornecedor_social';
      if (k.startsWith('seguidores')) return 'fama24h';
      if (k.startsWith('curtidas')) return 'fama24h';
      if (k.startsWith('visualizacoes')) return 'fama24h';
      return '';
    };

    if (issueFilter === 'unknown' || issueFilter === 'error') {
      filtered = filtered.filter(o => {
        if (serviceFilter === 'comentarios') {
          const issue = classifyWorldsmmCommentsIssue(o);
          return issue === issueFilter;
        }
        if (serviceFilter && serviceFilter !== 'all') {
          const expectedMain = expectedProviderFromServiceKey(serviceFilter);
          if (!expectedMain) return false;
          if (issueFilter === 'unknown') return isUnknownForProvider(o, expectedMain);
          return isErrorForProvider(o, expectedMain);
        }

        const anyUnknown =
          isUnknownForProvider(o, 'fama24h') ||
          isUnknownForProvider(o, 'fornecedor_social') ||
          classifyFamaBumpIssue(o && o.fama24h_views) === 'unknown' ||
          classifyFamaBumpIssue(o && o.fama24h_likes) === 'unknown' ||
          classifyWorldsmmCommentsIssue(o) === 'unknown';

        const anyError =
          isErrorForProvider(o, 'fama24h') ||
          isErrorForProvider(o, 'fornecedor_social') ||
          classifyFamaBumpIssue(o && o.fama24h_views) === 'error' ||
          classifyFamaBumpIssue(o && o.fama24h_likes) === 'error' ||
          classifyWorldsmmCommentsIssue(o) === 'error';

        return issueFilter === 'unknown' ? anyUnknown : anyError;
      });
    }

    const normalizeUsernameKey = (u) => {
      const s0 = String(u || '').trim();
      if (!s0) return '';
      const s1 = s0.startsWith('@') ? s0.slice(1) : s0;
      return s1.toLowerCase().trim();
    };

    const seenPrivates = new Set();
    filtered = filtered.filter(o => {
      if (!(o && o.isPrivate)) return true;
      const key = normalizeUsernameKey(resolveUser(o));
      if (!key) return true;
      if (seenPrivates.has(key)) return false;
      seenPrivates.add(key);
      return true;
    });

    const formatDateSP = (dateStr) => {
      const d0 = new Date(dateStr || new Date());
      const d = toSP(d0);
      const dd = String(d.getUTCDate()).padStart(2, '0');
      const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
      const yyyy = String(d.getUTCFullYear());
      const hh = String(d.getUTCHours()).padStart(2, '0');
      const mi = String(d.getUTCMinutes()).padStart(2, '0');
      return `${dd}/${mm}/${yyyy} ${hh}:${mi}`;
    };

    const headers = [
      'Data (SP)',
      'Order _id',
      'Usuário',
      'Privado',
      'Telefone',
      'Serviço',
      'Qtd contratada',
      'OrderBumps',
      'Fama24h OrderId',
      'Fama24h Status',
      'Fornecedor Social OrderId',
      'Fornecedor Social Status',
      'WorldSMM Comentários OrderId',
      'WorldSMM Comentários Status',
      'WorldSMM Comentários Qtd',
      'WorldSMM Comentários Link',
      'WorldSMM Comentários Erro',
      'Fama24h Views OrderId',
      'Fama24h Views Status',
      'Fama24h Views Qtd',
      'Fama24h Views Link',
      'Fama24h Views Erro',
      'Fama24h Curtidas OrderId',
      'Fama24h Curtidas Status',
      'Fama24h Curtidas Qtd',
      'Fama24h Curtidas Link',
      'Fama24h Curtidas Erro'
    ];

    const esc = (v) => {
      const s = String(typeof v === 'undefined' || v === null ? '' : v);
      if (/[\";\r\n]/.test(s)) return `"${s.replace(/\"/g, '""')}"`;
      return s;
    };

    const rows = filtered.map(o => {
      const createdAt = o.createdAt || o.woovi?.paidAt || o.paidAt;
      return [
        formatDateSP(createdAt),
        String(o._id || ''),
        resolveUser(o),
        o && o.isPrivate ? 'SIM' : 'NÃO',
        resolvePhone(o),
        resolveServiceLabel(o) || String(o.tipo || o.tipoServico || ''),
        String(resolveQtyWithUpgrade(o) || ''),
        String(extractInfoAny(o, 'order_bumps') || ''),
        String(o?.fama24h?.orderId || ''),
        String(o?.fama24h?.status || ''),
        String(o?.fornecedor_social?.orderId || ''),
        String(o?.fornecedor_social?.status || ''),
        String(o?.worldsmm_comments?.orderId || ''),
        String(o?.worldsmm_comments?.status || ''),
        String(o?.worldsmm_comments?.requestPayload?.quantity || ''),
        resolvePostLinkComments(o),
        String(o?.worldsmm_comments?.error || ''),
        String(o?.fama24h_views?.orderId || ''),
        String(o?.fama24h_views?.status || ''),
        String(o?.fama24h_views?.requestPayload?.quantity || ''),
        resolvePostLinkViews(o),
        String(o?.fama24h_views?.error || ''),
        String(o?.fama24h_likes?.orderId || ''),
        String(o?.fama24h_likes?.status || ''),
        String(o?.fama24h_likes?.requestPayload?.quantity || ''),
        resolvePostLinkLikes(o),
        String(o?.fama24h_likes?.error || '')
      ];
    });

    const csv = `\uFEFF${headers.map(esc).join(';')}\r\n${rows.map(r => r.map(esc).join(';')).join('\r\n')}\r\n`;
    const todayKey = (() => {
      const d = toSP(new Date());
      const yyyy = String(d.getUTCFullYear());
      const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(d.getUTCDate()).padStart(2, '0');
      return `${yyyy}${mm}${dd}`;
    })();

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="orderid-unknown-${todayKey}.csv"`);
    res.send(csv);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

app.post('/painel/custos', async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const settingsCol = await getCollection('settings');
    const body = req.body || {};
    const parseNumber = (name, fallback) => {
      const raw = body[name];
      if (typeof raw === 'undefined') return fallback;
      const str = String(raw).replace(',', '.');
      const num = parseFloat(str);
      return Number.isFinite(num) ? num : fallback;
    };
    const values = {
      seguidores_mistos: parseNumber('seguidores_mistos', DEFAULT_COST_SETTINGS.seguidores_mistos),
      seguidores_brasileiros: parseNumber('seguidores_brasileiros', DEFAULT_COST_SETTINGS.seguidores_brasileiros),
      seguidores_organicos: parseNumber('seguidores_organicos', DEFAULT_COST_SETTINGS.seguidores_organicos),
      curtidas_mistos: parseNumber('curtidas_mistos', DEFAULT_COST_SETTINGS.curtidas_mistos),
      curtidas: parseNumber('curtidas', DEFAULT_COST_SETTINGS.curtidas),
      comentarios: parseNumber('comentarios', DEFAULT_COST_SETTINGS.comentarios),
      visualizacoes: parseNumber('visualizacoes', DEFAULT_COST_SETTINGS.visualizacoes)
    };
    await settingsCol.updateOne(
      { _id: 'cost_settings' },
      { $set: { values } },
      { upsert: true }
    );
    const accept = String(req.headers['accept'] || '').toLowerCase();
    const isJson = accept.indexOf('application/json') !== -1;
    if (isJson) {
      return res.json({ ok: true, values });
    }
    res.redirect('/painel');
  } catch (e) {
    res.status(500).send(e.toString());
  }
});

// ==================== ROTAS DE GESTÃO DE CUPONS (ADMIN) ====================

function normalizeCouponCode(code) {
  return String(code || '').trim().toUpperCase();
}

function normalizeProfileKey(username) {
  return String(username || '').trim().replace(/^@+/, '').toLowerCase();
}

function parseOptionalDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

async function getCouponEligibility(code, profileKey) {
  const { getCollection } = require('./mongodbClient');
  const couponsCol = await getCollection('coupons');
  const coupon = await couponsCol.findOne({ code: normalizeCouponCode(code) });
  if (!coupon) return { ok: false, error: 'Cupom inválido ou inativo' };
  const inactive = (coupon.isActive === false) || (String(coupon.isActive || '').toLowerCase() === 'false');
  if (inactive) return { ok: false, error: 'Cupom inválido ou inativo' };

  const now = new Date();
  const validFrom = coupon.validFrom ? new Date(coupon.validFrom) : null;
  const validTo = coupon.validTo ? new Date(coupon.validTo) : null;
  if (validFrom && !Number.isNaN(validFrom.getTime()) && now < validFrom) return { ok: false, error: 'Cupom ainda não está válido' };
  if (validTo && !Number.isNaN(validTo.getTime()) && now > validTo) return { ok: false, error: 'Cupom expirado' };

  const usedCount = (typeof coupon.usedCount === 'number') ? coupon.usedCount : 0;
  const maxUsesTotal = (coupon.maxUsesTotal != null && Number(coupon.maxUsesTotal) > 0) ? Number(coupon.maxUsesTotal) : null;
  if (maxUsesTotal && usedCount >= maxUsesTotal) return { ok: false, error: 'Cupom esgotado' };

  const maxUsesPerProfile = (coupon.maxUsesPerProfile != null && Number(coupon.maxUsesPerProfile) > 0) ? Number(coupon.maxUsesPerProfile) : null;
  if (maxUsesPerProfile) {
    if (!profileKey) return { ok: false, error: 'Preencha o usuário do Instagram para usar este cupom' };
    const usesCol = await getCollection('coupon_uses');
    const count = await usesCol.countDocuments({ couponId: coupon._id, profileKey, status: 'consumed' });
    if (count >= maxUsesPerProfile) return { ok: false, error: 'Cupom já usado neste perfil' };
  }

  return { ok: true, coupon };
}

async function consumeCouponUsageFromOrder(order, meta = {}) {
  try {
    if (!order) return;
    const arrPaid = Array.isArray(order.additionalInfoPaid) ? order.additionalInfoPaid : [];
    const arrOrig = Array.isArray(order.additionalInfo) ? order.additionalInfo : [];
    const mapPaid = order.additionalInfoMapPaid || {};
    const addMap = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => {
      const k = String(it?.key || '').trim();
      if (k) acc[k] = String(it?.value || '').trim();
      return acc;
    }, {});
    const merged = Object.assign({}, addMap, mapPaid);
    const rawCode = merged.cupom || merged.coupon || '';
    const couponCode = normalizeCouponCode(rawCode);
    if (!couponCode) return;

    const profileKey = normalizeProfileKey(merged.instagram_username || order.instagramUsername || order.instauser || '');

    const eligibility = await getCouponEligibility(couponCode, profileKey);
    if (!eligibility.ok) return;

    const { getCollection } = require('./mongodbClient');
    const usesCol = await getCollection('coupon_uses');
    const couponsCol = await getCollection('coupons');

    const orderMongoId = order._id || null;
    const orderIdentifier = String(meta.orderIdentifier || order.identifier || order.woovi?.identifier || order.efi?.charge_id || '').trim();
    const orderCorrelationID = String(meta.correlationID || order.correlationID || '').trim();
    const dedupeKey = orderMongoId ? String(orderMongoId) : (orderIdentifier || orderCorrelationID);
    if (!dedupeKey) return;

    const up = await usesCol.updateOne(
      { couponId: eligibility.coupon._id, dedupeKey },
      { $setOnInsert: { couponId: eligibility.coupon._id, couponCode, profileKey, orderMongoId, orderIdentifier, correlationID: orderCorrelationID, status: 'consumed', consumedAt: new Date(), dedupeKey } },
      { upsert: true }
    );
    if (up.upsertedCount > 0) {
      await couponsCol.updateOne({ _id: eligibility.coupon._id }, { $inc: { usedCount: 1 } });
    }
  } catch (_) {}
}

app.get('/api/admin/coupons', requireAdmin, async (req, res) => {
    try {
        const { getCollection } = require('./mongodbClient');
        const col = await getCollection('coupons');
        const coupons = await col.find({}).sort({ createdAt: -1 }).toArray();
        res.json({ success: true, coupons });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/admin/coupons', requireAdmin, async (req, res) => {
    try {
        const code = normalizeCouponCode(req.body?.code);
        const discountPercentage = Number(req.body?.discountPercentage || 0) || 0;
        const maxUsesTotal = (req.body?.maxUsesTotal != null && String(req.body.maxUsesTotal).trim() !== '') ? (Number(req.body.maxUsesTotal) || null) : null;
        const maxUsesPerProfile = (req.body?.maxUsesPerProfile != null && String(req.body.maxUsesPerProfile).trim() !== '') ? (Number(req.body.maxUsesPerProfile) || null) : null;
        const validFrom = parseOptionalDate(req.body?.validFrom);
        const validTo = parseOptionalDate(req.body?.validTo);
        const isActive = (req.body?.isActive === false || req.body?.isActive === 'false') ? false : true;

        if (!code || !discountPercentage) {
            return res.status(400).json({ success: false, error: 'Código e porcentagem são obrigatórios' });
        }
        if (discountPercentage <= 0 || discountPercentage > 100) {
          return res.status(400).json({ success: false, error: 'Porcentagem inválida' });
        }
        if (validFrom && validTo && validFrom.getTime() > validTo.getTime()) {
          return res.status(400).json({ success: false, error: 'Validade inválida' });
        }
        
        const { getCollection } = require('./mongodbClient');
        const col = await getCollection('coupons');
        
        // Verificar duplicidade
        const existing = await col.findOne({ code });
        if (existing) {
            return res.status(400).json({ success: false, error: 'Cupom já existe' });
        }
        
        await col.insertOne({
            code,
            discountPercentage,
            maxUsesTotal: (maxUsesTotal && maxUsesTotal > 0) ? Math.floor(maxUsesTotal) : null,
            maxUsesPerProfile: (maxUsesPerProfile && maxUsesPerProfile > 0) ? Math.floor(maxUsesPerProfile) : null,
            validFrom: validFrom || null,
            validTo: validTo || null,
            usedCount: 0,
            createdAt: new Date(),
            isActive
        });
        
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.put('/api/admin/coupons/:id', requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { ObjectId } = require('mongodb');
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('coupons');

    const update = {};
    if (req.body?.code != null) update.code = normalizeCouponCode(req.body.code);
    if (req.body?.discountPercentage != null) update.discountPercentage = Number(req.body.discountPercentage || 0) || 0;
    if (req.body?.maxUsesTotal !== undefined) update.maxUsesTotal = (req.body.maxUsesTotal == null || String(req.body.maxUsesTotal).trim() === '') ? null : Math.floor(Number(req.body.maxUsesTotal) || 0);
    if (req.body?.maxUsesPerProfile !== undefined) update.maxUsesPerProfile = (req.body.maxUsesPerProfile == null || String(req.body.maxUsesPerProfile).trim() === '') ? null : Math.floor(Number(req.body.maxUsesPerProfile) || 0);
    if (req.body?.validFrom !== undefined) update.validFrom = parseOptionalDate(req.body.validFrom);
    if (req.body?.validTo !== undefined) update.validTo = parseOptionalDate(req.body.validTo);
    if (req.body?.isActive !== undefined) update.isActive = !(req.body.isActive === false || req.body.isActive === 'false');

    if (!Object.keys(update).length) return res.status(400).json({ success: false, error: 'Nenhum campo para atualizar' });
    if (update.code != null && !update.code) return res.status(400).json({ success: false, error: 'Código é obrigatório' });
    if (update.discountPercentage != null && !update.discountPercentage) return res.status(400).json({ success: false, error: 'Porcentagem é obrigatória' });
    if (update.discountPercentage != null && (update.discountPercentage <= 0 || update.discountPercentage > 100)) {
      return res.status(400).json({ success: false, error: 'Porcentagem inválida' });
    }
    if (update.validFrom && update.validTo && update.validFrom.getTime() > update.validTo.getTime()) return res.status(400).json({ success: false, error: 'Validade inválida' });
    if (update.maxUsesTotal != null && update.maxUsesTotal <= 0) update.maxUsesTotal = null;
    if (update.maxUsesPerProfile != null && update.maxUsesPerProfile <= 0) update.maxUsesPerProfile = null;

    if (update.code != null) {
      const existing = await col.findOne({ code: update.code, _id: { $ne: new ObjectId(id) } });
      if (existing) return res.status(400).json({ success: false, error: 'Cupom já existe' });
    }

    await col.updateOne({ _id: new ObjectId(id) }, { $set: update });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.delete('/api/admin/coupons/:id', requireAdmin, async (req, res) => {
    try {
        const { id } = req.params;
        const { getCollection } = require('./mongodbClient');
        const col = await getCollection('coupons');
        const { ObjectId } = require('mongodb');
        
        await col.deleteOne({ _id: new ObjectId(id) });
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Rota pública de validação de cupom
app.post('/api/validate-coupon', async (req, res) => {
    try {
        const code = normalizeCouponCode(req.body?.code);
        const profileKey = normalizeProfileKey(req.body?.instagram_username || '');
        if (!code) return res.status(400).json({ valid: false, error: 'Código não fornecido' });

        const eligibility = await getCouponEligibility(code, profileKey);
        if (!eligibility.ok) {
          return res.json({ valid: false, error: eligibility.error || 'Cupom inválido' });
        }
        const coupon = eligibility.coupon;
        return res.json({
          valid: true,
          discount: (Number(coupon.discountPercentage || 0) / 100),
          code: coupon.code
        });
    } catch (error) {
        console.error('Erro na validação de cupom:', error);
        res.status(500).json({ valid: false, error: 'Erro interno' });
    }
});

app.listen(port, () => {
  console.log("🗄️ Baserow configurado com sucesso");
  console.log(`Servidor rodando na porta ${port}`);
  console.log(`Preview disponível: http://localhost:${port}/checkout`);
});

(async () => {
  async function consolidateByUsername(colName) {
    try {
      const col = await getCollection(colName);
      const all = await col.find({}).toArray();
      const groups = all.reduce((acc, d) => { const u = String(d.username||'').trim().toLowerCase(); if (!u) return acc; (acc[u] = acc[u] || []).push(d); return acc; }, {});
      const keys = Object.keys(groups).filter(k => groups[k].length > 1 || (groups[k][0] && String(groups[k][0].username||'').trim().toLowerCase() !== k));
      for (const k of keys) {
        const arr = groups[k];
        arr.sort((a,b)=>{ const as = String(a.source||''); const bs = String(b.source||''); const aw = /verifyInstagramProfile|api\.checkInstagramProfile|api\.instagram\.posts/.test(as) ? 1 : 0; const bw = /verifyInstagramProfile|api\.checkInstagramProfile|api\.instagram\.posts/.test(bs) ? 1 : 0; if (aw !== bw) return bw - aw; const af = Number(a.followersCount||0); const bf = Number(b.followersCount||0); if (af !== bf) return bf - af; const at = new Date(a.checkedAt||a.createdAt||a.firstSeenAt||0).getTime(); const bt = new Date(b.checkedAt||b.createdAt||b.firstSeenAt||0).getTime(); return bt - at; });
        const base = Object.assign({}, arr[0]);
        base.username = k;
        base.fullName = base.fullName || (arr.find(x=>x.fullName)?.fullName || null);
        base.profilePicUrl = base.profilePicUrl || (arr.find(x=>x.profilePicUrl)?.profilePicUrl || null);
        base.isVerified = !!(base.isVerified || arr.some(x=>x.isVerified));
        base.isPrivate = !!(base.isPrivate || arr.some(x=>x.isPrivate));
        const fcounts = arr.map(x=>Number(x.followersCount||0)).concat([Number(base.followersCount||0)]);
        base.followersCount = Math.max.apply(null, fcounts);
        const dates = arr.map(x=>new Date(x.checkedAt||0).getTime()).concat([new Date(base.checkedAt||0).getTime()]).filter(n=>isFinite(n));
        const maxChecked = dates.length ? Math.max.apply(null, dates) : null;
        if (maxChecked !== null) base.checkedAt = new Date(maxChecked).toISOString();
        const firsts = arr.map(x=>new Date(x.firstSeenAt||0).getTime()).concat([new Date(base.firstSeenAt||0).getTime()]).filter(n=>isFinite(n));
        const minFirst = firsts.length ? Math.min.apply(null, firsts) : null;
        if (minFirst !== null) base.firstSeenAt = new Date(minFirst).toISOString();
        const lasts = arr.map(x=>new Date(x.lastAt||0).getTime()).concat([new Date(base.lastAt||0).getTime()]).filter(n=>isFinite(n));
        const maxLast = lasts.length ? Math.max.apply(null, lasts) : null;
        if (maxLast !== null) base.lastAt = new Date(maxLast).toISOString();
        base.linkId = base.linkId || (arr.find(x=>x.linkId)?.linkId || null);
        await col.updateOne({ username: k }, { $set: base, $setOnInsert: { username: k } }, { upsert: true });
        const idsToRemove = arr.slice(1).map(x=>x._id).filter(Boolean);
        if (idsToRemove.length) await col.deleteMany({ _id: { $in: idsToRemove } });
        const wrongCaseDocs = arr.filter(x=>String(x.username||'').trim().toLowerCase() !== k);
        const wrongIds = wrongCaseDocs.map(x=>x._id).filter(Boolean);
        if (wrongIds.length) await col.deleteMany({ _id: { $in: wrongIds } });
      }
    } catch (_) {}
  }
  try { await consolidateByUsername('validated_insta_users'); } catch(_) {}
  // coleções antigas removidas
})();

app.post('/api/payment/confirm', async (req, res) => {
  try {
    const body = req.body || {};
    const identifier = String(body.identifier || '').trim();
    const correlationID = String(body.correlationID || '').trim();
    const value = Number(body.value || 0) || 0;
    const paidAtRaw = body.paidAt || null;
    const endToEndId = String(body.endToEndId || '').trim() || null;
    const tipo = String(body.tipo_servico || '').trim();
    const qtd = Number(body.quantidade || 0) || 0;
    const instaUser = String(body.instagram_username || '').trim();
    const phoneRaw = String(body.phone || '').trim();

    const col = await getCollection('checkout_orders');
    const conds = [];
    if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
    if (correlationID) conds.push({ correlationID });
    if (phoneRaw) {
      const digits = phoneRaw.replace(/\D/g, '');
      if (digits) {
        conds.push({ 'customer.phone': `+55${digits}` });
        conds.push({ additionalInfo: { $elemMatch: { key: 'phone', value: digits } } });
      }
    }
    const filter = conds.length ? { $or: conds } : {};

    const setFields = {
      status: 'pago',
      'woovi.status': 'pago',
    };
    if (paidAtRaw) setFields['woovi.paidAt'] = paidAtRaw;
    if (endToEndId) setFields['woovi.endToEndId'] = endToEndId;
    if (typeof value === 'number') setFields['woovi.paymentMethods.pix.value'] = value;
    if (tipo) setFields['tipo'] = tipo;
    if (qtd) setFields['qtd'] = qtd;
    if (instaUser) setFields['instagramUsername'] = instaUser;

    const upd = await col.updateOne(filter, { $set: setFields });
    if (!upd.matchedCount) {
      return res.status(404).json({ ok: false, error: 'order_not_found', identifier, correlationID, phone: phoneRaw });
    }

    try {
      const record = await col.findOne(filter);
      const alreadySent = record?.fama24h?.orderId ? true : false;
      const resolvedTipo = tipo || record?.tipo || record?.tipoServico || '';
      const resolvedQtd = qtd || record?.quantidade || record?.qtd || 0;
      const resolvedUser = instaUser || record?.instagramUsername || record?.instauser || '';
      const key = process.env.FAMA24H_API_KEY || '';

      try {
        const uname = String(resolvedUser || '').trim().toLowerCase().replace(/^@/, '');
        const missingInitial = !record || record.initialFollowersCount == null;
        if (uname && missingInitial) {
          const vu = await getCollection('validated_insta_users');
          const vUser = await vu.findOne({ username: uname });
          if (vUser && typeof vUser.followersCount === 'number') {
            await col.updateOne(
              { _id: record._id, $or: [{ initialFollowersCount: { $exists: false } }, { initialFollowersCount: null }] },
              { $set: { initialFollowersCount: Number(vUser.followersCount), initialFollowersCheckedAt: vUser.checkedAt || new Date().toISOString() } }
            );
          }
        }
      } catch (_) {}
      
      const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
      const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
      const infoMap = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => {
          const k = String(it?.key || '').trim();
          if (k) acc[k] = String(it?.value || '').trim();
          return acc;
      }, {});
      const pacoteStr = String(infoMap['pacote'] || '').toLowerCase();
      const categoriaServ = String(infoMap['categoria_servico'] || '').toLowerCase();
      const isViewsBase = categoriaServ === 'visualizacoes' || /^visualizacoes_reels$/i.test(resolvedTipo);
      const isCurtidasBase = pacoteStr.includes('curtida') || categoriaServ === 'curtidas';
      const isOrganicosFollowers = /organicos/i.test(resolvedTipo) && !isCurtidasBase && !isViewsBase;
      
      let serviceId = null;
      if (isCurtidasBase) {
          if (/^mistos$/i.test(resolvedTipo)) {
              serviceId = 671;
          } else if (/^(brasileiros|organicos)$/i.test(resolvedTipo)) {
              serviceId = 679;
          }
      } else {
          if (/^mistos$/i.test(resolvedTipo)) {
              serviceId = 663;
          } else if (/^brasileiros$/i.test(resolvedTipo)) {
              serviceId = 23;
          }
      }
      
      // Ajuste: O provedor (Fama24h - serviço 663) exige mínimo de 100.
      // Se o pedido for de 50 (teste), enviamos 100 para garantir o processamento.
      let finalQtdFama = resolvedQtd;
      if (serviceId === 663 && finalQtdFama > 0 && finalQtdFama < 100) {
        finalQtdFama = 100;
      }

      const canSend = !!key && !!serviceId && !!resolvedUser && finalQtdFama > 0 && !alreadySent;
      if (canSend) {
        const axios = require('axios');
        const payload = new URLSearchParams({ key, action: 'add', service: String(serviceId), link: String(resolvedUser), quantity: String(finalQtdFama) });
        const famaResp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
        const famaData = famaResp.data || {};
        const orderId = famaData.order || famaData.id || null;
      await col.updateOne(filter, { $set: { fama24h: { orderId, status: orderId ? 'created' : 'unknown', requestPayload: { service: serviceId, link: resolvedUser, quantity: resolvedQtd }, response: famaData, requestedAt: new Date().toISOString() } } });
    }
    // Disparo para FornecedorSocial quando for orgânicos
    try {
          const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(resolvedTipo);
      const additionalInfoMap = record?.additionalInfoMapPaid || (Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid.reduce((acc, it) => { acc[it.key] = it.value; return acc; }, {}) : (Array.isArray(record?.additionalInfo) ? record.additionalInfo.reduce((acc, it) => { acc[it.key] = it.value; return acc; }, {}) : {}));
      const pacoteStrFS = String(additionalInfoMap['pacote'] || '').toLowerCase();
      const categoriaServFS = String(additionalInfoMap['categoria_servico'] || '').toLowerCase();
      const isViewsBaseFS = categoriaServFS === 'visualizacoes' || /^visualizacoes_reels$/i.test(resolvedTipo);
      const isCurtidasBaseFS = pacoteStrFS.includes('curtida') || categoriaServFS === 'curtidas';
      const isOrganicosFollowersFS = /organicos/i.test(resolvedTipo) && !isCurtidasBaseFS && !isViewsBaseFS;
      const bumpsStr0 = additionalInfoMap['order_bumps'] || (record?.additionalInfoPaid || []).find(it => it && it.key === 'order_bumps')?.value || (record?.additionalInfo || []).find(it => it && it.key === 'order_bumps')?.value || '';
      let upgradeAdd = 0;
      if (isFollowers && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
        if ((/brasileiros/i.test(resolvedTipo) || /organicos/i.test(resolvedTipo)) && Number(resolvedQtd) === 1000) upgradeAdd = 1000; else {
          const map = { 50: 50, 150: 150, 300: 200, 500: 200, 700: 300, 1000: 1000, 1200: 800, 2000: 1000, 3000: 1000, 4000: 1000, 5000: 2500, 7500: 2500, 10000: 5000 };
          upgradeAdd = map[Number(resolvedQtd)] || 0;
        }
      }
      const finalQtd = Math.max(0, Number(resolvedQtd) + Number(upgradeAdd));
      const alreadySentFS = !!(record && record.fornecedor_social && record.fornecedor_social.orderId);
      if (isOrganicosFollowersFS && !!resolvedUser && finalQtd > 0 && !alreadySentFS) {
        const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
        const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
        if (!!keyFS) {
          const lockUpdate = await col.updateOne(
            { _id: record._id, 'fornecedor_social.orderId': { $exists: false }, 'fornecedor_social.status': { $ne: 'processing' } },
            { $set: { 'fornecedor_social.status': 'processing', 'fornecedor_social.attemptedAt': new Date().toISOString() } }
          );
          if (lockUpdate.modifiedCount > 0) {
            const axios = require('axios');
            const linkFS = (/^https?:\/\//i.test(String(resolvedUser))) ? String(resolvedUser) : `https://instagram.com/${String(resolvedUser)}`;
            const payloadFS = new URLSearchParams({ key: keyFS, action: 'add', service: String(serviceFS), link: linkFS, quantity: String(finalQtd) });
            try {
              const respFS = await axios.post('https://fornecedorsocial.com/api/v2', payloadFS.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
              const dataFS = respFS.data || {};
              const orderIdFS = dataFS.order || dataFS.id || null;
              await col.updateOne(filter, { $set: { fornecedor_social: { orderId: orderIdFS, status: orderIdFS ? 'created' : 'unknown', requestPayload: { service: serviceFS, link: linkFS, quantity: finalQtd }, response: dataFS, requestedAt: new Date().toISOString() } } });
              try { await broadcastPaymentPaid(identifier, correlationID); } catch(_) {}
            } catch(_) {}
          }
        }
      }
    } catch(_) {}
      try {
        const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
        const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
        const bumpsStr = (arrPaid.find(it => it && it.key === 'order_bumps')?.value) || (arrOrig.find(it => it && it.key === 'order_bumps')?.value) || '';
        let viewsQty = 0;
        let likesQty = 0;
        if (typeof bumpsStr === 'string' && bumpsStr) {
          const parts = bumpsStr.split(';');
          const vPart = parts.find(p => /^views:\d+$/i.test(p.trim()));
          const lPart = parts.find(p => /^likes:\d+$/i.test(p.trim()));
          if (vPart) {
            const num = Number(vPart.split(':')[1]);
            if (!Number.isNaN(num) && num > 0) viewsQty = num;
          }
          if (lPart) {
            const numL = Number(lPart.split(':')[1]);
            if (!Number.isNaN(numL) && numL > 0) likesQty = numL;
          }
        }
        const arrPaid2 = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
        const arrOrig2 = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
        const sanitizeLink = (s) => {
          let v = String(s || '').replace(/[`\s]/g, '').trim();
          if (!v) return '';
          if (!/^https?:\/\//i.test(v)) {
            if (/^www\./i.test(v)) v = `https://${v}`;
            else if (/^instagram\.com\//i.test(v)) v = `https://${v}`;
            else if (/^\/\/+/i.test(v)) v = `https:${v}`;
          }
          v = v.split('#')[0].split('?')[0];
          const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?$/i.test(v);
          return ok ? (v.endsWith('/') ? v : (v + '/')) : '';
        };
        const mapPaid3 = record?.additionalInfoMapPaid || {};
        const viewsLink = sanitizeLink(mapPaid3['orderbump_post_views'] || (arrPaid2.find(it => it && it.key === 'orderbump_post_views')?.value) || (arrOrig2.find(it => it && it.key === 'orderbump_post_views')?.value) || '');
        const likesLinkSel = sanitizeLink(mapPaid3['orderbump_post_likes'] || (arrPaid2.find(it => it && it.key === 'orderbump_post_likes')?.value) || (arrOrig2.find(it => it && it.key === 'orderbump_post_likes')?.value) || '');
        const alreadyViews4 = !!(record && record.fama24h_views && (record.fama24h_views.orderId || record.fama24h_views.status === 'processing' || record.fama24h_views.status === 'created' || typeof record.fama24h_views.error !== 'undefined'));
        if (viewsQty > 0 && (process.env.FAMA24H_API_KEY || '') && viewsLink && !alreadyViews4) {
          const lockUpdate = await col.updateOne(
            { ...filter, 'fama24h_views.orderId': { $exists: false }, 'fama24h_views.status': { $nin: ['processing', 'created'] } },
            { $set: { 'fama24h_views.status': 'processing', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_views.error': '' } }
          );
          if (lockUpdate.modifiedCount > 0) {
            const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
            try {
              const respViews = await postFormWithRetry('https://fama24h.net/api/v2', payloadViews.toString(), 60000, 3);
              const dataViews = normalizeProviderResponseData(respViews.data);
              const orderIdViews = extractProviderOrderId(dataViews);
              const setObj = { 'fama24h_views.status': orderIdViews ? 'created' : 'unknown', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.response': dataViews, 'fama24h_views.requestedAt': new Date().toISOString() };
              if (orderIdViews) setObj['fama24h_views.orderId'] = orderIdViews;
              await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_views.error': '' } });
            } catch (e2) {
              const errVal = e2?.response?.data || e2?.message || String(e2);
              const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
              const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
              await col.updateOne(filter, { $set: { 'fama24h_views.status': st, 'fama24h_views.error': errVal, 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
            }
          }
        } else if (viewsQty > 0 && (process.env.FAMA24H_API_KEY || '') && !viewsLink && !alreadyViews4) {
          await col.updateOne(filter, { $set: { 'fama24h_views.status': 'error', 'fama24h_views.error': 'invalid_link', 'fama24h_views.requestPayload': { service: 250, link: viewsLink, quantity: viewsQty }, 'fama24h_views.requestedAt': new Date().toISOString() } });
        }
        const alreadyLikes4 = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
        if (likesQty > 0 && (process.env.FAMA24H_API_KEY || '') && likesLinkSel && !alreadyLikes4) {
          const lockUpdate = await col.updateOne(
            { ...filter, 'fama24h_likes.orderId': { $exists: false }, 'fama24h_likes.status': { $nin: ['processing', 'created'] } },
            { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() }, $unset: { 'fama24h_likes.error': '' } }
          );
          if (lockUpdate.modifiedCount > 0) {
              const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '671', link: String(likesLinkSel), quantity: String(likesQty) });
              try {
                const respLikes = await postFormWithRetry('https://fama24h.net/api/v2', payloadLikes.toString(), 60000, 3);
                const dataLikes = normalizeProviderResponseData(respLikes.data);
                const orderIdLikes = extractProviderOrderId(dataLikes);
                const setObj = { 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.response': dataLikes, 'fama24h_likes.requestedAt': new Date().toISOString() };
                if (orderIdLikes) setObj['fama24h_likes.orderId'] = orderIdLikes;
                await col.updateOne(filter, { $set: setObj, $unset: { 'fama24h_likes.error': '' } });
              } catch (e3) {
                const errVal = e3?.response?.data || e3?.message || String(e3);
                const errStr = (typeof errVal === 'string') ? errVal : JSON.stringify(errVal);
                const st = errStr && errStr.includes('link_duplicate') ? 'duplicate' : 'error';
                await col.updateOne(filter, { $set: { 'fama24h_likes.error': errVal, 'fama24h_likes.status': st, 'fama24h_likes.requestPayload': { service: 671, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.requestedAt': new Date().toISOString() } });
              }
          }
        }
      } catch (_) {}
      broadcastPaymentPaid(identifier, correlationID);
      // try { await trackMetaPurchaseForOrder(identifier, correlationID, req); } catch(_) {}
      try { await ensureRefilLink(identifier, correlationID, req); } catch(_) {}

      
      // Atualizar status do perfil no banco de validação (isPrivate, etc)
      if (instaUser) {
        try {
          const mockReq = { session: {}, query: {}, body: {} };
          // Executar em background para não travar o webhook
          verifyInstagramProfile(instaUser, 'Webhook-Payment', req.ip || '127.0.0.1', mockReq, null)
            .catch(err => console.error('❌ [webhook] Falha ao atualizar perfil Instagram:', err.message));
        } catch (_) {}
      }
    } catch (_) {}

    return res.json({ ok: true, updated: upd.matchedCount });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.post('/webhook/validar-confirmado', async (req, res) => {
  try {
    const body = req.body || {};
    const identifier = String(body.identifier || '').trim();
    const correlationID = String(body.correlationID || '').trim();
    const value = Number(body.value || 0) || 0;
    const paidAtRaw = body.paidAt || null;
    const endToEndId = String(body.endToEndId || '').trim() || null;
    const tipo = String(body.tipo_servico || '').trim();
    const qtd = Number(body.quantidade || 0) || 0;
    const instaUser = String(body.instagram_username || '').trim();
    const phoneRaw = String(body.phone || '').trim();

    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');
    const conds = [];
    if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
    if (correlationID) conds.push({ correlationID });
    if (phoneRaw) {
      const digits = phoneRaw.replace(/\D/g, '');
      if (digits) {
        conds.push({ 'customer.phone': `+55${digits}` });
        conds.push({ additionalInfo: { $elemMatch: { key: 'phone', value: digits } } });
      }
    }
    const filter = conds.length ? { $or: conds } : {};

    // 1. Fetch existing order FIRST to validate value
    const record = await col.findOne(filter);
    if (!record) {
      return res.status(404).json({ ok: false, error: 'order_not_found', identifier, correlationID, phone: phoneRaw });
    }

    try {
      const uname = String(instaUser || record.instagramUsername || record.instauser || '').trim().toLowerCase().replace(/^@/, '');
      const missingInitial = record.initialFollowersCount == null;
      if (uname && missingInitial) {
        const vu = await getCollection('validated_insta_users');
        const vUser = await vu.findOne({ username: uname });
        if (vUser && typeof vUser.followersCount === 'number') {
          await col.updateOne(
            { _id: record._id, $or: [{ initialFollowersCount: { $exists: false } }, { initialFollowersCount: null }] },
            { $set: { initialFollowersCount: Number(vUser.followersCount), initialFollowersCheckedAt: vUser.checkedAt || new Date().toISOString() } }
          );
        }
      }
    } catch (_) {}

    // ---------------------------------------------------------
    // VALIDAÇÃO RIGOROSA DE VALOR
    // ---------------------------------------------------------
    const expectedValue = record.expectedValueCents;
    // Use body value if provided, otherwise fallback to record value (though body value is preferred for 'payment confirmed' hook)
    const paidValue = value || record.valueCents;
    
    let isDivergent = false;
    let mismatchDetails = null;

    if (expectedValue && typeof paidValue === 'number') {
        // Tolerância zero para diferença
        if (expectedValue !== paidValue) {
            console.error(`🚨 PAGAMENTO DIVERGENTE (Webhook Confirmado): ID=${identifier}. Esperado=${expectedValue}, Pago=${paidValue}`);
            isDivergent = true;
            mismatchDetails = { expected: expectedValue, paid: paidValue, detectedAt: new Date().toISOString() };
        }
    } else if (!expectedValue && paidValue <= 10) {
        // Fallback safety for suspiciously low values without expectedValue
         console.warn(`🚨 PAGAMENTO SUSPEITO (Webhook Confirmado): ID=${identifier}. Valor=${paidValue} (sem expectedValueCents)`);
         isDivergent = true;
    }

    const setFields = {
      status: isDivergent ? 'divergent_value' : 'pago',
      'woovi.status': isDivergent ? 'divergent_value' : 'pago',
    };
    if (isDivergent && mismatchDetails) {
        setFields.mismatchDetails = mismatchDetails;
    }

    if (paidAtRaw) setFields['woovi.paidAt'] = paidAtRaw;
    if (endToEndId) setFields['woovi.endToEndId'] = endToEndId;
    if (typeof value === 'number') setFields['woovi.paymentMethods.pix.value'] = value;
    if (tipo) setFields['tipo'] = tipo;
    if (qtd) setFields['qtd'] = qtd;
    if (instaUser) setFields['instagramUsername'] = instaUser;

    const upd = await col.updateOne(filter, { $set: setFields });

    // 2. Fulfill ONLY if not divergent
    if (!isDivergent) {
        try {
          const updatedRecord = await col.findOne(filter);
          await processOrderFulfillment(updatedRecord, col, req);
        } catch (e) {
          console.error('Error processing fulfillment in payment/confirm:', e);
        }
    } else {
        console.error('🚨 Fulfillment BLOCKED due to payment value mismatch (Webhook Confirmado).');
        return res.json({ ok: true, status: 'divergent_value', message: 'Payment value mismatch. Service not dispatched.' });
    }

    return res.json({ ok: true, updated: upd.matchedCount });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Buscar últimos posts com metadados (timestamp, tipo, mídia)
async function fetchInstagramRecentPosts(username) {
  const now = Date.now();
  const USAGE_INTERVAL_MS = 5000;
  const MAX_ERRORS_PER_PROFILE = 5;
  const DISABLE_TIME_MS = 60 * 1000;
  const REQUEST_TIMEOUT = 3000; // REDUZIDO DE 5000 PARA 3000 PARA FALHAR MAIS RÁPIDO

  // Selecionar candidatos
  const available = cookieProfiles.filter(p => p.disabledUntil <= now && !isCookieLocked(p.ds_user_id))
    .sort((a,b) => {
      if (a.errorCount !== b.errorCount) return a.errorCount - b.errorCount;
      return a.lastUsed - b.lastUsed;
    });

  // Tentar até 3 perfis em paralelo para maximizar velocidade
  const candidates = available.slice(0, 3);
  
  const tryProfile = async (profile) => {
    if (!profile) throw new Error('No profile');
    
    // Bloqueio otimista
    if (isCookieLocked(profile.ds_user_id)) throw new Error('Locked');
    lockCookie(profile.ds_user_id);
    
    try {
      console.log(`[IG] Tentando (Paralelo) API autenticada com cookie ${profile.ds_user_id}`);
      const proxyAgent = profile.proxy ? new HttpsProxyAgent(
        `http://${encodeURIComponent(profile.proxy.auth.username)}:${encodeURIComponent(profile.proxy.auth.password)}@${profile.proxy.host}:${profile.proxy.port}`,
        { rejectUnauthorized: false }
      ) : null;
      
      const headers = {
        "User-Agent": profile.userAgent,
        "X-IG-App-ID": "936619743392459",
        "Cookie": `sessionid=${profile.sessionid}; ds_user_id=${profile.ds_user_id}`,
        "Accept": "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "X-Requested-With": "XMLHttpRequest"
      };

      const resp = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`, { 
        headers, 
        httpsAgent: proxyAgent, 
        timeout: REQUEST_TIMEOUT 
      });

      if (resp.status !== 200) throw new Error(`HTTP ${resp.status}`);
      
      const user = resp.data && resp.data.data && resp.data.data.user;
      if (!user || user.is_private) {
        // Sucesso técnico, mas falha de negócio (privado) - não conta como erro de conexão
        profile.lastUsed = Date.now();
        profile.errorCount = 0;
        return { success: false, error: 'Perfil privado ou inexistente' };
      }

      const edges = (user.edge_owner_to_timeline_media && Array.isArray(user.edge_owner_to_timeline_media.edges)) ? user.edge_owner_to_timeline_media.edges : [];
      const posts = edges.map(e => e && e.node ? ({
        shortcode: e.node.shortcode,
        takenAt: e.node.taken_at_timestamp,
        isVideo: !!e.node.is_video,
        displayUrl: e.node.display_url || e.node.thumbnail_src || null,
        videoUrl: e.node.video_url || null,
        typename: e.node.__typename || ''
      }) : null).filter(Boolean).sort((a,b)=> Number(b.takenAt||0) - Number(a.takenAt||0)).slice(0, 8);

      profile.lastUsed = Date.now();
      profile.errorCount = 0;
      return { success: true, username: user.username, posts };

    } catch (err) {
      console.error(`[IG] Falha cookie ${profile.ds_user_id}:`, err?.message || String(err));
      profile.errorCount++;
      if (profile.errorCount >= MAX_ERRORS_PER_PROFILE) profile.disabledUntil = Date.now() + DISABLE_TIME_MS;
      throw err;
    } finally {
      unlockCookie(profile.ds_user_id);
    }
  };

  if (candidates.length > 0) {
    try {
      // Promise.any retorna a primeira que resolver (sucesso ou retorno de erro de negócio)
      // Se todas rejeitarem (erro de rede/auth), lança AggregateError
      return await Promise.any(candidates.map(p => tryProfile(p)));
    } catch (err) {
      console.log('Todas as tentativas autenticadas falharam.');
    }
  }

  throw new Error('Falha ao buscar posts (timeout ou erro)');
}

async function fetchInstagramFollowersInfo(username) {
  const now = Date.now();
  const MAX_ERRORS_PER_PROFILE = 5;
  const DISABLE_TIME_MS = 60 * 1000;
  const REQUEST_TIMEOUT = 3500;
  const FALLBACK_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

  const fetchFollowersPublicFallback = async () => {
    try {
      const url = `https://www.instagram.com/${encodeURIComponent(username)}/?__a=1&__d=dis`;
      const resp = await axios.get(url, {
        headers: {
          'User-Agent': FALLBACK_UA,
          'Accept': 'application/json',
          'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache',
          'Referer': `https://www.instagram.com/${encodeURIComponent(username)}/`
        },
        timeout: REQUEST_TIMEOUT,
        validateStatus: () => true
      });

      if (resp.status !== 200) return null;
      if (typeof resp.data === 'string') return null;

      const root = resp.data || {};
      const user = (root.graphql && root.graphql.user) ? root.graphql.user : (root.user || null);
      if (!user) return null;

      const followersCount = (user.edge_followed_by && typeof user.edge_followed_by.count === 'number') ? user.edge_followed_by.count : null;
      if (typeof followersCount !== 'number') return null;

      return {
        username: String(user.username || username),
        followersCount,
        isPrivate: typeof user.is_private === 'boolean' ? user.is_private : null
      };
    } catch (_) {
      return null;
    }
  };

  const available = cookieProfiles.filter(p => p.disabledUntil <= now && !isCookieLocked(p.ds_user_id))
    .sort((a, b) => {
      if (a.errorCount !== b.errorCount) return a.errorCount - b.errorCount;
      return a.lastUsed - b.lastUsed;
    });

  const candidates = available.slice(0, 3);

  const tryProfile = async (profile) => {
    if (!profile) throw new Error('No profile');
    if (isCookieLocked(profile.ds_user_id)) throw new Error('Locked');
    lockCookie(profile.ds_user_id);

    try {
      const proxyAgent = profile.proxy ? new HttpsProxyAgent(
        `http://${encodeURIComponent(profile.proxy.auth.username)}:${encodeURIComponent(profile.proxy.auth.password)}@${profile.proxy.host}:${profile.proxy.port}`,
        { rejectUnauthorized: false }
      ) : null;
      const headers = {
        "User-Agent": profile.userAgent,
        "X-IG-App-ID": "936619743392459",
        "Cookie": `sessionid=${profile.sessionid}; ds_user_id=${profile.ds_user_id}`,
        "Accept": "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": `https://www.instagram.com/${encodeURIComponent(username)}/`
      };

      const resp = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`, {
        headers,
        httpsAgent: proxyAgent,
        timeout: REQUEST_TIMEOUT
      });

      if (resp.status !== 200) throw new Error(`HTTP ${resp.status}`);
      const user = resp.data && resp.data.data && resp.data.data.user;
      if (!user || !user.username) {
        profile.lastUsed = Date.now();
        profile.errorCount = 0;
        return { success: false, error: 'Perfil inexistente' };
      }

      const followersCount = (user.edge_followed_by && typeof user.edge_followed_by.count === 'number') ? user.edge_followed_by.count : 0;
      const result = {
        success: true,
        profile: {
          username: String(user.username || username),
          followersCount,
          isPrivate: !!user.is_private,
          checkedAt: new Date().toISOString(),
          source: 'web_profile_info'
        }
      };

      profile.lastUsed = Date.now();
      profile.errorCount = 0;
      return result;
    } catch (err) {
      profile.errorCount++;
      if (profile.errorCount >= MAX_ERRORS_PER_PROFILE) profile.disabledUntil = Date.now() + DISABLE_TIME_MS;
      throw err;
    } finally {
      unlockCookie(profile.ds_user_id);
    }
  };

  if (candidates.length > 0) {
    try {
      return await Promise.any(candidates.map(p => tryProfile(p)));
    } catch (_) {}
  }

  const publicFallback = await fetchFollowersPublicFallback();
  if (publicFallback) {
    return {
      success: true,
      profile: {
        username: publicFallback.username,
        followersCount: publicFallback.followersCount,
        isPrivate: publicFallback.isPrivate == null ? false : !!publicFallback.isPrivate,
        checkedAt: new Date().toISOString(),
        source: 'public_fallback'
      }
    };
  }

  return { success: false, error: 'Falha ao buscar seguidores' };
}

