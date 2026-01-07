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
        if (!/organicos/i.test(String(tipo))) continue;
        const qtdBase = Number(additionalInfoMap['quantidade'] || record.quantidade || record.qtd || 0) || 0;
        const instaUserRaw = additionalInfoMap['instagram_username'] || record.instagramUsername || record.instauser || '';
        const instaUser = (/^https?:\/\//i.test(String(instaUserRaw))) ? String(instaUserRaw) : `https://instagram.com/${String(instaUserRaw)}`;
        const bumpsStr0 = additionalInfoMap['order_bumps'] || (record.additionalInfoPaid || []).find(it => it && it.key === 'order_bumps')?.value || '';
        let upgradeAdd = 0;
        if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000 && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) upgradeAdd = 1000;
        else if (/(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
          const map = { 150: 150, 500: 200, 1200: 800, 3000: 1000, 5000: 2500, 10000: 5000 };
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
        
        if (result.success && result.posts) {
            const shortcodes = result.posts.map(p => p.shortcode);
            console.log(`📊 IDs de posts encontrados: ${shortcodes.length}`);
            return {
                success: true,
                posts: shortcodes,
                totalPosts: shortcodes.length
            };
        }
        
        return { 
            success: false, 
            error: result.error || 'Erro ao buscar posts',
            details: result.error
        };

    } catch (error) {
        console.error('❌ Erro ao buscar posts do Instagram:', error.message);
        return {
            success: false,
            error: 'Erro ao buscar posts do Instagram',
            details: error.message
        };
    }
}
const PROFILE_CACHE = new Map();
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

async function verifyInstagramProfile(username, userAgent, ip, req, res) {
    console.log(`🔍 Iniciando verificação do perfil (PARALELO): @${username}`);
    console.log(`📊 Total de perfis disponíveis: ${cookieProfiles.length}`);
    
    const cached = getCachedProfile(username);
    if (cached) {
        console.log(`✅ Perfil @${username} retornado do cache`);
        return cached;
    }

    const BATCH_SIZE = 3;
    const MAX_TOTAL_ATTEMPTS = 6;
    const REQUEST_BUDGET_MS = 12 * 1000;
    const startedAt = Date.now();
    const usedProfileIds = new Set();
    const MAX_ERRORS_PER_PROFILE = 5;
    
    // Helper function for single profile attempt
    const tryProfile = async (profile) => {
        if (isCookieLocked(profile.ds_user_id)) throw new Error('Locked');
        lockCookie(profile.ds_user_id);
        usedProfileIds.add(profile.ds_user_id);

        try {
            const proxyAgent = profile.proxy ? new HttpsProxyAgent(`http://${profile.proxy.auth.username}:${profile.proxy.auth.password}@${profile.proxy.host}:${profile.proxy.port}`, { rejectUnauthorized: false }) : null;
            
            const headers = {
                "User-Agent": profile.userAgent,
                "X-IG-App-ID": "936619743392459",
                "Cookie": `sessionid=${profile.sessionid}; ds_user_id=${profile.ds_user_id}`,
                "Accept": "*/*",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Ch-Ua": `"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"`,
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": `"Windows"`,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "X-Asbd-Id": "129477",
                "X-Csrftoken": "missing",
                "X-Ig-Www-Claim": "0",
                "X-Instagram-Ajax": "1010394699",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": `https://www.instagram.com/${username}/`,
                "Origin": "https://www.instagram.com"
            };

            const response = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`, {
                headers,
                httpsAgent: proxyAgent,
                timeout: 8000,
                validateStatus: status => true 
            });

            if (response.status === 404) {
                return { is404: true, profile };
            }

            // Validar Content-Type para evitar falsos positivos com HTML (login page)
            const contentType = response.headers['content-type'] || '';
            if (!contentType.includes('application/json')) {
                // Se receber HTML 200 OK, provavelmente é login page ou soft block
                throw new Error(`Resposta não é JSON (Content-Type: ${contentType})`);
            }

            if (response.status < 200 || response.status >= 300) {
                throw new Error(`HTTP ${response.status}`);
            }

            const data = response.data;
            if (!data.data || !data.data.user) {
                 return { is404: true, profile };
            }

            return { user: data.data.user, profile, originalResponse: response };

        } catch (error) {
            const status = error.response?.status || 500;
            if (status === 404) return { is404: true, profile };
            
            console.warn(`⚠️ Erro ${status} no perfil ${profile.ds_user_id}`);
            profile.errorCount++;
            if (profile.errorCount >= MAX_ERRORS_PER_PROFILE) {
                profile.disabledUntil = Date.now() + (60 * 1000);
            }
            throw error;
        } finally {
            unlockCookie(profile.ds_user_id);
        }
    };

    let attempts = 0;
    while (attempts < MAX_TOTAL_ATTEMPTS) {
        if (Date.now() - startedAt > REQUEST_BUDGET_MS) break;

        const available = cookieProfiles.filter(p => 
            !usedProfileIds.has(p.ds_user_id) && 
            !isCookieLocked(p.ds_user_id) && 
            p.disabledUntil <= Date.now()
        ).sort((a, b) => a.errorCount - b.errorCount || a.lastUsed - b.lastUsed);

        if (available.length === 0) break;

        const batch = available.slice(0, BATCH_SIZE);
        console.log(`🚀 [Parallel] Iniciando lote de ${batch.length} requisições para @${username}...`);

        try {
            const result = await Promise.any(batch.map(p => tryProfile(p)));
            
            // Handle 404
            if (result.is404) {
                 try { await registerUserInControle(userAgent, ip, username, "404"); } catch (_) {}
                 const result404 = { success: false, status: 404, error: "Perfil não localizado, nome de usuário pode estar incorreto." };
                 setCache(username, result404, NEGATIVE_CACHE_TTL_MS);
                 return result404;
            }

            // Handle Success
            const { user, profile } = result;
            profile.lastUsed = Date.now();
            profile.errorCount = 0;
            
            console.log(`✅ Perfil encontrado: @${user.username} (Privado: ${user.is_private})`);

            if (user.is_private) {
                const originalImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
                let driveImageUrl = null;

                if (user.profile_pic_url_hd || user.profile_pic_url) {
                    try {
                        if (driveManager.isReady()) {
                            try {
                                console.log(`📸 Usando URL original do Instagram: ${originalImageUrl}`);
                            } catch (driveError) {
                                console.warn("Erro ao processar imagem:", driveError.message);
                            }
                        }
                    } catch (imageError) {
                        console.warn("Erro ao processar imagem:", imageError.message);
                    }
                }

                const privateResult = { 
                    success: false, 
                    status: 200, 
                    error: "Este perfil é privado. Para que o serviço seja realizado, o perfil precisa estar no modo público.",
                    code: 'INSTAUSER_PRIVATE',
                    profile: {
                        username: user.username,
                        fullName: user.full_name,
                        profilePicUrl: driveImageUrl || (originalImageUrl ? `/image-proxy?url=${encodeURIComponent(originalImageUrl)}` : null),
                        driveImageUrl: driveImageUrl,
                        isVerified: user.is_verified,
                        followersCount: user.edge_followed_by ? user.edge_followed_by.count : 0,
                        isPrivate: user.is_private
                    }
                };
                try {
                    const vu = await getCollection('validated_insta_users');
                    const linkId = req.session.linkSlug || req.query.id || req.body.id || null;
                    const doc = {
                        username: String(user.username || '').trim().toLowerCase(),
                        fullName: String(user.full_name || ''),
                        profilePicUrl: String(driveImageUrl || user.profile_pic_url_hd || user.profile_pic_url || ''),
                        isVerified: !!user.is_verified,
                        isPrivate: !!user.is_private,
                        followersCount: user.edge_followed_by ? Number(user.edge_followed_by.count || 0) : 0,
                        checkedAt: new Date().toISOString(),
                        linkId,
                        ip: String(ip || ''),
                        userAgent: String(userAgent || ''),
                        source: 'verifyInstagramProfile'
                    };
                    try { await vu.updateOne({ username: doc.username }, { $set: doc, $setOnInsert: { createdAt: new Date().toISOString() } }, { upsert: true }); } catch(_) {}
                    try { console.log('🗃️ MongoDB: validação PRIVADA upsert em validated_insta_users', { username: doc.username }); } catch(_) {}
                } catch(_) {}
                setCache(username, privateResult, NEGATIVE_CACHE_TTL_MS);
                return privateResult;
            }

            // Public Profile Success
            let driveImageUrl = null;
            const originalImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
            if (originalImageUrl) {
                try {
                    const proxyAgentForImage = profile.proxy ? new HttpsProxyAgent(`http://${profile.proxy.auth.username}:${profile.proxy.auth.password}@${profile.proxy.host}:${profile.proxy.port}`, { rejectUnauthorized: false }) : null;
                    
                    const localImageUrl = await downloadAndServeImage(originalImageUrl, user.username, proxyAgentForImage);
                    driveImageUrl = localImageUrl || originalImageUrl;

                    if (driveManager.isReady()) {
                        axios.get(originalImageUrl, {
                            responseType: 'arraybuffer',
                            timeout: 10000
                        }).then(async (imageResponse) => {
                            try {
                                const fileName = `${user.username}_profile_${Date.now()}.jpg`;
                                await driveManager.uploadBuffer(
                                    imageResponse.data,
                                    fileName,
                                    'image/jpeg',
                                    driveManager.profileImagesFolderId
                                );
                            } catch (driveErr) {
                                console.warn('Falha ao enviar imagem ao Google Drive (async):', driveErr.message);
                            }
                        }).catch(err => {});
                    }
                } catch (error) {
                    driveImageUrl = originalImageUrl;
                }
            }

            req.session.instagramProfile = {
                username: user.username,
                fullName: user.full_name,
                profilePicUrl: driveImageUrl || user.profile_pic_url_hd || user.profile_pic_url,
                isVerified: user.is_verified,
                followersCount: user.edge_followed_by ? user.edge_followed_by.count : 0,
                checkedAt: new Date().toISOString(),
                cookieUsed: profile.ds_user_id
            };

            console.log(`✅ Perfil verificado com sucesso: @${user.username} (Cookie ID: ${profile.ds_user_id})`);

            // Baserow Update
            const linkId = req.session.linkSlug || req.query.id || req.body.id;
            if (linkId) {
                (async () => {
                    try {
                        const fieldName = CONTROLE_FIELDS.LINK || 'link';
                        const filters = {};
                        filters[`filter__${fieldName}__equal`] = linkId;
                        filters['user_field_names'] = 'true';

                        const result = await baserowManager.getTableRows(BASEROW_TABLES.CONTROLE, {
                            filters,
                            size: 1
                        });

                        if (result.success) {
                            const row = result.rows.length > 0 ? result.rows[0] : null;
                            if (row) {
                                const fingerprint = generateFingerprint(ip, userAgent);
                                const updateData = {
                                    'user-agent': fingerprint,
                                    'ip': ip,
                                    'instauser': user.username,
                                    'statushttp': '200',
                                    'teste': ''
                                };
                                await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, mapControleData(updateData));
                                console.log(`📊 Linha do Baserow atualizada para link=${linkId}, id=${row.id}`);
                            } else {
                                const fingerprint = generateFingerprint(ip, userAgent);
                                const data = {
                                    'user-agent': fingerprint || '',
                                    'ip': ip || '',
                                    'instauser': user.username || '',
                                    'link': linkId,
                                    'teste': '',
                                    'statushttp': '200',
                                    'criado': new Date().toISOString()
                                };
                                await baserowManager.createRow(BASEROW_TABLES.CONTROLE, mapControleData(data));
                            }
                        }
                    } catch (err) {
                        console.error('❌ Erro background Baserow:', err.message);
                    }
                })();
            }

            try {
                const vu = await getCollection('validated_insta_users');
                const linkId = req.session.linkSlug || req.query.id || req.body.id || null;
                const doc = {
                    username: String(user.username || '').trim().toLowerCase(),
                    fullName: String(user.full_name || ''),
                    profilePicUrl: String(driveImageUrl || user.profile_pic_url_hd || user.profile_pic_url || ''),
                    isVerified: !!user.is_verified,
                    isPrivate: !!user.is_private,
                    followersCount: user.edge_followed_by ? Number(user.edge_followed_by.count || 0) : 0,
                    checkedAt: new Date().toISOString(),
                    linkId,
                    ip: String(ip || ''),
                    userAgent: String(userAgent || ''),
                    source: 'verifyInstagramProfile'
                };
                try { await vu.updateOne({ username: doc.username }, { $set: doc, $setOnInsert: { createdAt: new Date().toISOString() } }, { upsert: true }); } catch(_) {}
                try { console.log('🗃️ MongoDB: validação PÚBLICA upsert em validated_insta_users', { username: doc.username }); } catch(_) {}
            } catch (mongoErr) {
                try { console.error('❌ Falha ao registrar validação em MongoDB:', mongoErr?.message || String(mongoErr)); } catch(_) {}
            }

            const instauserExists = await checkInstauserExists(username);
            
            const responseProfile = {
                username: user.username,
                fullName: user.full_name,
                profilePicUrl: driveImageUrl || (originalImageUrl ? `/image-proxy?url=${encodeURIComponent(originalImageUrl)}` : null),
                isVerified: user.is_verified,
                followersCount: user.edge_followed_by ? user.edge_followed_by.count : 0,
                followingCount: user.edge_follow ? user.edge_follow.count : 0,
                postsCount: (user.edge_owner_to_timeline_media && user.edge_owner_to_timeline_media.count) ? user.edge_owner_to_timeline_media.count : 0,
                isPrivate: user.is_private,
                alreadyTested: instauserExists
            };
            
            const okResult = { success: true, status: 200, profile: responseProfile };
            setCache(username, okResult, CACHE_TTL_MS);
            return okResult;

        } catch (err) {
            attempts += batch.length;
            console.warn("⚠️ Lote falhou, tentando próximo...");
        }
    }
    
    // Fallback error
    // FALLBACK: Tentar scraping público HTML (sem proxy/cookies)
    console.log(`⚠️ Todas as tentativas com cookies falharam para @${username}. Tentando fallback público HTML...`);
    try {
        const publicHeaders = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document"
        };
        
        // Try direct connection (no proxy) - 1st attempt
        // Adicionando timestamp para evitar cache local
        const htmlResponse = await axios.get(`https://www.instagram.com/${username}/?__a=1&__d=dis`, {
             headers: { ...publicHeaders, "X-Requested-With": "XMLHttpRequest" },
             timeout: 5000,
             validateStatus: status => status < 400
        }).catch(async () => {
             // 2nd attempt: pure HTML scrape
             return await axios.get(`https://www.instagram.com/${username}/`, {
                headers: publicHeaders,
                timeout: 6000,
                validateStatus: status => status < 400
            });
        });
        
        const data = htmlResponse.data;

        // Se retornou JSON (API pública não oficial)
        if (typeof data === 'object' && data.graphql && data.graphql.user) {
             const user = data.graphql.user;
             const profile = {
                username: user.username,
                fullName: user.full_name || user.username,
                profilePicUrl: user.profile_pic_url_hd || user.profile_pic_url || "https://upload.wikimedia.org/wikipedia/commons/a/ac/Default_pfp.jpg",
                isVerified: user.is_verified,
                followersCount: user.edge_followed_by ? user.edge_followed_by.count : 0,
                followingCount: user.edge_follow ? user.edge_follow.count : 0,
                postsCount: user.edge_owner_to_timeline_media ? user.edge_owner_to_timeline_media.count : 0,
                isPrivate: user.is_private,
                alreadyTested: await checkInstauserExists(username)
            };
            const okResult = { success: true, status: 200, profile: profile };
            setCache(username, okResult, CACHE_TTL_MS);
            return okResult;
        }
        
        if (typeof data === 'string') {
            const html = data;
            // Regex para extrair metadados
            const ogTitle = (html.match(/<meta property="og:title" content="([^"]+)"/i) || [])[1];
            const ogDescription = (html.match(/<meta property="og:description" content="([^"]+)"/i) || [])[1];
            const ogImage = (html.match(/<meta property="og:image" content="([^"]+)"/i) || [])[1];
            
            if (ogDescription) {
                console.log(`✅ Fallback HTML sucesso: ${ogDescription}`);
                // Ex: "100 Followers, 50 Following, 20 Posts - See Instagram photos..."
                const descParts = ogDescription.split(' - ')[0];
                const stats = descParts.split(', ');
                
                const parseCount = (s) => {
                    if (!s) return 0;
                    s = s.replace(/,/g, '').trim();
                    let m = 1;
                    if (s.toLowerCase().includes('k')) m = 1000;
                    if (s.toLowerCase().includes('m')) m = 1000000;
                    return parseFloat(s) * m;
                };

                let followers = 0;
                let following = 0;
                let posts = 0;
                
                stats.forEach(stat => {
                    if (stat.includes('Followers')) followers = parseCount(stat.replace('Followers', ''));
                    if (stat.includes('Following')) following = parseCount(stat.replace('Following', ''));
                    if (stat.includes('Posts')) posts = parseCount(stat.replace('Posts', ''));
                });
                
                let fullName = '';
                if (ogTitle) {
                    // Ex: "Name (@username) • Instagram..."
                    const titleParts = ogTitle.split(' (@')[0];
                    fullName = titleParts;
                }
                
                const isPrivate = html.includes('"is_private":true') || html.includes('This account is private');
                const isVerified = html.includes('"is_verified":true');
                
                const profile = {
                    username: username,
                    fullName: fullName || username,
                    profilePicUrl: ogImage || "https://upload.wikimedia.org/wikipedia/commons/a/ac/Default_pfp.jpg",
                    isVerified: isVerified,
                    followersCount: followers,
                    followingCount: following,
                    postsCount: posts,
                    isPrivate: isPrivate,
                    alreadyTested: await checkInstauserExists(username)
                };
                
                const okResult = { success: true, status: 200, profile: profile };
                setCache(username, okResult, CACHE_TTL_MS);
                return okResult;
            } else {
                 console.warn(`⚠️ Fallback HTML: ogDescription não encontrado. Title: ${ogTitle || 'N/A'}. HTML length: ${html.length}`);
             }
        }
    } catch (fallbackErr) {
        console.error(`❌ Fallback HTML falhou: ${fallbackErr.message}`);
    }

    try {
        console.log("📊 Tentando registrar erro no Baserow...");
        await registerUserInControle(userAgent, ip, username, "error");
    } catch (_) {}
    
    // LAST RESORT: Return a dummy profile to allow the user to proceed
    console.warn(`⚠️ Verificação falhou totalmente para @${username}. Retornando perfil provisório.`);
    const dummyProfile = {
        username: username,
        fullName: username,
        profilePicUrl: "https://upload.wikimedia.org/wikipedia/commons/a/ac/Default_pfp.jpg",
        isVerified: false,
        followersCount: 0,
        followingCount: 0,
        postsCount: 0,
        isPrivate: false,
        alreadyTested: await checkInstauserExists(username),
        isProvisional: true
    };
    const okResult = { success: true, status: 200, profile: dummyProfile, warning: "Perfil não verificado completamente" };
    setCache(username, okResult, 30 * 1000); // 30 seconds cache
    return okResult;

    // try { setCache(username, { success: false, status: 503, error: "Erro ao verificar perfil após múltiplas tentativas. Tente novamente mais tarde." }, NEGATIVE_CACHE_TTL_MS); } catch (_) {}
    // throw new Error("Não foi possível verificar o perfil com os perfis de cookie disponíveis. Tente novamente mais tarde.");
}

const app = express();
app.set("trust proxy", true); // Confiar em cabeçalhos de proxy

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
}

// Registrar usuário na tabela controle
async function registerUserInControle(userAgent, ip, instauser, statushttp) {
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
}

// Atualizar status do serviço na tabela controle
async function updateTesteStatus(recordId, testeStatus) {
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
}

// Verificar se instauser já foi usado
async function checkInstauserExists(instauser) {
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
}

// Função para atualizar o campo 'teste' para 'OK' na linha correta do Baserow
async function updateBaserowTesteStatus(instauser) {
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
}

// Lista de fingerprints bloqueados manualmente
const blockedFingerprints = ['e7DMDkz0nWbVn4O3OPoE'];
// Remover whitelist de fingerprints
// const allowedFingerprints = ['e7DMDkz0nWbVn4O3OPoE'];

// ==================== ROTAS ====================

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

// Configurar view engine ANTES de qualquer render
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use((req, res, next) => { res.locals.PIXEL_ID = process.env.PIXEL_ID || ''; next(); });

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
    const doc = await col.findOne(filter, { projection: { _id: 1, instauser: 1, instagramUsername: 1, additionalInfoPaid: 1, additionalInfo: 1, customer: 1 } });
    if (!doc) return null;
    const arrPaid = Array.isArray(doc?.additionalInfoPaid) ? doc.additionalInfoPaid : [];
    const arrOrig = Array.isArray(doc?.additionalInfo) ? doc.additionalInfo : [];
    const map = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
    const iu = doc.instauser || doc.instagramUsername || map['instagram_username'] || '';
    const phoneFromCustomer = (doc && doc.customer && doc.customer.phone) ? String(doc.customer.phone).replace(/\D/g, '') : '';
    const phoneFromMap = map['phone'] ? String(map['phone']).replace(/\D/g, '') : '';
    const phoneDigits = phoneFromCustomer || phoneFromMap || '';
    const tl = await getCollection('temporary_links');

    // Primeiro: tentar reutilizar link existente por telefone (um token único por telefone)
    if (phoneDigits) {
      const existingByPhone = await tl.findOne({ purpose: 'refil', phone: phoneDigits });
      if (existingByPhone) {
        await col.updateOne({ _id: doc._id }, { $set: { refilLinkId: existingByPhone.id } });
        const sets = { instauser: existingByPhone.instauser || iu || null };
        await tl.updateOne({ id: existingByPhone.id }, { $set: sets, $addToSet: { orders: String(doc._id) } });
        return existingByPhone;
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
      createdAt: new Date().toISOString(),
      expiresAt: new Date(info.expiresAt).toISOString()
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

// Rotas diretas antes de estáticos
app.use((req, res, next) => {
  try {
    if (req.method === 'GET' && req.path === '/engajamento') {
      return res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '', ENG_MODE: true });
    }
    if (req.method === 'GET' && req.path === '/servicos') {
      return res.render('servicos');
    }
  } catch (_) {}
  next();
});
app.get('/engajamento', (req, res) => {
  try {
    return res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '', ENG_MODE: true });
  } catch (e) {
    return res.status(500).send('Erro ao renderizar engajamento');
  }
});
app.get('/servicos', (req, res) => {
  try {
    return res.render('servicos');
  } catch (e) {
    return res.status(500).send('Erro ao renderizar serviços');
  }
});

// Servir arquivos estáticos
app.use(express.static("public"));
app.use('/temp-images', express.static(path.join(__dirname, 'temp_images')));

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
      timeout: 10000,
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
    res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '' }, (err, html) => {
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
    res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '' }, (err, html) => {
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
  res.render('engajamento', { PIXEL_ID: process.env.PIXEL_ID || '' }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar engajamento:', err.message);
      return res.status(500).send('Erro ao renderizar engajamento');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página Serviços (três serviços iguais ao principal)
app.get('/servicos', (req, res) => {
  console.log('🧩 Acessando rota /servicos');
  res.render('servicos', {}, (err, html) => {
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
  res.render('servicos-instagram', { PIXEL_ID: process.env.PIXEL_ID || '' }, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar servicos-instagram:', err.message);
      return res.status(500).send('Erro ao renderizar servicos-instagram');
    }
    res.type('text/html');
    res.send(html);
  });
});

// Página de Refil
app.get('/refil', async (req, res) => {
  console.log('🔁 Acessando rota /refil');
  try {
    const token = String(req.query.token || '').trim();
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
        if (!isValid) {
            // Tentar recuperar do banco para ver se apenas expirou ou renovar
            try {
                const tl = await getCollection('temporary_links');
                const linkRec = await tl.findOne({ id: token });
                if (linkRec && String(linkRec.purpose || '').toLowerCase() === 'refil') {
                    const nowMs = Date.now();
                    const newExp = new Date(nowMs + 24 * 60 * 60 * 1000).toISOString();
                    await tl.updateOne({ id: token }, { $set: { expiresAt: newExp } });
                    isValid = true;
                    if (req.session) {
                        req.session.refilAccessAllowed = true;
                        req.session.linkSlug = token;
                        req.session.linkAccessTime = Date.now();
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
             token = linkRec.id; // Atualizar token para o válido encontrado
             // Renovar e logar
             const nowMs = Date.now();
             const newExp = new Date(nowMs + 24 * 60 * 60 * 1000).toISOString();
             await tl.updateOne({ id: token }, { $set: { expiresAt: newExp } });
             isValid = true;
             if (req.session) {
                req.session.refilAccessAllowed = true;
                req.session.linkSlug = token;
                req.session.linkAccessTime = Date.now();
             }
             console.log('🔁 Refil: Acesso recuperado via telefone:', digits);
           }
        }
      } catch(_) {}
    }
  } catch(_) {}
  if (!(req.session && req.session.refilAccessAllowed)) {
    const from = '/refil';
    const qs = token ? `from=${encodeURIComponent(from)}&token=${encodeURIComponent(token)}` : `from=${encodeURIComponent(from)}`;
    return res.redirect(`/restrito?${qs}`);
  }
  res.render('refil', {}, (err, html) => {
    if (err) {
      console.error('❌ Erro ao renderizar refil:', err.message);
      return res.status(500).send('Erro ao carregar página de refil');
    }
    res.type('text/html');
    res.send(html);
  });
});

// API para solicitar refil (proxy para smmrefil)
app.post('/api/refil/create', async (req, res) => {
    try {
        const { order_id, username } = req.body || {};
        if (!order_id) return res.status(400).json({ error: 'missing_order_id' });
        const axios = require('axios');
        const payload = { order_id: String(order_id).trim(), username: String(username || 'arraso') };
        console.log('🔁 [Refil] Solicitando:', payload);
        const response = await axios.post('https://refilfama24h.net/api/refill/create', payload, { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, timeout: 20000 });
        console.log('✅ [Refil] OK status:', response.status);
        return res.status(200).json(response.data);
    } catch (err) {
        const status = err.response?.status || 500;
        const details = err.response?.data || { message: err.message };
        console.error('❌ [Refil] Erro:', { status, details });
        return res.status(status).json({ error: 'refil_error', details });
    }
});

// API: criar cobrança PIX via Woovi
app.post('/api/woovi/charge', async (req, res) => {
    const WOOVI_AUTH = process.env.WOOVI_AUTH || 'Q2xpZW50X0lkXzI1OTRjODMwLWExN2YtNDc0Yy05ZTczLWJjNDRmYTc4NTU2NzpDbGllbnRfU2VjcmV0X0NCVTF6Szg4eGJyRTV0M1IxVklGZEpaOHZLQ0N4aGdPR29UQnE2dDVWdU09';
    const {
        correlationID,
        value,
        comment,
        customer,
        additionalInfo,
        profile_is_private
    } = req.body || {};

    if (!value || typeof value !== 'number') {
        return res.status(400).json({ error: 'invalid_value', message: 'Campo value (centavos) é obrigatório.' });
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

    const customerPayload = {
        name: sanitizeText((customer && customer.name) ? customer.name : 'Cliente Checkout'),
        phone: normalizePhone((customer && customer.phone) ? customer.phone : ''),
    };
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

    const payload = {
        correlationID: chargeCorrelationID,
        value,
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
            const addInfoArr = Array.isArray(sanitizedAdditionalFiltered) ? sanitizedAdditionalFiltered : [];
            const addInfo = addInfoArr.reduce((acc, item) => {
                const k = String(item?.key || '').trim();
                const v = String(item?.value || '').trim();
                acc[k] = v;
                return acc;
            }, {});

            let utms = {};
            try {
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
                status: 'pendente',
                qtd,
                tipo,
                utms,
                geolocation,

                // Demais campos já utilizados pelo app
                valueCents: value,
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
            console.log('🗃️ MongoDB: pedido do checkout persistido (insertedId=', insertResult.insertedId, ')');
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
    const filter = { _id: record._id };
    
    const instaUser = record?.instagramUsername || record?.instauser || '';
    const identifier = record?.identifier;
    
    // Check privacy before dispatch
    let isPriv = record.isPrivate === true || record.profilePrivacy?.isPrivate === true;
    
    if (!isPriv && instaUser) {
        try {
            // Live privacy check
            const check = await verifyInstagramProfile(instaUser, 'ProcessFulfillment-Check', '127.0.0.1', { session: {} }, null);
            if (check && (check.code === 'INSTAUSER_PRIVATE' || (check.profile && check.profile.isPrivate))) {
                isPriv = true;
                await col.updateOne(filter, { 
                    $set: { 
                        isPrivate: true, 
                        'profilePrivacy.isPrivate': true, 
                        'profilePrivacy.updatedAt': new Date().toISOString() 
                    } 
                });
                console.log('🔒 Profile detected as PRIVATE during fulfillment:', instaUser);
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
    const serviceId = (/^mistos$/i.test(tipo)) ? 659 : (/^brasileiros$/i.test(tipo)) ? 23 : null;
    const arrPaid = Array.isArray(record?.additionalInfoPaid) ? record.additionalInfoPaid : [];
    const arrOrig = Array.isArray(record?.additionalInfo) ? record.additionalInfo : [];
    const bumpsStr = (arrPaid.find(it => it && it.key === 'order_bumps')?.value) || (arrOrig.find(it => it && it.key === 'order_bumps')?.value) || '';
    const hasUpgrade = typeof bumpsStr === 'string' && /(^|;)upgrade:\d+/i.test(bumpsStr);
    const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
    let upgradeAdd = 0;
    if (hasUpgrade && isFollowers) {
        if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) {
            upgradeAdd = 1000;
        } else {
            const map = { 150: 150, 500: 200, 1200: 800, 3000: 1000, 5000: 2500, 10000: 5000 };
            upgradeAdd = map[qtdBase] || 0;
        }
    }
    let qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
    
    // Ajuste: O provedor (Fama24h - serviço 659) exige mínimo de 100.
    if (serviceId === 659 && qtd > 0 && qtd < 100) {
        qtd = 100;
    }

    const isOrganicos = /organicos/i.test(tipo);
    if (!isOrganicos) {
        const canSend = !!key && !!serviceId && !!instaUser && qtd > 0;
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
                const payload = new URLSearchParams({ key, action: 'add', service: String(serviceId), link: String(instaUser), quantity: String(qtd) });
                try {
                    const famaResp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                    const famaData = famaResp.data || {};
                    const orderId = famaData.order || famaData.id || null;
                    await col.updateOne(filter, { $set: { fama24h: { orderId, status: orderId ? 'created' : 'unknown', requestPayload: { service: serviceId, link: instaUser, quantity: qtd }, response: famaData, requestedAt: new Date().toISOString() } } });
                    try { await broadcastPaymentPaid(identifier, correlationID); } catch(_) {}
                } catch (err) {
                    console.error('Erro ao enviar para Fama24h:', err.message);
                    await col.updateOne(filter, { $set: { 'fama24h.status': 'error', 'fama24h.error': err.message } });
                }
            }
        }
    } else {
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
    
    // Order Bumps (Views/Likes)
    try {
        const additionalInfoMap = (arrPaid.length ? arrPaid : arrOrig).reduce((acc, it) => { const k = String(it?.key||'').trim(); if (k) acc[k] = String(it?.value||'').trim(); return acc; }, {});
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
        // Links selecionados para orderbumps
        const mapPaid = record?.additionalInfoMapPaid || {};
        const viewsLinkRaw = mapPaid['orderbump_post_views'] || additionalInfoMap['orderbump_post_views'] || (arrPaid.find(it => it && it.key === 'orderbump_post_views')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_views')?.value) || '';
        const likesLinkRaw = mapPaid['orderbump_post_likes'] || additionalInfoMap['orderbump_post_likes'] || (arrPaid.find(it => it && it.key === 'orderbump_post_likes')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_likes')?.value) || '';
        
        try { console.log('🔎 orderbump_links_raw', { identifier, correlationID, viewsLinkRaw, likesLinkRaw, viewsQty, likesQty }); } catch(_) {}
        const sanitizeLink = (s) => {
            const v = String(s || '').replace(/[`\s]/g, '').trim();
            const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v);
            return ok ? (v.endsWith('/') ? v : (v + '/')) : '';
        };
        const viewsLink = sanitizeLink(viewsLinkRaw);
        const likesLink = sanitizeLink(likesLinkRaw);
        try { console.log('🔎 orderbump_links_sanitized', { viewsLink, likesLink }); } catch(_) {}

        if (viewsQty > 0 && viewsLink) {
            if (process.env.FAMA24H_API_KEY || '') {
                const axios = require('axios');
                const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
                try { console.log('🚀 sending_fama24h_views', { service: 250, link: viewsLink, quantity: viewsQty }); } catch(_) {}
                try {
                    const respViews = await axios.post('https://fama24h.net/api/v2', payloadViews.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                    const dataViews = respViews.data || {};
                    const orderIdViews = dataViews.order || dataViews.id || null;
                    await col.updateOne(filter, { $set: { fama24h_views: { orderId: orderIdViews, status: orderIdViews ? 'created' : 'unknown', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, response: dataViews, requestedAt: new Date().toISOString() } } });
                } catch (e2) {
                    try { console.error('❌ fama24h_views_error', e2?.response?.data || e2?.message || String(e2), { link: viewsLink, quantity: viewsQty }); } catch(_) {}
                    await col.updateOne(filter, { $set: { fama24h_views: { error: e2?.response?.data || e2?.message || String(e2), requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
                }
            }
        } else if (viewsQty > 0 && !viewsLink) {
            try { console.warn('⚠️ views_link_invalid', { viewsLinkRaw, sanitized: viewsLink }); } catch(_) {}
        }
        
        const alreadyLikes = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
        if (likesQty > 0 && likesLink && !alreadyLikes) {
            if (process.env.FAMA24H_API_KEY || '') {
                const lockUpdate = await col.updateOne(
                    { ...filter, 'fama24h_likes.status': { $exists: false } },
                    { $set: { fama24h_likes: { status: 'processing', requestedAt: new Date().toISOString() } } }
                );
                if (lockUpdate.modifiedCount > 0) {
                    const axios = require('axios');
                    const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '666', link: String(likesLink), quantity: String(likesQty) });
                    try { console.log('🚀 sending_fama24h_likes', { service: 666, link: likesLink, quantity: likesQty }); } catch(_) {}
                    try {
                        const respLikes = await axios.post('https://fama24h.net/api/v2', payloadLikes.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                        const dataLikes = respLikes.data || {};
                        const orderIdLikes = dataLikes.order || dataLikes.id || null;
                        await col.updateOne(filter, { $set: { 'fama24h_likes.orderId': orderIdLikes, 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 666, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': dataLikes } });
                    } catch (e3) {
                        try { console.error('❌ fama24h_likes_error', e3?.response?.data || e3?.message || String(e3), { link: likesLink, quantity: likesQty }); } catch(_) {}
                        await col.updateOne(filter, { $set: { 'fama24h_likes.error': e3?.response?.data || e3?.message || String(e3), 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 666, link: likesLink, quantity: likesQty } } });
                    }
                }
            }
        } else if (likesQty > 0 && !likesLink) {
            try { console.warn('⚠️ likes_link_invalid', { likesLinkRaw, sanitized: likesLink }); } catch(_) {}
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
      const paidFlag = charge.paid === true || /paid/.test(status);
      if (paidFlag) {
        const col = await getCollection('checkout_orders');
        const pixMethod = charge.paymentMethods?.pix || {};
        const identifier = charge.identifier || null;
        const correlationID = charge.correlationID || null;
        const paidAtRaw = charge.paidAt || null;
        const txId = pixMethod?.txId || charge?.transactionID || null;
        const endToEndId = charge?.endToEndId || null;
        const setFields = {
          status: 'pago',
          'woovi.status': 'pago',
          paidAt: new Date().toISOString(),
        };
        if (paidAtRaw) setFields['woovi.paidAt'] = paidAtRaw;
        if (typeof endToEndId === 'string') setFields['woovi.endToEndId'] = endToEndId;
        if (typeof txId === 'string') setFields['woovi.paymentMethods.pix.txId'] = txId;
        if (typeof pixMethod.status === 'string') setFields['woovi.paymentMethods.pix.status'] = pixMethod.status;
        if (typeof pixMethod.value === 'number') setFields['woovi.paymentMethods.pix.value'] = pixMethod.value;
        const conds = [ { 'woovi.chargeId': id } ];
        if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
        if (correlationID) conds.push({ correlationID });
        const filter = { $or: conds };
        const upd = await col.updateOne(filter, { $set: setFields });
        if (!upd.matchedCount && identifier) {
          await col.updateOne({ identifier }, { $set: setFields });
        }
        try {
          const record = await col.findOne(filter);
          await processOrderFulfillment(record, col, req);
        } catch (e) {
          console.error('Error processing fulfillment in charge-status:', e);
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
      await col.updateOne({ 'fama24h.orderId': Number(orderParam) }, { $set: { 'fama24h.statusPayload': data, 'fama24h.lastStatusAt': new Date().toISOString() } });
    } catch (_) {}
    return res.json({ ok: true, data: resp.data || {} });
  } catch (e) {
    try { console.error('🛰️ Fama status error', e?.response?.data || e?.message || String(e)); } catch(_) {}
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Rota para liberar acesso à /perfil após validação de link temporário
app.get('/:slug', async (req, res, next) => {
    const { slug } = req.params;
    console.log('🔎 Capturado em /:slug:', slug);
    // EXCEÇÕES explícitas devem ser tratadas antes de qualquer validação
    if (slug === 'checkout') {
        return res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '' });
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
                return res.render('refil', {}, (err, html) => {
                  if (err) { console.error('❌ Erro ao renderizar refil via slug:', err.message); return res.status(500).send('Erro ao carregar página de refil'); }
                  res.type('text/html');
                  res.send(html);
                });
              }
            } catch(_) {}
            req.session.perfilAccessAllowed = true;
            // Atualizar linha do Baserow com IP e User-Agent (mantém igual)
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
    const userAgent = req.get("User-Agent") || "";
    const ip = req.realIP || req.ip || req.connection.remoteAddress || "";

    if (!username || username.length < 1) {
        return res.status(400).json({
            success: false,
            error: "Nome de usuário é obrigatório"
        });
    }

    try {
        // Usa verifyInstagramProfile mas ignora a verificação de "já usado" do endpoint principal
        // A função verifyInstagramProfile em si não bloqueia, apenas retorna os dados
        const result = await verifyInstagramProfile(username, userAgent, ip, req, res);
        
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
    const { username, utms } = req.body;
    const userAgent = req.get("User-Agent") || "";
    const ip = req.realIP || req.ip || req.connection.remoteAddress || "";

    if (!username || username.length < 1) {
        return res.status(400).json({
            success: false,
            error: "Nome de usuário é obrigatório"
        });
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

    // (Verificação antecipada de uso removida para garantir atualização de dados do perfil)
    // A verificação será feita dentro de verifyInstagramProfile e retornada no objeto result

    try {
      // Chamar a função através da fila para limitar concorrência e evitar sobrecarga
      const result = await instagramQueue.add(() => verifyInstagramProfile(username, userAgent, ip, req, res));
        try {
          if (result && result.success && result.profile && result.profile.username) {
            const vu = await getCollection('validated_insta_users');
            const doc = {
              username: String(result.profile.username || '').trim().toLowerCase(),
              fullName: String(result.profile.fullName || ''),
              profilePicUrl: String(result.profile.profilePicUrl || ''),
              isVerified: !!result.profile.isVerified,
              isPrivate: !!result.profile.isPrivate,
              followersCount: typeof result.profile.followersCount === 'number' ? result.profile.followersCount : 0,
              checkedAt: new Date().toISOString(),
              linkId: req.session ? req.session.linkSlug || null : null,
              ip: String(ip || ''),
              userAgent: String(userAgent || ''),
              source: 'api.checkInstagramProfile',
              utms: utms || {}
            };
            try { await vu.updateOne({ username: doc.username }, { $set: doc, $setOnInsert: { createdAt: new Date().toISOString() } }, { upsert: true }); } catch(_) {}
          }
        } catch (err) { try { console.error('fallback insert error', err?.message || String(err)); } catch(_) {} }

        // Se o perfil já foi testado, retornar 409 mas com os dados atualizados do perfil
        if (result && result.success && result.profile && result.profile.alreadyTested) {
             return res.status(409).json({
                success: false,
                error: "Este perfil já foi testado anteriormente. O serviço de teste já foi realizado para este usuário.",
                code: "INSTAUSER_ALREADY_USED",
                profile: result.profile
            });
        }

        return res.status(result.status || 200).json(result);
    } catch (error) {
        console.error("Erro na verificação de perfil:", error.message);
        return res.status(500).json({
            success: false,
            error: "Erro ao verificar perfil. Tente novamente."
        });
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
            const selectedServiceId = serviceMap[selectedServiceKey] || '659';
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
        const selectedServiceId = serviceMap[selectedServiceKey] || '659';
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
      const result = await baserowManager.createRow(BASEROW_TABLES.CONTROLE, mapControleData(data));
      if (result.success) {
        createdIds.push(result.row.id);
      } else {
        errors.push({ tel, error: result.error });
      }
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
          const serviceId = (/^mistos$/i.test(tipo)) ? 659 : (/^brasileiros$/i.test(tipo)) ? 23 : null;
          const bumpsStr0 = additionalInfoMap['order_bumps'] || '';
          const hasUpgrade = typeof bumpsStr0 === 'string' && /(^|;)upgrade:\d+/i.test(bumpsStr0);
          const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
          let upgradeAdd = 0;
          if (hasUpgrade && isFollowers) {
            if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) {
              upgradeAdd = 1000;
            } else {
              const map = { 50: 50, 150: 150, 500: 200, 1200: 800, 3000: 1000, 5000: 2500, 10000: 5000 };
              upgradeAdd = map[qtdBase] || 0;
            }
          }
          const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
          const isOrganicos = /organicos/i.test(tipo);
          if (!isOrganicos) {
            const canSend = !!key && !!serviceId && !!instaUser && qtd > 0 && !alreadySentFama;
            if (canSend) {
              const axios = require('axios');
              const payload = new URLSearchParams({ key, action: 'add', service: String(serviceId), link: String(instaUser), quantity: String(qtd) });
              console.log('➡️ Enviando pedido Fama24h', { service: serviceId, link: instaUser, quantity: qtd });
              try {
                const famaResp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const famaData = famaResp.data || {};
                console.log('✅ Fama24h resposta', { status: famaResp.status, data: famaData });
                const orderId = famaData.order || famaData.id || null;
                await col.updateOne(filter, { $set: { fama24h: { orderId, status: orderId ? 'created' : 'unknown', requestPayload: { service: serviceId, link: instaUser, quantity: qtd }, response: famaData, requestedAt: new Date().toISOString() } } });
                try { await broadcastPaymentPaid(charge?.identifier, charge?.correlationID); } catch(_) {}
              } catch (fErr) {
                console.error('❌ Fama24h erro', fErr?.response?.data || fErr?.message || String(fErr));
                await col.updateOne(filter, { $set: { fama24h: { error: fErr?.response?.data || fErr?.message || String(fErr), requestPayload: { service: serviceId, link: instaUser, quantity: qtd }, requestedAt: new Date().toISOString() } } });
              }
            } else {
              console.log('ℹ️ Fama24h não enviado', { hasKey: !!key, tipo, qtd: qtdBase, instaUser, alreadySentFama, hasUpgrade });
            }
          } else {
            const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
            const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
            const canSendFS = !!keyFS && !!instaUser && qtd > 0 && !alreadySentFS;
            if (canSendFS) {
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
            if (typeof bumpsStr === 'string' && bumpsStr) {
              const parts = bumpsStr.split(';');
              const vPart = parts.find(p => /^views:\d+$/i.test(p.trim()));
              const lPartStatus = parts.find(p => /^likes:\d+$/i.test(p.trim()));
              if (vPart) {
                const num = Number(vPart.split(':')[1]);
                if (!Number.isNaN(num) && num > 0) viewsQty = num;
              }
              if (lPartStatus) {
                const numL = Number(lPartStatus.split(':')[1]);
                if (!Number.isNaN(numL) && numL > 0) likesQtyForStatus = numL;
              }
            }
            if (viewsQty > 0) {
              if ((process.env.FAMA24H_API_KEY || '')) {
                const axios = require('axios');
                const sanitizeLink = (s) => { const v = String(s || '').replace(/[`\s]/g, '').trim(); const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v); return ok ? (v.endsWith('/') ? v : (v + '/')) : ''; };
                const mapPaid2 = record?.additionalInfoMapPaid || {};
                const selectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};
                const selViews = selectedFor && selectedFor.views && selectedFor.views.link ? String(selectedFor.views.link) : '';
                const viewsLinkRaw = mapPaid2['orderbump_post_views'] || selViews || additionalInfoMap['orderbump_post_views'] || (arrPaid.find(it => it && it.key === 'orderbump_post_views')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_views')?.value) || '';
                try { console.log('🔎 orderbump_views_raw', { identifier: charge?.identifier, correlationID: charge?.correlationID, viewsLinkRaw, viewsQty }); } catch(_) {}
                const viewsLink = sanitizeLink(viewsLinkRaw);
                try { console.log('🔎 orderbump_views_sanitized', { viewsLink }); } catch(_) {}
                if (!viewsLink) {
                  await col.updateOne(filter, { $set: { fama24h_views: { error: 'invalid_link', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
                } else {
                  const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
                  try { console.log('🚀 sending_fama24h_views', { service: 250, link: viewsLink, quantity: viewsQty }); } catch(_) {}
                  try {
                    const respViews = await axios.post('https://fama24h.net/api/v2', payloadViews.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                    const dataViews = respViews.data || {};
                    const orderIdViews = dataViews.order || dataViews.id || null;
                    await col.updateOne(filter, { $set: { fama24h_views: { orderId: orderIdViews, status: orderIdViews ? 'created' : 'unknown', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, response: dataViews, requestedAt: new Date().toISOString() } } });
                  } catch (e2) {
                    try { console.error('❌ fama24h_views_error', e2?.response?.data || e2?.message || String(e2), { link: viewsLink, quantity: viewsQty }); } catch(_) {}
                    await col.updateOne(filter, { $set: { fama24h_views: { error: e2?.response?.data || e2?.message || String(e2), requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
                  }
                }
              }
            }
            if (likesQtyForStatus > 0) {
              if ((process.env.FAMA24H_API_KEY || '')) {
                const axios = require('axios');
                const sanitizeLinkL = (s) => { const v = String(s || '').replace(/[`\s]/g, '').trim(); const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v); return ok ? (v.endsWith('/') ? v : (v + '/')) : ''; };
                const selLikes = selectedFor && selectedFor.likes && selectedFor.likes.link ? String(selectedFor.likes.link) : '';
                const likesLinkRaw = mapPaid2['orderbump_post_likes'] || selLikes || additionalInfoMap['orderbump_post_likes'] || (arrPaid.find(it => it && it.key === 'orderbump_post_likes')?.value) || (arrOrig.find(it => it && it.key === 'orderbump_post_likes')?.value) || '';
                const likesLinkSel = sanitizeLinkL(likesLinkRaw);
                try { console.log('🔎 orderbump_likes_raw', { identifier: charge?.identifier, correlationID: charge?.correlationID, likesLinkRaw, likesQtyForStatus }); } catch(_) {}
                try { console.log('🔎 orderbump_likes_sanitized', { likesLinkSel }); } catch(_) {}
                const alreadyLikes2 = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
                if (!likesLinkSel) {
                  await col.updateOne(filter, { $set: { fama24h_likes: { error: 'invalid_link', requestPayload: { service: 666, link: likesLinkSel, quantity: likesQtyForStatus }, requestedAt: new Date().toISOString() } } });
                } else if (!alreadyLikes2) {
                  const lockUpdate = await col.updateOne(
                    { ...filter, 'fama24h_likes.status': { $exists: false } },
                    { $set: { fama24h_likes: { status: 'processing', requestedAt: new Date().toISOString() } } }
                  );
                  if (lockUpdate.modifiedCount > 0) {
                      const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '666', link: String(likesLinkSel), quantity: String(likesQtyForStatus) });
                      try { console.log('🚀 sending_fama24h_likes', { service: 666, link: likesLinkSel, quantity: likesQtyForStatus }); } catch(_) {}
                      try {
                        const respLikes = await axios.post('https://fama24h.net/api/v2', payloadLikes.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                        const dataLikes = respLikes.data || {};
                        const orderIdLikes = dataLikes.order || dataLikes.id || null;
                        await col.updateOne(filter, { $set: { 'fama24h_likes.orderId': orderIdLikes, 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 666, link: likesLinkSel, quantity: likesQtyForStatus }, 'fama24h_likes.response': dataLikes } });
                      } catch (e3) {
                        try { console.error('❌ fama24h_likes_error', e3?.response?.data || e3?.message || String(e3), { link: likesLinkSel, quantity: likesQtyForStatus }); } catch(_) {}
                        await col.updateOne(filter, { $set: { 'fama24h_likes.error': e3?.response?.data || e3?.message || String(e3), 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 666, link: likesLinkSel, quantity: likesQtyForStatus } } });
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
        const map = { 50: 50, 150: 150, 500: 200, 1200: 800, 3000: 1000, 5000: 2500, 10000: 5000 };
        upgradeAdd = map[qtdBase] || 0;
      }
    }
    const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
    const isOrganicos = /organicos/i.test(tipo);
    if (!isOrganicos) {
      const key = process.env.FAMA24H_API_KEY || '';
      const serviceId = (/^mistos$/i.test(tipo)) ? 659 : (/^brasileiros$/i.test(tipo)) ? 23 : null;
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
    if (!username) return res.status(400).json({ ok: false, error: 'missing_username' });
    const vu = await getCollection('validated_insta_users');
    const doc = { username, ip: req.realIP || req.ip || null, userAgent: req.get('User-Agent') || '', source: 'api.validet.track', lastTrackAt: new Date().toISOString() };
    await vu.updateOne({ username }, { $setOnInsert: { username, firstSeenAt: new Date().toISOString() }, $set: doc }, { upsert: true });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
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
    const likesLink = sanitize(map['orderbump_post_likes']);
    const key = process.env.FAMA24H_API_KEY || '';
    const results = { views: null, likes: null };
    if (key && viewsQty > 0 && viewsLink) {
      const axios = require('axios');
      const payload = new URLSearchParams({ key, action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
      try {
        const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
        const data = resp.data || {};
        const orderIdViews = data.order || data.id || null;
        await col.updateOne(filter, { $set: { fama24h_views: { orderId: orderIdViews, status: orderIdViews ? 'created' : 'unknown', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, response: data, requestedAt: new Date().toISOString() } } });
        results.views = { orderId: orderIdViews, data };
      } catch (e) {
        await col.updateOne(filter, { $set: { fama24h_views: { error: e?.response?.data || e?.message || String(e), requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
        results.views = { error: e?.message || String(e) };
      }
    }
    if (key && likesQty > 0 && likesLink) {
      const alreadyLikes = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
      if (!alreadyLikes) {
        const lockUpdate = await col.updateOne(
          { ...filter, 'fama24h_likes.status': { $nin: ['processing', 'created'] }, 'fama24h_likes.orderId': { $exists: false } },
          { $set: { fama24h_likes: { status: 'processing', requestedAt: new Date().toISOString() } } }
        );
        if (lockUpdate.modifiedCount > 0) {
          const axios = require('axios');
          const payload = new URLSearchParams({ key, action: 'add', service: '666', link: String(likesLink), quantity: String(likesQty) });
          try {
            const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
            const data = resp.data || {};
            const orderIdLikes = data.order || data.id || null;
            await col.updateOne(filter, { $set: { 'fama24h_likes.orderId': orderIdLikes, 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 666, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data } });
            results.likes = { orderId: orderIdLikes, data };
          } catch (e) {
            await col.updateOne(filter, { $set: { 'fama24h_likes.error': e?.response?.data || e?.message || String(e), 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 666, link: likesLink, quantity: likesQty } } });
            results.likes = { error: e?.message || String(e) };
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
      const likesLink = sanitize(map['orderbump_post_likes']);
      const filter = { _id: record._id };
      const resultItem = { id: String(record._id), views: null, likes: null };
      if (key && viewsQty > 0 && viewsLink) {
        const currentViewsLink = String(record?.fama24h_views?.requestPayload?.link || '');
        const isInvalid = !/^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(currentViewsLink);
        if (isInvalid) {
          const axios = require('axios');
          const payload = new URLSearchParams({ key, action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
          try {
            const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
            const data = resp.data || {};
            const orderIdViews = data.order || data.id || null;
            await col.updateOne(filter, { $set: { fama24h_views: { orderId: orderIdViews, status: orderIdViews ? 'created' : 'unknown', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, response: data, requestedAt: new Date().toISOString() } } });
            resultItem.views = { orderId: orderIdViews };
          } catch (e) {
            await col.updateOne(filter, { $set: { fama24h_views: { error: e?.response?.data || e?.message || String(e), requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
            resultItem.views = { error: e?.message || String(e) };
          }
        }
      }
      if (key && likesQty > 0 && likesLink) {
        const currentLikesLink = String(record?.fama24h_likes?.requestPayload?.link || '');
        const isInvalidLikes = !/^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(currentLikesLink);
        const isProcessing = record?.fama24h_likes?.status === 'processing';
        if (isInvalidLikes && !isProcessing) {
          const lockUpdate = await col.updateOne(
             { ...filter, 'fama24h_likes.status': { $ne: 'processing' } },
             { $set: { 'fama24h_likes.status': 'processing', 'fama24h_likes.requestedAt': new Date().toISOString() } }
          );
          if (lockUpdate.modifiedCount > 0) {
              const axios = require('axios');
              const payload = new URLSearchParams({ key, action: 'add', service: '666', link: String(likesLink), quantity: String(likesQty) });
              try {
                const resp = await axios.post('https://fama24h.net/api/v2', payload.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const data = resp.data || {};
                const orderIdLikes = data.order || data.id || null;
                await col.updateOne(filter, { $set: { 'fama24h_likes.orderId': orderIdLikes, 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 666, link: likesLink, quantity: likesQty }, 'fama24h_likes.response': data } });
                resultItem.likes = { orderId: orderIdLikes };
              } catch (e) {
                await col.updateOne(filter, { $set: { 'fama24h_likes.error': e?.response?.data || e?.message || String(e), 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 666, link: likesLink, quantity: likesQty } } });
                resultItem.likes = { error: e?.message || String(e) };
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
    if (id) conds.push({ 'woovi.chargeId': id });
    if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
    if (correlationID) conds.push({ correlationID });
    const filter = conds.length ? { $or: conds } : {};
    const doc = await col.findOne(filter, { projection: { status: 1, woovi: 1 } });
    const paid = !!doc && (String(doc.status).toLowerCase() === 'pago' || String(doc.woovi?.status || '').toLowerCase() === 'pago');
    return res.json({ ok: true, paid, order: doc || null });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
app.get('/pedido', async (req, res) => {
  try {
    const identifier = String(req.query.identifier || '').trim();
    const correlationID = String(req.query.correlationID || '').trim();
    const orderIDRaw = String(req.query.orderID || req.query.orderid || '').trim();
    const phoneRaw = String(req.query.phone || '').trim();
    const hasQuery = !!(identifier || correlationID || orderIDRaw || phoneRaw);
    const hasSessionCtx = !!(req.session && (req.session.selectedOrderID || req.session.lastPaidIdentifier || req.session.lastPaidCorrelationID));
    if (!hasQuery && !hasSessionCtx) {
      return res.redirect('/cliente');
    }
    const col = await getCollection('checkout_orders');
    let doc = null;
    // 1) Priorizar pedido selecionado explicitamente em sessão
    if (req.session && req.session.selectedOrderID) {
      const soid = req.session.selectedOrderID;
      const { ObjectId } = require('mongodb');
      const orConds = [ { 'fama24h.orderId': soid }, { 'fornecedor_social.orderId': soid } ];
      if (typeof soid === 'string' && /^[0-9a-fA-F]{24}$/.test(soid)) {
        try { orConds.push({ _id: new ObjectId(soid) }); } catch(_) {}
      }
      doc = await col.findOne({ $or: orConds });
    }
    // 2) Em seguida, tentar pelos parâmetros de consulta
    if (!doc) {
      const conds = [];
      if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
      if (correlationID) conds.push({ correlationID });
      if (orderIDRaw) {
        const maybeNum = Number(orderIDRaw);
        if (!Number.isNaN(maybeNum)) conds.push({ 'fama24h.orderId': maybeNum });
        if (!Number.isNaN(maybeNum)) conds.push({ 'fornecedor_social.orderId': maybeNum });
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
      if (conds.length) {
        doc = await col.findOne({ $or: conds });
      }
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
    return res.render('pedido', { order, PIXEL_ID: process.env.PIXEL_ID || '' });
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
    const username = usernameParam || usernameSession || '';
    if (!username) return res.status(400).json({ success: false, error: 'missing_username' });
    const debugInsert = String(req.query.debug || '').trim() === '1';
    let debugInfo = null;
    try {
      const vu = await getCollection('validated_insta_users');
      const doc = { source: 'api.instagram.posts', lastPostsAt: new Date().toISOString() };
      // Remove username from $setOnInsert to avoid conflict if it's already in the filter
      await vu.updateOne({ username }, { $setOnInsert: { firstSeenAt: new Date().toISOString() }, $set: doc }, { upsert: true });
      debugInfo = { ok: true, username };
      try { console.log('🗃️ Posts route: upsert ok', debugInfo); } catch(_) {}
    } catch (err) { debugInfo = { ok: false, error: err?.message || String(err) }; try { console.error('❌ Posts route: upsert error', err?.message || String(err)); } catch(_) {} }
    try {
      console.log('[API] tentando web_profile_info com cookies');
      const result = await fetchInstagramRecentPosts(username);
      if (result && result.success && Array.isArray(result.posts) && result.posts.length) {
        if (debugInsert) return res.json(Object.assign({}, result, { debugInsert: debugInfo }));
        return res.json(result);
      }
    } catch (e) { /* fallback abaixo */ }
    try {
      console.log('[API] tentando web_profile_info sem cookies');
      const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "X-IG-App-ID": "936619743392459",
        "Accept": "application/json",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "X-Requested-With": "XMLHttpRequest"
      };
      const resp = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`, { headers, timeout: 8000 });
      const user = resp.data && resp.data.data && resp.data.data.user;
      if (resp.status === 200 && user && !user.is_private) {
        const edges = (user.edge_owner_to_timeline_media && Array.isArray(user.edge_owner_to_timeline_media.edges)) ? user.edge_owner_to_timeline_media.edges : [];
        const posts = edges.map(e => e && e.node ? ({
          shortcode: e.node.shortcode,
          takenAt: e.node.taken_at_timestamp,
          isVideo: !!e.node.is_video,
          displayUrl: e.node.display_url || e.node.thumbnail_src || null,
          videoUrl: e.node.video_url || null,
          typename: e.node.__typename || ''
        }) : null).filter(Boolean).sort((a,b)=> Number(b.takenAt||0) - Number(a.takenAt||0)).slice(0, 8);
        if (posts.length) return res.json({ success: true, username: user.username, posts, debugInsert: debugInsert ? debugInfo : undefined });
      }
    } catch (e3) { /* fallback abaixo */ }
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
        const isFollowers = /(mistos|brasileiros|organicos|seguidores_tiktok)/i.test(tipo);
        let upgradeAdd = 0;
        if (isFollowers && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
          if ((/brasileiros/i.test(tipo) || /organicos/i.test(tipo)) && qtdBase === 1000) upgradeAdd = 1000; else {
            const map = { 50: 50, 150: 150, 500: 200, 1200: 800, 3000: 1000, 5000: 2500, 10000: 5000 };
            upgradeAdd = map[qtdBase] || 0;
          }
        }
        const qtd = Math.max(0, Number(qtdBase) + Number(upgradeAdd));
        if (/organicos/i.test(tipo) && !alreadySentFS) {
          const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
          const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
          if (!!keyFS && !!instaUser && qtd > 0) {
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
              const expMsX = modeX === 'life' ? (nowMsX + 3650 * 24 * 60 * 60 * 1000) : (nowMsX + 30 * 24 * 60 * 60 * 1000);
              await tl.updateOne({ id: linkIdX }, { $set: { expiresAt: new Date(expMsX).toISOString() } });
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
          const sanitizeLink = (s) => { const v = String(s || '').replace(/[`\s]/g, '').trim(); const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v); return ok ? (v.endsWith('/') ? v : (v + '/')) : ''; };
          const selectedFor = (req.session && req.session.selectedFor) ? req.session.selectedFor : {};
          const selViews = selectedFor && selectedFor.views && selectedFor.views.link ? String(selectedFor.views.link) : '';
          const selLikes = selectedFor && selectedFor.likes && selectedFor.likes.link ? String(selectedFor.likes.link) : '';
          const viewsLinkRaw = mapPaid['orderbump_post_views'] || selViews || (record?.additionalInfoPaid || []).find(it => it && it.key === 'orderbump_post_views')?.value || (record?.additionalInfo || []).find(it => it && it.key === 'orderbump_post_views')?.value || '';
          const likesLinkRaw = mapPaid['orderbump_post_likes'] || selLikes || (record?.additionalInfoPaid || []).find(it => it && it.key === 'orderbump_post_likes')?.value || (record?.additionalInfo || []).find(it => it && it.key === 'orderbump_post_likes')?.value || '';
          const viewsLink = sanitizeLink(viewsLinkRaw);
          const likesLinkSel = sanitizeLink(likesLinkRaw);
          try { console.log('🔎 [mark-paid] orderbump_raw', { identifier, correlationID, viewsLinkRaw, likesLinkRaw, selectedFor, viewsQty, likesQty }); } catch(_) {}
          try { console.log('🔎 [mark-paid] orderbump_sanitized', { viewsLink, likesLinkSel }); } catch(_) {}
          if ((process.env.FAMA24H_API_KEY || '') && viewsQty > 0) {
            if (!viewsLink) {
              try { console.warn('⚠️ [mark-paid] views_link_invalid', { viewsLinkRaw }); } catch(_) {}
              await col.updateOne(filter, { $set: { fama24h_views: { error: 'invalid_link', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
            } else {
              const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
              try { console.log('🚀 [mark-paid] sending_fama24h_views', { service: 250, link: viewsLink, quantity: viewsQty }); } catch(_) {}
              try {
                const respV = await axios.post('https://fama24h.net/api/v2', payloadViews.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const dataV = respV.data || {};
                const orderIdV = dataV.order || dataV.id || null;
                await col.updateOne(filter, { $set: { fama24h_views: { orderId: orderIdV, status: orderIdV ? 'created' : 'unknown', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, response: dataV, requestedAt: new Date().toISOString() } } });
              } catch (e2) {
                try { console.error('❌ [mark-paid] fama24h_views_error', e2?.response?.data || e2?.message || String(e2)); } catch(_) {}
                await col.updateOne(filter, { $set: { fama24h_views: { error: e2?.response?.data || e2?.message || String(e2), requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
              }
            }
          }
          const alreadyLikes3 = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
          if ((process.env.FAMA24H_API_KEY || '') && likesQty > 0 && !alreadyLikes3) {
            if (!likesLinkSel) {
              try { console.warn('⚠️ [mark-paid] likes_link_invalid', { likesLinkRaw }); } catch(_) {}
              await col.updateOne(filter, { $set: { fama24h_likes: { error: 'invalid_link', requestPayload: { service: 666, link: likesLinkSel, quantity: likesQty }, requestedAt: new Date().toISOString() } } });
            } else {
               const lockUpdate = await col.updateOne(
                  { ...filter, 'fama24h_likes.status': { $exists: false } },
                  { $set: { fama24h_likes: { status: 'processing', requestedAt: new Date().toISOString() } } }
               );
               if (lockUpdate.modifiedCount > 0) {
                  const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '666', link: String(likesLinkSel), quantity: String(likesQty) });
                  try { console.log('🚀 [mark-paid] sending_fama24h_likes', { service: 666, link: likesLinkSel, quantity: likesQty }); } catch(_) {}
                  try {
                    const respL = await axios.post('https://fama24h.net/api/v2', payloadLikes.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                    const dataL = respL.data || {};
                    const orderIdL = dataL.order || dataL.id || null;
                    await col.updateOne(filter, { $set: { 'fama24h_likes.orderId': orderIdL, 'fama24h_likes.status': orderIdL ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 666, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.response': dataL } });
                  } catch (e3) {
                    try { console.error('❌ [mark-paid] fama24h_likes_error', e3?.response?.data || e3?.message || String(e3)); } catch(_) {}
                    await col.updateOne(filter, { $set: { 'fama24h_likes.error': e3?.response?.data || e3?.message || String(e3), 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 666, link: likesLinkSel, quantity: likesQty } } });
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
    const identifier = String(req.query.identifier || '').trim();
    const correlationID = String(req.query.correlationID || '').trim();
    const orderIDRaw = String(req.query.orderID || req.query.orderid || '').trim();
    const phoneRaw = String(req.query.phone || '').trim();
    const col = await getCollection('checkout_orders');
    let doc = null;
    if (id) {
      try { doc = await col.findOne({ _id: new (require('mongodb').ObjectId)(id) }); } catch(_) {}
    }
    if (!doc && req.session && req.session.selectedOrderID) {
      const soid = req.session.selectedOrderID;
      doc = await col.findOne({ $or: [ { 'fama24h.orderId': soid }, { 'fornecedor_social.orderId': soid } ] });
    }
    if (!doc) {
      const conds = [];
      if (identifier) { conds.push({ 'woovi.identifier': identifier }); conds.push({ identifier }); }
      if (correlationID) conds.push({ correlationID });
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
app.get('/painel', async (req, res) => {
  try {
    const { getCollection } = require('./mongodbClient');
    const col = await getCollection('checkout_orders');

    // Filter by paid status
    const query = {
      $or: [
        { status: 'pago' },
        { 'woovi.status': 'pago' }
      ]
    };

    const orders = await col.find(query).sort({ createdAt: -1 }).toArray();

    // Timezone correction (-3 hours) helper
    // Returns a Date object shifted by -3 hours so that UTC methods return SP components
    const toSP = (d) => new Date(d.getTime() - 3 * 60 * 60 * 1000);

    // Filter logic
    const period = req.query.period || 'today';
    const nowUTC = new Date();
    const nowSP = toSP(nowUTC);
    
    // Start of today in SP (00:00 SP time)
    const startOfTodaySP = new Date(Date.UTC(nowSP.getUTCFullYear(), nowSP.getUTCMonth(), nowSP.getUTCDate(), 0, 0, 0, 0));

    let filteredOrders = orders.filter(o => {
      const dateStr = o.createdAt || o.woovi?.paidAt || o.paidAt;
      if (!dateStr) return false;
      
      const orderDateUTC = new Date(dateStr);
      const orderDateSP = toSP(orderDateUTC);

      if (period === 'today') {
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

    // Cost calculation
    let totalCost = 0;
    let totalRevenue = 0;
    const report = filteredOrders.map(o => {
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
         const tItem = Array.isArray(o.additionalInfoPaid) ? o.additionalInfoPaid.find(i => i.key === 'tipo_servico') : null;
         if (tItem) type = tItem.value;
      } else if (!type && o.additionalInfoMap && o.additionalInfoMap.tipo_servico) {
          type = o.additionalInfoMap.tipo_servico;
      }
      type = String(type || '').toLowerCase();

      // Determine cost per 1000
      let costPer1000 = 0;
      // Per user rules:
       // mistos=5.40, brasileiros=15.48, organicos=35, curtidas=2, comentarios=0.3, visualizacao=0.01
       if (type.includes('mistos')) costPer1000 = 5.40;
       else if (type.includes('brasileiros') && !type.includes('curtidas') && !type.includes('comentarios') && !type.includes('visualiza')) costPer1000 = 15.48;
       else if (type.includes('organicos')) costPer1000 = 35.0;
      else if (type.includes('curtidas')) costPer1000 = 2.0;
      else if (type.includes('comentarios')) costPer1000 = 0.3;
      else if (type.includes('visualiza')) costPer1000 = 0.01;
      else if (type.includes('views')) costPer1000 = 0.01; // Alias

      const serviceCost = (qty / 1000) * costPer1000;
      const totalItemCost = 0.85 + serviceCost;
      totalCost += totalItemCost;

      // Revenue calculation
      let revenue = 0;
      if (o.valueCents) {
          revenue = Number(o.valueCents) / 100;
      } else if (o.woovi && o.woovi.paymentMethods && o.woovi.paymentMethods.pix && o.woovi.paymentMethods.pix.value) {
          revenue = Number(o.woovi.paymentMethods.pix.value) / 100;
      }
      totalRevenue += revenue;

      return {
        _id: o._id,
        createdAt: o.createdAt,
        type,
        qty,
        costPer1000,
        cost: totalItemCost,
        revenue
      };
    });

    res.render('painel', { orders: report, totalCost, totalRevenue, period, totalTransactions: filteredOrders.length });
  } catch (e) {
    res.status(500).send(e.toString());
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
      const serviceId = (/^mistos$/i.test(resolvedTipo)) ? 659 : (/^brasileiros$/i.test(resolvedTipo)) ? 23 : null;
      
      // Ajuste: O provedor (Fama24h - serviço 659) exige mínimo de 100.
      // Se o pedido for de 50 (teste), enviamos 100 para garantir o processamento.
      let finalQtdFama = resolvedQtd;
      if (serviceId === 659 && finalQtdFama > 0 && finalQtdFama < 100) {
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
      const bumpsStr0 = additionalInfoMap['order_bumps'] || (record?.additionalInfoPaid || []).find(it => it && it.key === 'order_bumps')?.value || (record?.additionalInfo || []).find(it => it && it.key === 'order_bumps')?.value || '';
      let upgradeAdd = 0;
      if (isFollowers && /(^|;)upgrade:\d+/i.test(String(bumpsStr0))) {
        if ((/brasileiros/i.test(resolvedTipo) || /organicos/i.test(resolvedTipo)) && Number(resolvedQtd) === 1000) upgradeAdd = 1000; else {
          const map = { 50: 50, 150: 150, 500: 200, 1200: 800, 3000: 1000, 5000: 2500, 10000: 5000 };
          upgradeAdd = map[Number(resolvedQtd)] || 0;
        }
      }
      const finalQtd = Math.max(0, Number(resolvedQtd) + Number(upgradeAdd));
      const alreadySentFS = !!(record && record.fornecedor_social && record.fornecedor_social.orderId);
      if (/organicos/i.test(resolvedTipo) && !!resolvedUser && finalQtd > 0 && !alreadySentFS) {
        const keyFS = process.env.FORNECEDOR_SOCIAL_API_KEY || '';
        const serviceFS = Number(process.env.FORNECEDOR_SOCIAL_SERVICE_ID_ORGANICOS || 312);
        if (!!keyFS) {
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
        const sanitizeLink = (s) => { const v = String(s || '').replace(/[`\s]/g, '').trim(); const ok = /^https?:\/\/(www\.)?instagram\.com\/(p|reel)\/[A-Za-z0-9_-]+\/?$/i.test(v); return ok ? (v.endsWith('/') ? v : (v + '/')) : ''; };
        const mapPaid3 = record?.additionalInfoMapPaid || {};
        const viewsLink = sanitizeLink(mapPaid3['orderbump_post_views'] || (arrPaid2.find(it => it && it.key === 'orderbump_post_views')?.value) || (arrOrig2.find(it => it && it.key === 'orderbump_post_views')?.value) || '');
        const likesLinkSel = sanitizeLink(mapPaid3['orderbump_post_likes'] || (arrPaid2.find(it => it && it.key === 'orderbump_post_likes')?.value) || (arrOrig2.find(it => it && it.key === 'orderbump_post_likes')?.value) || '');
        if (viewsQty > 0 && (process.env.FAMA24H_API_KEY || '') && viewsLink) {
          const axios = require('axios');
          const payloadViews = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '250', link: String(viewsLink), quantity: String(viewsQty) });
          try {
            const respViews = await axios.post('https://fama24h.net/api/v2', payloadViews.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
            const dataViews = respViews.data || {};
            const orderIdViews = dataViews.order || dataViews.id || null;
            await col.updateOne(filter, { $set: { fama24h_views: { orderId: orderIdViews, status: orderIdViews ? 'created' : 'unknown', requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, response: dataViews, requestedAt: new Date().toISOString() } } });
          } catch (e2) {
            await col.updateOne(filter, { $set: { fama24h_views: { error: e2?.response?.data || e2?.message || String(e2), requestPayload: { service: 250, link: viewsLink, quantity: viewsQty }, requestedAt: new Date().toISOString() } } });
          }
        }
        const alreadyLikes4 = !!(record && record.fama24h_likes && (record.fama24h_likes.orderId || record.fama24h_likes.status === 'processing' || record.fama24h_likes.status === 'created'));
        if (likesQty > 0 && (process.env.FAMA24H_API_KEY || '') && likesLinkSel && !alreadyLikes4) {
          const lockUpdate = await col.updateOne(
            { ...filter, 'fama24h_likes.status': { $exists: false } },
            { $set: { fama24h_likes: { status: 'processing', requestedAt: new Date().toISOString() } } }
          );
          if (lockUpdate.modifiedCount > 0) {
              const axios = require('axios');
              const payloadLikes = new URLSearchParams({ key: String(process.env.FAMA24H_API_KEY), action: 'add', service: '666', link: String(likesLinkSel), quantity: String(likesQty) });
              try {
                const respLikes = await axios.post('https://fama24h.net/api/v2', payloadLikes.toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
                const dataLikes = respLikes.data || {};
                const orderIdLikes = dataLikes.order || dataLikes.id || null;
                await col.updateOne(filter, { $set: { 'fama24h_likes.orderId': orderIdLikes, 'fama24h_likes.status': orderIdLikes ? 'created' : 'unknown', 'fama24h_likes.requestPayload': { service: 666, link: likesLinkSel, quantity: likesQty }, 'fama24h_likes.response': dataLikes } });
              } catch (e3) {
                await col.updateOne(filter, { $set: { 'fama24h_likes.error': e3?.response?.data || e3?.message || String(e3), 'fama24h_likes.status': 'error', 'fama24h_likes.requestPayload': { service: 666, link: likesLinkSel, quantity: likesQty } } });
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
      await processOrderFulfillment(record, col, req);
    } catch (e) {
      console.error('Error processing fulfillment in payment/confirm:', e);
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
  const REQUEST_TIMEOUT = 5000;

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
      const proxyAgent = profile.proxy ? new HttpsProxyAgent(`http://${profile.proxy.auth.username}:${profile.proxy.auth.password}@${profile.proxy.host}:${profile.proxy.port}`, { rejectUnauthorized: false }) : null;
      
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
      console.log('Todas as tentativas autenticadas falharam, tentando fallback anônimo.');
    }
  }

  // Fallback: Sem cookies ou todos falharam
  try {
    console.log('[IG] Tentando fallback anônimo...');
    const headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
      "X-IG-App-ID": "936619743392459",
      "Accept": "application/json",
      "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
      "Cache-Control": "no-cache",
      "Pragma": "no-cache",
      "X-Requested-With": "XMLHttpRequest"
    };
    const resp = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`, { headers, timeout: REQUEST_TIMEOUT });
    if (resp.status === 200 && resp.data && resp.data.data && resp.data.data.user && !resp.data.data.user.is_private) {
      const user = resp.data.data.user;
      const edges = (user.edge_owner_to_timeline_media && Array.isArray(user.edge_owner_to_timeline_media.edges)) ? user.edge_owner_to_timeline_media.edges : [];
      const posts = edges.map(e => e && e.node ? ({
        shortcode: e.node.shortcode,
        takenAt: e.node.taken_at_timestamp,
        isVideo: !!e.node.is_video,
        displayUrl: e.node.display_url || e.node.thumbnail_src || null,
        videoUrl: e.node.video_url || null,
        typename: e.node.__typename || ''
      }) : null).filter(Boolean).sort((a,b)=> Number(b.takenAt||0) - Number(a.takenAt||0)).slice(0, 8);
      return { success: true, username: user.username, posts };
    }
  } catch (_) {}
  
  throw new Error('Falha ao buscar posts (timeout ou erro)');
}

