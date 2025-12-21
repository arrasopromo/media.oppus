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

let globalIndex = 0; // Variável global para round-robin

const instagramQueue = new PQueue({ concurrency: 3 }); // Concorrência 1 para evitar problemas com cookies

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

// Função para buscar posts do Instagram e extrair IDs
async function fetchInstagramPosts(username) {
    try {
        console.log(`🔍 Buscando posts do Instagram para: @${username}`);
        
        const url = `https://www.instagram.com/${username}/embed`;
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Upgrade-Insecure-Requests': '1'
            },
            timeout: 15000
        });

        if (response.status !== 200) {
            console.log(`❌ Erro ao buscar posts: Status ${response.status}`);
            return { success: false, error: 'Erro ao acessar perfil do Instagram' };
        }

        const htmlContent = response.data;
        console.log(`📄 HTML recebido (${htmlContent.length} caracteres)`);

        // Extrair IDs dos posts usando regex
        const regex = /\\\"shortcode\\\"\s*:\s*\\\"([CD][^\\\"]+)\\\"/g;
        const matches = [];
        let match;

        while ((match = regex.exec(htmlContent)) !== null) {
            matches.push(match[1]);
        }

        console.log(`📊 IDs de posts encontrados: ${matches.length}`);
        if (matches.length > 0) {
            console.log(`📋 Primeiros 5 IDs: ${matches.slice(0, 5).join(', ')}`);
        }

        return {
            success: true,
            posts: matches,
            totalPosts: matches.length
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

// Função principal para verificar perfil do Instagram
async function verifyInstagramProfile(username, userAgent, ip, req, res) {
    console.log(`🔍 Iniciando verificação do perfil: @${username}`);
    console.log(`📊 Total de perfis disponíveis: ${cookieProfiles.length}`);
    
    let selectedProfile = null;
    let attempts = 0;
    const MAX_ATTEMPTS = cookieProfiles.length * 2; // Definir MAX_ATTEMPTS aqui
    const USAGE_INTERVAL_MS = 30 * 1000; // 30 segundos de intervalo entre usos do mesmo perfil
    const MAX_ERRORS_PER_PROFILE = 5; // Número máximo de erros antes de desativar o perfil
    const DISABLE_TIME_MS = 1 * 60 * 1000; // Tempo de desativação do perfil (1 minuto)
    
    while (attempts < MAX_ATTEMPTS) {
        const now = Date.now();
        const availableProfiles = cookieProfiles
            .filter(profile => {
                // Não desativado e não bloqueado
                return profile.disabledUntil <= now && !isCookieLocked(profile.ds_user_id);
            })
            .sort((a, b) => {
                // Priorizar cookies com menos erros
                if (a.errorCount !== b.errorCount) {
                    return a.errorCount - b.errorCount;
                }
                // Priorizar cookies com último uso mais antigo
                return a.lastUsed - b.lastUsed;
            });

        console.log(`📋 Perfis disponíveis: ${availableProfiles.length}/${cookieProfiles.length}`);

        if (availableProfiles.length === 0) {
            console.warn("⚠️ Nenhum perfil de cookie disponível no momento. Todos estão bloqueados, desativados ou em cooldown.");
            throw new Error("Não foi possível verificar o perfil no momento. Aguarde 10 segundos e tente novamente.");
        }

        // Selecionar o próximo perfil usando round-robin global entre os disponíveis
        const nextProfileIndex = globalIndex % availableProfiles.length;
        selectedProfile = availableProfiles[nextProfileIndex];

        // Atualizar o índice global para a próxima requisição
        globalIndex = (globalIndex + 1) % availableProfiles.length;

        // Verificar intervalo de uso (já filtrado, mas para garantir)
        if (selectedProfile.lastUsed && (now - selectedProfile.lastUsed < USAGE_INTERVAL_MS)) {
            console.log(`⏳ Perfil ${selectedProfile.ds_user_id} usado recentemente. Pulando.`);
            attempts++;
            continue; // Este continue está dentro do while
        }

        // Tentar bloquear o cookie
        if (isCookieLocked(selectedProfile.ds_user_id)) {
            console.log(`🔒 Perfil ${selectedProfile.ds_user_id} está bloqueado. Pulando.`);
            attempts++;
            continue; // Este continue está dentro do while
        }

        console.log(`🔄 Tentando perfil ${selectedProfile.ds_user_id}. Tentativa ${attempts + 1}/${MAX_ATTEMPTS}`);

        try {
            const proxyAgent = selectedProfile.proxy ? new HttpsProxyAgent(`http://${selectedProfile.proxy.auth.username}:${selectedProfile.proxy.auth.password}@${selectedProfile.proxy.host}:${selectedProfile.proxy.port}`, { rejectUnauthorized: false }) : null;

            const headers = {
                "User-Agent": selectedProfile.userAgent,
                "X-IG-App-ID": "936619743392459",
                "Cookie": `sessionid=${selectedProfile.sessionid}; ds_user_id=${selectedProfile.ds_user_id}`,
                "Accept": "application/json",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Ch-Ua": `"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`,
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": `"Windows"`,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "X-Asbd-Id": "129477",
                "X-Csrftoken": "missing",
                "X-Ig-Www-Claim": "0",
                "X-Instagram-Ajax": "1010394699",
                "X-Requested-With": "XMLHttpRequest"
            };

            console.log(`🔍 Fazendo requisição para: @${username}`);

            const response = await axios.get(`https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(username)}`, {
                headers: headers,
                httpsAgent: proxyAgent,
                timeout: 15000 // 15 segundos de timeout
            });

            console.log(`📡 Resposta recebida: ${response.status}`);

            if (response.status < 200 || response.status >= 300) {
                if (response.status === 404) {
                    unlockCookie(selectedProfile.ds_user_id); // Liberar cookie em caso de erro 404
                    return { success: false, status: 404, error: "Perfil não localizado, nome de usuário pode estar incorreto." };
                } else if (response.status === 429 || response.status === 401 || response.status === 403) {
                    console.warn(`⚠️ Erro ${response.status} para o perfil ${selectedProfile.ds_user_id}. Incrementando contador de erros.`);
                    selectedProfile.errorCount++;
                    if (selectedProfile.errorCount >= MAX_ERRORS_PER_PROFILE) {
                        selectedProfile.disabledUntil = now + DISABLE_TIME_MS;
                        console.warn(`🚫 Perfil ${selectedProfile.ds_user_id} desativado temporariamente por ${DISABLE_TIME_MS / 60000} minutos devido a múltiplos erros.`);
                    }
                    unlockCookie(selectedProfile.ds_user_id); // Liberar cookie
                    attempts++;
                    continue; // Tentar o próximo perfil
                }

                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = response.data;

            if (!data.data || !data.data.user) {
                unlockCookie(selectedProfile.ds_user_id); // Liberar cookie
                return { success: false, status: 404, error: "Perfil não encontrado ou privado. Verifique se o nome de usuário está correto e se o perfil é público." };
            }

            const user = data.data.user;
            console.log(`✅ Perfil encontrado: @${user.username} (Privado: ${user.is_private})`);

            if (user.is_private) {
                unlockCookie(selectedProfile.ds_user_id); // Liberar cookie
                const originalImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
                let driveImageUrl = null;

                if (user.profile_pic_url_hd || user.profile_pic_url) {
                    try {
                        if (driveManager.isReady()) {
                            try {
                                // Tentar fazer upload para Google Drive se disponível
                                // Por enquanto, usar apenas a URL original do Instagram
                                console.log(`📸 Usando URL original do Instagram: ${originalImageUrl}`);
                            } catch (driveError) {
                                console.warn("Erro ao processar imagem:", driveError.message);
                            }
                        }
                    } catch (imageError) {
                        console.warn("Erro ao processar imagem:", imageError.message);
                    }
                }

                return { 
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
            }

            // Sucesso: Atualizar lastUsed e resetar errorCount
            selectedProfile.lastUsed = now;
            selectedProfile.errorCount = 0;
            unlockCookie(selectedProfile.ds_user_id); // Liberar cookie

            // Processar imagem do perfil
            let driveImageUrl = null;

            const originalImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
            if (originalImageUrl) {
                try {
                    // Sempre tentar baixar e servir localmente para evitar bloqueios externos
                    const localImageUrl = await downloadAndServeImage(originalImageUrl, user.username, proxyAgent);
                    driveImageUrl = localImageUrl || originalImageUrl;

                    // Upload opcional ao Google Drive (se configurado), sem afetar a URL usada
                    if (driveManager.isReady()) {
                        try {
                            const imageResponse = await axios.get(originalImageUrl, {
                                responseType: 'arraybuffer',
                                timeout: 10000
                            });
                            const fileName = `${user.username}_profile_${Date.now()}.jpg`;
                            await driveManager.uploadBuffer(
                                imageResponse.data,
                                fileName,
                                'image/jpeg',
                                driveManager.profileImagesFolderId
                            );
                        } catch (driveErr) {
                            console.warn('Falha ao enviar imagem ao Google Drive:', driveErr.message);
                        }
                    }
                } catch (error) {
                    // Em caso de erro ao baixar, usar URL original do Instagram
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
                cookieUsed: selectedProfile.ds_user_id
            };

            console.log(`✅ Perfil verificado com sucesso: @${user.username} (Cookie ID: ${selectedProfile.ds_user_id})`);

            try {
                const linkId = req.session.linkSlug || req.query.id || req.body.id;
                if (linkId) {
                    const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
                    if (result.success) {
                        const row = result.rows.find(r => r[CONTROLE_FIELDS.LINK] === linkId);
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
                            // Fallback: criar nova linha (não deve acontecer normalmente)
                            console.warn(`⚠️ Nenhuma linha encontrada para link=${linkId}. Criando nova linha como fallback.`);
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
                    } else {
                        console.error('❌ Erro ao buscar linhas do Baserow:', result.error);
                    }
                } else {
                    console.warn('⚠️ Nenhum linkId encontrado na sessão ou request. Não foi possível atualizar o Baserow.');
                }
            } catch (baserowError) {
                console.error('❌ Erro ao atualizar/salvar no Baserow:', baserowError);
            }

            // Verificar se o perfil já foi testado anteriormente
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
            
            return { success: true, status: 200, profile: responseProfile };

        } catch (error) {
            console.error(`❌ Erro na requisição com perfil ${selectedProfile.ds_user_id}:`, error.message);
            selectedProfile.errorCount++;
            if (selectedProfile.errorCount >= MAX_ERRORS_PER_PROFILE) {
                selectedProfile.disabledUntil = now + DISABLE_TIME_MS;
                console.warn(`🚫 Perfil ${selectedProfile.ds_user_id} desativado temporariamente por ${DISABLE_TIME_MS / 60000} minutos devido a múltiplos erros.`);
            }
            unlockCookie(selectedProfile.ds_user_id); // Liberar cookie em caso de erro
            attempts++;
            if (attempts === MAX_ATTEMPTS) {
                // Se todas as tentativas falharem
                try {
                    console.log("📊 Tentando registrar erro no Baserow...");
                    const registerResult = await registerUserInControle(userAgent, ip, username, "error");
                    if (registerResult) {
                        console.log("📊 Erro registrado no Baserow com ID:", registerResult.id);
                    } else {
                        console.error("❌ Falha ao registrar erro no Baserow - resultado nulo");
                    }
                } catch (baserowError) {
                    console.error("❌ Erro ao salvar erro no Baserow:", baserowError);
                }
                throw new Error("Erro ao verificar perfil após múltiplas tentativas. Tente novamente mais tarde.");
            }
        }
    }

    // Se o loop terminar sem sucesso (todos os perfis foram tentados e falharam)
    throw new Error("Não foi possível verificar o perfil com os perfis de cookie disponíveis. Tente novamente mais tarde.");
}

const app = express();
app.set("trust proxy", true); // Confiar em cabeçalhos de proxy
const port = process.env.PORT ? Number(process.env.PORT) : 3000;

// (Removido) Handler ASAP de /checkout antes do view engine
// Motivo: estava tentando renderizar antes de configurar a engine,
// causando respostas vazias (Content-Length: 0). A rota oficial de
// checkout é registrada após a configuração da view engine.

// Inicializar gerenciadores
const linkManager = new LinkManager();
const driveManager = new GoogleDriveManager();
const baserowManager = new BaserowManager("https://baserow.atendimento.info", process.env.BASEROW_TOKEN);
const { getCollection } = require('./mongodbClient');

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
        const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
        
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
        const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
        if (!result.success) {
            console.error("❌ Erro ao buscar linhas:", result.error);
            return false; // Em caso de erro, permitir continuar
        }
        // Verificar se alguma linha tem o mesmo instauser E teste === 'OK'
        const existingUser = result.rows.find(row => {
            const iu = row[CONTROLE_FIELDS.INSTAUSER];
            return (iu && iu.toLowerCase() === instauser.toLowerCase());
        });
        
        if (existingUser) {
            // Verificar se o teste está como 'OK'
            const testeValue = existingUser[CONTROLE_FIELDS.TESTE];
            if (testeValue === 'OK') {
                console.log(`❌ Instauser '${instauser}' já foi usado na linha ${existingUser.id} (teste=OK)`);
                return true;
            }
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
    // Buscar a linha pelo instauser usando o baserowManager
    const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
    if (!result.success) {
      console.error('Erro ao buscar linhas do Baserow:', result.error);
      return;
    }
    
    // Encontrar a linha mais recente pelo instauser (última linha criada)
    const matchingRows = result.rows.filter(r => 
      (r.instauser && r.instauser.toLowerCase() === instauser.toLowerCase())
    );
    
    console.log(`🔍 Encontradas ${matchingRows.length} linhas para instauser: ${instauser}`);
    
    // Pegar a linha mais recente (última criada)
    const row = matchingRows[matchingRows.length - 1];
    
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

// Rota de diagnóstico simples
app.get('/ping', (req, res) => {
  console.log('🏓 Ping recebido');
  res.type('text/plain').send('pong');
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
        'Accept': 'image/*,*/*;q=0.8'
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
    return res.redirect('/');
}

// Log global de requisições para diagnosticar roteamento
app.use((req, res, next) => {
    try {
        console.log('➡️', req.method, req.originalUrl);
    } catch (_) {}
    next();
});

// Home: renderizar Checkout como página inicial
app.get('/', (req, res) => {
    console.log('🏠 Acessando rota / (home -> checkout)');
    res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '' }, (err, html) => {
        if (err) {
            console.error('❌ Erro ao renderizar home/checkout:', err.message);
            return res.status(500).send('Erro ao renderizar checkout');
        }
        res.type('text/html');
        res.send(html);
    });
});

// Página dedicada de Cliente (consulta de pedidos)
app.get('/cliente', (req, res) => {
    console.log('👤 Acessando rota /cliente');
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

// Página de Refil
app.get('/refil', (req, res) => {
    console.log('🔁 Acessando rota /refil');
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
        const response = await axios.post('https://smmrefil.net/api/refill/create', payload, { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }, timeout: 20000 });
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
        additionalInfo
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

    // Criar correlationID no formato xxx-xxx-{phoneDigits}
    const phoneDigitsRaw = (customer && customer.phone) ? String(customer.phone).replace(/\D/g, '') : '';
    const randChunk = () => Math.random().toString(36).slice(2, 5);
    const chargeCorrelationID = `${randChunk()}-${randChunk()}-${phoneDigitsRaw || 'no-phone'}`;

    const payload = {
        correlationID: chargeCorrelationID,
        value,
        comment: sanitizeText(comment || 'Agência OPPUS - Checkout'),
        customer: customerPayload,
        additionalInfo: sanitizedAdditional
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
            const addInfoArr = Array.isArray(sanitizedAdditional) ? sanitizedAdditional : [];
            const addInfo = addInfoArr.reduce((acc, item) => {
                const k = String(item?.key || '').trim();
                const v = String(item?.value || '').trim();
                acc[k] = v;
                return acc;
            }, {});

            const tipo = addInfo['tipo_servico'] || '';
            const qtd = Number(addInfo['quantidade'] || 0) || 0;
            const instauserFromClient = addInfo['instagram_username'] || '';
            const userAgent = req.get('User-Agent') || '';
            const ip = req.realIP || req.ip || req.connection?.remoteAddress || 'unknown';
            const slug = req.session?.linkSlug || '';

            const pix = charge?.paymentMethods?.pix || {};
            const createdIso = new Date().toISOString();
            const identifier = charge?.identifier || pix?.transactionID || null;
            const record = {
                // Campos principais solicitados
                nomeUsuario: null, // será atualizado quando o pagamento for confirmado
                telefone: customerPayload.phone || '',
                correlationID: chargeCorrelationID,
                instauser: instauserFromClient,
                criado: createdIso,
                identifier,
                status: 'pendente',
                qtd,
                tipo,

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
    return res.status(200).json(response.data);
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

// Rota para liberar acesso à /perfil após validação de link temporário
app.get('/:slug', async (req, res, next) => {
    const { slug } = req.params;
    console.log('🔎 Capturado em /:slug:', slug);
    // EXCEÇÕES explícitas devem ser tratadas antes de qualquer validação
    if (slug === 'checkout') {
        return res.render('checkout', { PIXEL_ID: process.env.PIXEL_ID || '' });
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
            req.session.perfilAccessAllowed = true;
            req.session.linkSlug = slug;
            req.session.linkAccessTime = Date.now();
            // Atualizar linha do Baserow com IP e User-Agent (mantém igual)
            const userAgent = req.get('User-Agent') || '';
            const ip = req.realIP || req.ip || req.connection.remoteAddress || 'unknown';
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

// API para verificar perfil do Instagram (usando API interna)
app.post("/api/check-instagram-profile", async (req, res) => {
    const { username } = req.body;
    const userAgent = req.get("User-Agent") || "";
    const ip = req.realIP || req.ip || req.connection.remoteAddress || "";

    if (!username || username.length < 1) {
        return res.status(400).json({
            success: false,
            error: "Nome de usuário é obrigatório"
        });
    }

    // Verificar se instauser já foi usado anteriormente
    const instauserExists = await checkInstauserExists(username);
    if (instauserExists) {
        return res.status(409).json({
            success: false,
            error: "Este perfil já foi testado anteriormente. O serviço de teste já foi realizado para este usuário.",
            code: "INSTAUSER_ALREADY_USED"
        });
    }

    try {
        // Chamar a função diretamente para debug
        const result = await verifyInstagramProfile(username, userAgent, ip, req, res);
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
                curtidas: '20'
            };
            const quantity = quantitiesMap[selectedServiceKey] || '50';
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

// Meta CAPI: Track InitiateCheckout
app.post('/api/meta/track', async (req, res) => {
  try {
    const PIXEL_ID = process.env.PIXEL_ID || '1019661457030791';
    const ACCESS_TOKEN = process.env.META_CAPI_TOKEN || 'EAAbmJnZB6GX8BP0bbQu4rrUk1FrzDQJGJu58NGKq1YIApXxPbDQ9TcZCJTBIWlsri8iG3dL1BTLc6L65LylloIeicHRPj1oxzZAUcLdSHzOOOFJ6NhrbIgZA4l7VqYffK89T4viCdHqBwcBIHLjZAoDhe1N58ZCzblp7kbvrDk6bYgW3eLroywac08SopAnAZDZD';

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

    // Normalização e hashing do telefone para CAPI
    const normalizePhone = (p) => (String(p || '').replace(/[^0-9]/g, ''));
    const phoneNorm = normalizePhone(phone);
    const phoneHash = phoneNorm ? crypto.createHash('sha256').update(phoneNorm, 'utf8').digest('hex') : undefined;

    const event_time = Math.floor(Date.now() / 1000);
    const userAgent = req.headers['user-agent'] || '';
    const clientIp = (req.headers['x-forwarded-for']?.split(',')[0] || req.socket?.remoteAddress || '').toString();

    const testCode = process.env.META_TEST_EVENT_CODE;
    const payload = {
      data: [
        {
          event_name: eventName,
          event_time,
          action_source: 'website',
          event_source_url: eventSourceUrl || `${req.protocol}://${req.get('host')}${req.originalUrl}`,
          event_id: correlationID || undefined,
          user_data: {
            fbp: fbp || undefined,
            client_user_agent: userAgent,
            client_ip_address: clientIp,
            ph: phoneHash ? [phoneHash] : undefined,
          },
          custom_data: {
            value: Number(value) || 0,
            currency,
            content_name: contentName,
            contents: Array.isArray(contents) ? contents : [],
            num_items: Array.isArray(contents) ? contents.reduce((acc, c) => acc + (Number(c.quantity) || 0), 0) : 0,
          }
        }
      ],
      ...(testCode ? { test_event_code: testCode } : {})
    };

    const url = `https://graph.facebook.com/v18.0/${PIXEL_ID}/events?access_token=${ACCESS_TOKEN}`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await resp.json();
    if (!resp.ok) {
      return res.status(resp.status).json({ success: false, error: data?.error || 'meta_error', details: data });
    }
    return res.json({ success: true, result: data });
  } catch (err) {
    return res.status(500).json({ success: false, error: 'exception', details: err?.message || String(err) });
  }
});

// Webhook Woovi/OpenPix: CHARGE_CREATED -> enviar InitiateCheckout (CAPI)
app.post('/api/openpix/webhook', async (req, res) => {
  try {
    const body = req.body || {};
    const event = String(body.event || '').toUpperCase();

    // Atualiza status para 'pago' quando a cobrança for concluída
    if (/CHARGE_COMPLETED/.test(event)) {
      const charge = body.charge || {};
      const customerName = charge?.customer?.name || null;

      try {
        const col = await getCollection('checkout_orders');
        const conds = [];
        if (charge?.id) conds.push({ 'woovi.chargeId': charge.id });
        if (charge?.correlationID) conds.push({ correlationID: charge.correlationID });
        if (charge?.identifier) conds.push({ 'woovi.identifier': charge.identifier });
        const filter = conds.length ? { $or: conds } : { correlationID: charge?.correlationID || '' };

        const update = {
          $set: {
            status: 'pago',
            'woovi.status': 'pago',
            ...(customerName ? { nomeUsuario: customerName } : {}),
            paidAt: new Date().toISOString()
          }
        };

        const result = await col.updateOne(filter, update);
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
    const addInfoArr = Array.isArray(additionalInfo) ? additionalInfo.map((item) => ({ key: sanitizeText(String(item?.key ?? '')), value: sanitizeText(String(item?.value ?? '')) })) : [];
    const addInfo = addInfoArr.reduce((acc, item) => { acc[String(item.key || '')] = String(item.value || ''); return acc; }, {});
    const tipo = addInfo['tipo_servico'] || '';
    const qtd = Number(addInfo['quantidade'] || 0) || 0;
    const instauserFromClient = addInfo['instagram_username'] || '';
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
app.listen(port, () => {
  console.log("🗄️ Baserow configurado com sucesso");
  console.log(`Servidor rodando na porta ${port}`);
  console.log(`Preview disponível: http://localhost:${port}/checkout`);
});

