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
async function downloadAndServeImage(imageUrl, username) {
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
      timeout: 10000
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
                let profileImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
                let driveImageUrl = null;

                if (user.profile_pic_url_hd || user.profile_pic_url) {
                    try {
                        if (driveManager.isReady()) {
                            try {
                                // Tentar fazer upload para Google Drive se disponível
                                // Por enquanto, usar apenas a URL original do Instagram
                                console.log(`📸 Usando URL original do Instagram: ${user.profile_pic_url_hd || user.profile_pic_url}`);
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
                    status: 403, 
                    error: "Este perfil é privado. Para que o serviço seja realizado, o perfil precisa estar no modo público.",
                    profile: {
                        username: user.username,
                        fullName: user.full_name,
                        profilePicUrl: profileImageUrl,
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

            if (user.profile_pic_url_hd || user.profile_pic_url) {
                try {
                    if (driveManager.isReady()) {
                        const imageUrl = user.profile_pic_url_hd || user.profile_pic_url;
                        const imageResponse = await axios.get(imageUrl, {
                            responseType: 'arraybuffer',
                            timeout: 10000
                        });
                        
                        const fileName = `${user.username}_profile_${Date.now()}.jpg`;
                        const uploadResult = await driveManager.uploadBuffer(
                            imageResponse.data,
                            fileName,
                            'image/jpeg',
                            driveManager.profileImagesFolderId
                        );
                        
                        if (uploadResult.success) {
                            // Baixar e servir localmente
                            const localImageUrl = await downloadAndServeImage(imageUrl, user.username);
                            driveImageUrl = localImageUrl || imageUrl;
                        } else {
                            driveImageUrl = imageUrl;
                        }
                    } else {
                        driveImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
                    }
                } catch (error) {
                    driveImageUrl = user.profile_pic_url_hd || user.profile_pic_url;
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
                        const row = result.rows.find(r => r.link === linkId);
                        if (row) {
                            const fingerprint = generateFingerprint(ip, userAgent);
                            const updateData = {
                                'user-agent': fingerprint,
                                'ip': ip,
                                'instauser': user.username,
                                'statushttp': '200',
                                'teste': ''
                            };
                            await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, updateData);
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
                            await baserowManager.createRow(BASEROW_TABLES.CONTROLE, data);
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
                profilePicUrl: driveImageUrl || user.profile_pic_url_hd || user.profile_pic_url,
                isVerified: user.is_verified,
                followersCount: user.edge_followed_by ? user.edge_followed_by.count : 0,
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
const port = 3000;

// Inicializar gerenciadores
const linkManager = new LinkManager();
const driveManager = new GoogleDriveManager();
const baserowManager = new BaserowManager("https://baserow.atendimento.info", process.env.BASEROW_TOKEN);

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
            (row['user-agent'] === userAgent) &&  // user-agent
            (row.ip === ip) &&                     // ip
            (row.instauser === instauser)         // instauser
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
        
        const result = await baserowManager.createRow(BASEROW_TABLES.CONTROLE, data);
        
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
        const result = await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, recordId, data);
        
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
        const existingUser = result.rows.find(row =>
            (row.instauser && row.instauser.toLowerCase() === instauser.toLowerCase())
        );
        
        if (existingUser) {
            // Verificar se o teste está como 'OK'
            const testeValue = existingUser.teste;
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

// Servir arquivos estáticos
app.use(express.static("public"));
app.use('/temp-images', express.static(path.join(__dirname, 'temp_images')));

// Configurar view engine
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

// Middleware para controlar acesso à página de perfil
function perfilAccessGuard(req, res, next) {
    // Permitir acesso se sessão indicar acesso válido
    if (req.session && req.session.perfilAccessAllowed) {
        return next();
    }
    // Caso contrário, renderizar restrito
    return res.status(403).render('restrito');
}

// Rota para bloquear acesso direto à raiz
app.get('/', (req, res) => {
    return res.status(403).render('restrito');
});

// Rota especial para teste123 (DEVE vir ANTES da rota /:slug)
app.get('/teste123', (req, res) => {
    req.session.perfilAccessAllowed = true;
    req.session.linkSlug = 'teste123';
    req.session.linkAccessTime = Date.now();
    res.render('index');
});

// Rota para liberar acesso à /perfil após validação de link temporário
app.get('/:slug', async (req, res, next) => {
    const { slug } = req.params;
    const reservedSlugs = [
        'perfil', 'used.html', 'admin', 'api', 'generate', 'favicon.ico', 'robots.txt', 'css', 'js', 'images', 'public', 'node_modules', 'teste123'
    ];
    if (reservedSlugs.includes(slug)) return next();

    // EXCEÇÃO: Permitir fluxo normal para /teste123
    if (slug === 'teste123') {
        req.session.perfilAccessAllowed = true;
        req.session.linkSlug = slug;
        req.session.linkAccessTime = Date.now();
        return res.render('index');
    }

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
                const row = result.rows.find(r => r.link === slug);
                if (row) {
                    const updateData = {
                        'user-agent': userAgent,
                        'ip': ip
                    };
                    await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, updateData);
                }
            }
            return res.render('index');
        }
        return res.status(410).render('used');
    } catch (err) {
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
    // Caso contrário, acesso restrito
    return res.status(403).render('restrito');
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
        // EXCEÇÃO: Para teste123, somente quando passado explicitamente via query
        if ((req.query.id || '') === 'teste123') {
            // Mapear serviço conforme escolha
            const serviceMap = {
                seguidores_mistos: '659',
                seguidores_brasileiros: '617',
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
            const isFollowerService = ['659', '617'].includes(String(selectedServiceId)) || (selectedServiceKey || '').startsWith('seguidores');
            const isLikesService = (selectedServiceKey || '').startsWith('curtidas');
            // Preparar valor do campo alvo SEMPRE usando 'link'
            const rawValue = linkFromBody || username || '';
            let targetField = 'link';
            let targetValue = rawValue;
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
                const params = new URLSearchParams();
                params.append('key', 'da6969dfc71de1e0e182b0800b395367');
                params.append('action', 'add');
                params.append('service', selectedServiceId);
                params.append(targetField, targetValue);
                params.append('quantity', quantity);
                console.log('[FAMA24H][TESTE123] Enviando pedido', { service: selectedServiceId, quantity, [targetField]: targetValue });
                const apiCandidates = ['https://fama24h.com/api/v2', 'https://www.fama24h.com/api/v2', 'https://fama24h.net/api/v2'];
                for (const apiUrl of apiCandidates) {
                    try {
                        response = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                        console.log('[FAMA24H][TESTE123] Sucesso em', apiUrl);
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
            return res.json({
                ...response.data,
                success: true,
                message: 'Pedido realizado com sucesso (teste123)'
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
            seguidores_mistos: '659',
            seguidores_brasileiros: '617',
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
        const rawValue = linkFromBody || '';
        const isFollowerService = ['659', '617'].includes(String(selectedServiceId)) || (selectedServiceKey || '').startsWith('seguidores');
        const isLikesService = (selectedServiceKey || '').startsWith('curtidas');
        // Definir campo/valor correto conforme tipo de serviço
        let targetField = 'link';
        let targetValue = rawValue || '';
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
        } else {
            // Fama24h para seguidores e visualizações
            const params = new URLSearchParams();
            params.append('key', 'da6969dfc71de1e0e182b0800b395367');
            params.append('action', 'add');
            params.append('service', selectedServiceId);
            params.append(targetField, targetValue);
            params.append('quantity', quantity);
            console.log('[FAMA24H] Enviando pedido', { service: selectedServiceId, quantity, [targetField]: targetValue });
            // Tentar múltiplos domínios para evitar ENOTFOUND
            const apiCandidates = ['https://fama24h.com/api/v2', 'https://www.fama24h.com/api/v2', 'https://fama24h.net/api/v2'];
            for (const apiUrl of apiCandidates) {
                try {
                    response = await axios.post(apiUrl, params, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
                    console.log('[FAMA24H] Sucesso em', apiUrl);
                    break;
                } catch (err) {
                    if (err.code === 'ENOTFOUND') {
                        console.warn('[FAMA24H] ENOTFOUND em', apiUrl, '- tentando próximo');
                        continue;
                    }
                    throw err;
                }
            }
        }
        if (response.data.order) {
            // Buscar a linha correta no Baserow pelo campo 'link' igual ao linkId
            const result = await baserowManager.getAllTableRows(BASEROW_TABLES.CONTROLE);
            if (result.success) {
                const row = result.rows.find(r => r.link === linkId);
                if (row) {
                    const updateData = {
                        instauser: targetValue,
                        statushttp: 'OK',
                        teste: 'OK'
                    };
                    await baserowManager.updateRowPatch(BASEROW_TABLES.CONTROLE, row.id, updateData);
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
        res.json(response.data);
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
      (row['user-agent'] === userAgent) &&
      (row.ip === ip)
    );
    if (found) {
      // Verificar se o teste está como 'OK' - só bloquear se teste for OK
      const testeValue = found.teste;
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
      (row.link === id) &&
      (row.teste === 'OK')
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
    const result = await baserowManager.createRow(BASEROW_TABLES.CONTROLE, data);
    if (result.success) {
      console.log("✅ Webhook registrado na tabela controle:", result.row.id);
      // Verificar leitura imediata para confirmar persistência
      const readBack = await baserowManager.getRow(BASEROW_TABLES.CONTROLE, result.row.id);
      if (!readBack.success) {
        console.error("⚠️ Criado mas não foi possível ler a linha imediatamente:", readBack.error);
      } else {
        console.log("🔎 Linha confirmada no Baserow:", readBack.row?.id);
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

app.listen(port, () => {
  console.log(`Servidor rodando na porta ${port}`);
});

