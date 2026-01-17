const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

class GoogleDriveManager {
    constructor() {
        this.oauth2Client = null;
        this.drive = null;
        this.isConfigured = false;
        
        // Configurações OAuth2 via variáveis de ambiente
        this.clientId = process.env.GOOGLE_CLIENT_ID || '';
        this.clientSecret = process.env.GOOGLE_CLIENT_SECRET || '';
        this.redirectUri = process.env.GOOGLE_REDIRECT_URI || '';
        
        // ID da pasta específica para imagens de perfil (será criada se não existir)
        this.profileImagesFolderId = null;
        
        this.initializeOAuth();
    }
    
    initializeOAuth() {
        try {
            if (!this.clientId || !this.clientSecret || !this.redirectUri) {
                console.log('⚠️ Google OAuth não configurado: defina GOOGLE_CLIENT_ID/SECRET/REDIRECT_URI.');
                return;
            }
            this.oauth2Client = new google.auth.OAuth2(
                this.clientId,
                this.clientSecret,
                this.redirectUri
            );
            
            // Verificar se há tokens salvos
            this.loadSavedTokens();
            
        } catch (error) {
            console.error('Erro ao inicializar OAuth2:', error);
        }
    }
    
    loadSavedTokens() {
        try {
            const tokenPath = path.join(__dirname, 'google-tokens.json');
            if (fs.existsSync(tokenPath)) {
                const tokens = JSON.parse(fs.readFileSync(tokenPath, 'utf8'));
                this.oauth2Client.setCredentials(tokens);
                this.drive = google.drive({ version: 'v3', auth: this.oauth2Client });
                this.isConfigured = true;
                console.log('✅ Google Drive configurado com tokens salvos');
                
                // Inicializar pasta de imagens de perfil
                this.initializeProfileImagesFolder();
            } else {
                console.log('⚠️ Tokens do Google Drive não encontrados');
                console.log('🔗 URL de autenticação:', this.getAuthUrl());
            }
        } catch (error) {
            console.error('Erro ao carregar tokens:', error);
        }
    }
    
    async initializeProfileImagesFolder() {
        if (!this.drive) {
            console.warn('Google Drive não inicializado, pulando pasta de imagens');
            return;
        }
        
        try {
            const response = await this.drive.files.list({
                q: "name='Instagram Profile Images' and mimeType='application/vnd.google-apps.folder'",
                fields: 'files(id, name)'
            });
            
            if (response.data.files.length > 0) {
                this.profileImagesFolderId = response.data.files[0].id;
                console.log('📁 Pasta de imagens de perfil encontrada:', this.profileImagesFolderId);
            } else {
                const folderMetadata = {
                    name: 'Instagram Profile Images',
                    mimeType: 'application/vnd.google-apps.folder'
                };
                
                const folder = await this.drive.files.create({
                    resource: folderMetadata,
                    fields: 'id'
                });
                
                this.profileImagesFolderId = folder.data.id;
                console.log('📁 Pasta de imagens de perfil criada:', this.profileImagesFolderId);
                
                await this.drive.permissions.create({
                    fileId: this.profileImagesFolderId,
                    resource: {
                        role: 'reader',
                        type: 'anyone'
                    }
                });
            }
        } catch (error) {
            const unauthorized = error && (
                error.code === 401 ||
                error.status === 401 ||
                (error.response && error.response.status === 401) ||
                (typeof error.message === 'string' && error.message.toLowerCase().includes('unauthorized_client'))
            );
            
            if (unauthorized) {
                console.error('Credenciais do Google Drive inválidas (401/unauthorized_client), desativando integração');
                this.isConfigured = false;
                this.profileImagesFolderId = null;
                
                try {
                    const tokenPath = path.join(__dirname, 'google-tokens.json');
                    if (fs.existsSync(tokenPath)) {
                        fs.unlinkSync(tokenPath);
                        console.log('Tokens inválidos do Google Drive removidos (google-tokens.json)');
                    }
                } catch (cleanupError) {
                    console.error('Erro ao remover tokens inválidos do Google Drive:', cleanupError);
                }
            } else {
                console.error('Erro ao inicializar pasta de imagens:', error);
            }
        }
    }
    
    saveTokens(tokens) {
        try {
            const tokenPath = path.join(__dirname, 'google-tokens.json');
            fs.writeFileSync(tokenPath, JSON.stringify(tokens, null, 2));
            console.log('💾 Tokens do Google Drive salvos');
        } catch (error) {
            console.error('Erro ao salvar tokens:', error);
        }
    }
    
    getAuthUrl() {
        const scopes = [
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive.readonly'
        ];
        
        return this.oauth2Client.generateAuthUrl({
            access_type: 'offline',
            scope: scopes,
            prompt: 'consent'
        });
    }
    
    async setCredentials(code) {
        try {
            const { tokens } = await this.oauth2Client.getToken(code);
            this.oauth2Client.setCredentials(tokens);
            this.saveTokens(tokens);
            
            this.drive = google.drive({ version: 'v3', auth: this.oauth2Client });
            this.isConfigured = true;
            
            // Inicializar pasta de imagens de perfil
            await this.initializeProfileImagesFolder();
            
            return { success: true, tokens };
        } catch (error) {
            console.error('Erro ao definir credenciais:', error);
            return { success: false, error: error.message };
        }
    }
    
    async uploadFile(filePath, fileName, mimeType = 'image/jpeg', folderId = null) {
        if (!this.isConfigured) {
            throw new Error('Google Drive não está configurado. Execute a autenticação primeiro.');
        }
        
        try {
            const fileMetadata = {
                name: fileName,
                parents: folderId ? [folderId] : undefined
            };
            
            const media = {
                mimeType: mimeType,
                body: fs.createReadStream(filePath)
            };
            
            const response = await this.drive.files.create({
                resource: fileMetadata,
                media: media,
                fields: 'id,name,webViewLink,webContentLink'
            });
            
            // Tornar o arquivo público
            let permResult = null;
            try {
                permResult = await this.drive.permissions.create({
                    fileId: response.data.id,
                    resource: {
                        role: 'reader',
                        type: 'anyone'
                    }
                });
                console.log('Permissão pública aplicada:', JSON.stringify(permResult.data));
            } catch (permError) {
                console.error('Erro ao aplicar permissão pública:', permError.response?.data || permError.message || permError);
            }
            
            console.log(`📤 Arquivo enviado para Google Drive: ${response.data.name} (ID: ${response.data.id})`);
            console.log(`🔗 webViewLink: ${response.data.webViewLink}`);
            console.log(`🔗 webContentLink: ${response.data.webContentLink}`);
            console.log(`🔗 directLink: https://drive.google.com/uc?export=view&id=${response.data.id}`);
            
            return {
                success: true,
                fileId: response.data.id,
                fileName: response.data.name,
                webViewLink: response.data.webViewLink,
                webContentLink: response.data.webContentLink,
                directLink: `https://drive.google.com/uc?export=view&id=${response.data.id}`,
                thumbnailLink: `https://drive.google.com/thumbnail?id=${response.data.id}&sz=w400-h400`,
                permResult: permResult ? permResult.data : null
            };
            
        } catch (error) {
            console.error('Erro ao fazer upload:', error);
            throw error;
        }
    }
    
    async uploadBuffer(buffer, fileName, mimeType = 'image/jpeg', folderId = null) {
        if (!this.isConfigured) {
            throw new Error('Google Drive não está configurado. Execute a autenticação primeiro.');
        }
        
        try {
            const fileMetadata = {
                name: fileName,
                parents: folderId ? [folderId] : undefined
            };
            
            // Criar um stream a partir do buffer
            const { Readable } = require('stream');
            const stream = new Readable();
            stream.push(buffer);
            stream.push(null); // Indica o fim do stream
            
            const media = {
                mimeType: mimeType,
                body: stream
            };
            
            const response = await this.drive.files.create({
                resource: fileMetadata,
                media: media,
                fields: 'id,name,webViewLink,webContentLink'
            });
            
            // Tornar o arquivo público
            try {
                await this.drive.permissions.create({
                    fileId: response.data.id,
                    resource: {
                        role: 'reader',
                        type: 'anyone'
                    }
                });
            } catch (permError) {
                console.error('Erro ao aplicar permissão pública:', permError.message);
            }
            
            return {
                success: true,
                id: response.data.id,
                fileName: response.data.name,
                webViewLink: response.data.webViewLink,
                webContentLink: response.data.webContentLink,
                directLink: `https://drive.google.com/uc?export=view&id=${response.data.id}`,
                thumbnailLink: `https://drive.google.com/thumbnail?id=${response.data.id}&sz=w400-h400`
            };
            
        } catch (error) {
            console.error('Erro ao fazer upload:', error);
            throw error;
        }
    }
    
    async uploadProfileImage(imagePath, username) {
        if (!this.isConfigured) {
            console.warn('Google Drive não configurado, pulando upload');
            return { success: false, error: 'Google Drive não configurado' };
        }
        
        try {
            // Verificar se já existe uma imagem para este usuário
            const existingFile = await this.findProfileImage(username);
            if (existingFile) {
                console.log(`🔄 Atualizando imagem existente para @${username}`);
                // Deletar arquivo antigo
                await this.deleteFile(existingFile.id);
            }
            
            const fileName = `profile_${username}_${Date.now()}.jpg`;
            const result = await this.uploadFile(imagePath, fileName, 'image/jpeg', this.profileImagesFolderId);
            
            console.log(`✅ Imagem de perfil de @${username} enviada para Google Drive`);
            return result;
            
        } catch (error) {
            console.error(`Erro ao fazer upload da imagem de @${username}:`, error);
            return { success: false, error: error.message };
        }
    }
    
    async findProfileImage(username) {
        if (!this.isConfigured) {
            return null;
        }
        
        try {
            const response = await this.drive.files.list({
                q: `name contains 'profile_${username}' and parents in '${this.profileImagesFolderId}'`,
                fields: 'files(id, name, createdTime)',
                orderBy: 'createdTime desc'
            });
            
            return response.data.files.length > 0 ? response.data.files[0] : null;
        } catch (error) {
            console.error('Erro ao buscar imagem de perfil:', error);
            return null;
        }
    }
    
    async getProfileImageUrl(username) {
        const file = await this.findProfileImage(username);
        if (file) {
            return `https://drive.google.com/uc?export=view&id=${file.id}`;
        }
        return null;
    }
    
    async listFiles(pageSize = 10) {
        if (!this.isConfigured) {
            throw new Error('Google Drive não está configurado');
        }
        
        try {
            const response = await this.drive.files.list({
                pageSize: pageSize,
                fields: 'nextPageToken, files(id, name, mimeType, createdTime, size)'
            });
            
            return {
                success: true,
                files: response.data.files
            };
            
        } catch (error) {
            console.error('Erro ao listar arquivos:', error);
            throw error;
        }
    }
    
    async deleteFile(fileId) {
        if (!this.isConfigured) {
            throw new Error('Google Drive não está configurado');
        }
        
        try {
            await this.drive.files.delete({
                fileId: fileId
            });
            
            console.log(`🗑️ Arquivo deletado do Google Drive: ${fileId}`);
            return { success: true };
            
        } catch (error) {
            console.error('Erro ao deletar arquivo:', error);
            throw error;
        }
    }
    
    isReady() {
        return this.isConfigured;
    }
    
    getStatus() {
        return {
            configured: this.isConfigured,
            hasTokens: this.oauth2Client && this.oauth2Client.credentials,
            authUrl: this.isConfigured ? null : this.getAuthUrl(),
            profileImagesFolderId: this.profileImagesFolderId,
            drive: this.drive ? 'Initialized' : 'Not initialized'
        };
    }
}

module.exports = GoogleDriveManager;

