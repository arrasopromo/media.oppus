const axios = require('axios');

class BaserowManager {
    constructor(baseUrl = 'https://api.baserow.io', token = null) {
        this.baseUrl = baseUrl;
        this.token = token;
        this.headers = {
            'Content-Type': 'application/json',
            'Authorization': token ? `Token ${token}` : null
        };
    }

    // Configurar token de autenticação
    setToken(token) {
        this.token = token;
        this.headers['Authorization'] = `Token ${token}`;
    }

    // Configurar URL base (para instâncias self-hosted)
    setBaseUrl(baseUrl) {
        this.baseUrl = baseUrl;
    }

    // ==================== OPERAÇÕES DE TABELA ====================

    // Listar campos de uma tabela
    async getTableFields(tableId) {
        try {
            const response = await axios.get(
                `${this.baseUrl}/api/database/fields/table/${tableId}/`,
                { headers: this.headers }
            );
            return { success: true, fields: response.data };
        } catch (error) {
            console.error('Erro ao buscar campos da tabela:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Listar linhas de uma tabela
    async getTableRows(tableId, options = {}) {
        try {
            const params = new URLSearchParams();
            
            // Adicionar parâmetros opcionais
            if (options.page) params.append('page', options.page);
            if (options.size) params.append('size', options.size);
            if (options.search) params.append('search', options.search);
            if (options.order_by) params.append('order_by', options.order_by);
            
            // Adicionar filtros
            if (options.filters) {
                Object.entries(options.filters).forEach(([key, value]) => {
                    params.append(key, value);
                });
            }

            const url = `${this.baseUrl}/api/database/rows/table/${tableId}/?${params.toString()}`;
            const response = await axios.get(url, { headers: this.headers });
            
            return { 
                success: true, 
                rows: response.data.results,
                count: response.data.count,
                next: response.data.next,
                previous: response.data.previous
            };
        } catch (error) {
            console.error('Erro ao buscar linhas da tabela:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Obter uma linha específica
    async getRow(tableId, rowId) {
        try {
            const response = await axios.get(
                `${this.baseUrl}/api/database/rows/table/${tableId}/${rowId}/`,
                { headers: this.headers }
            );
            return { success: true, row: response.data };
        } catch (error) {
            console.error('Erro ao buscar linha:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Criar nova linha
    async createRow(tableId, data) {
        try {
            const response = await axios.post(
                `${this.baseUrl}/api/database/rows/table/${tableId}/`,
                data,
                { headers: this.headers }
            );
            return { success: true, row: response.data };
        } catch (error) {
            console.error('Erro ao criar linha:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Atualizar linha existente
    async updateRow(tableId, rowId, data) {
        try {
            const response = await axios.patch(
                `${this.baseUrl}/api/database/rows/table/${tableId}/${rowId}/`,
                data,
                { headers: this.headers }
            );
            return { success: true, row: response.data };
        } catch (error) {
            console.error('Erro ao atualizar linha:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Deletar linha
    async deleteRow(tableId, rowId) {
        try {
            await axios.delete(
                `${this.baseUrl}/api/database/rows/table/${tableId}/${rowId}/`,
                { headers: this.headers }
            );
            return { success: true };
        } catch (error) {
            console.error('Erro ao deletar linha:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // ==================== OPERAÇÕES ESPECÍFICAS PARA O PROJETO ====================

    // Registrar acesso de usuário
    async logUserAccess(userData) {
        const accessData = {
            ip_address: userData.ip,
            user_agent: userData.userAgent,
            instagram_username: userData.instagramUsername || null,
            profile_verified: userData.profileVerified || false,
            access_timestamp: new Date().toISOString(),
            session_id: userData.sessionId || null,
            link_id: userData.linkId || null,
            webhook_response: userData.webhookResponse || null,
            status: userData.status || 'pending'
        };

        return await this.createRow(process.env.BASEROW_ACCESS_LOG_TABLE_ID, accessData);
    }

    // Registrar perfil verificado
    async logProfileVerification(profileData) {
        const verificationData = {
            instagram_username: profileData.username,
            full_name: profileData.fullName || null,
            profile_pic_url: profileData.profilePicUrl || null,
            follower_count: profileData.followerCount || null,
            following_count: profileData.followingCount || null,
            is_verified: profileData.isVerified || false,
            is_private: profileData.isPrivate || false,
            verification_timestamp: new Date().toISOString(),
            api_response: JSON.stringify(profileData.rawResponse || {}),
            status: 'verified'
        };

        return await this.createRow(process.env.BASEROW_PROFILES_TABLE_ID, verificationData);
    }

    // Registrar webhook call
    async logWebhookCall(webhookData) {
        const logData = {
            webhook_type: webhookData.type, // 'POST' ou 'GET'
            webhook_url: webhookData.url,
            request_payload: JSON.stringify(webhookData.payload || {}),
            response_status: webhookData.responseStatus || null,
            response_body: JSON.stringify(webhookData.responseBody || {}),
            instagram_username: webhookData.instagramUsername || null,
            call_timestamp: new Date().toISOString(),
            success: webhookData.success || false,
            error_message: webhookData.errorMessage || null
        };

        return await this.createRow(process.env.BASEROW_WEBHOOKS_TABLE_ID, logData);
    }

    // Buscar histórico de usuário por IP
    async getUserHistory(ip, userAgent = null) {
        const filters = {
            'filter__ip_address__equal': ip
        };

        if (userAgent) {
            filters['filter__user_agent__equal'] = userAgent;
        }

        return await this.getTableRows(process.env.BASEROW_ACCESS_LOG_TABLE_ID, {
            filters,
            order_by: '-access_timestamp',
            size: 10
        });
    }

    // Verificar se perfil já foi processado
    async checkProfileExists(username) {
        const filters = {
            'filter__instagram_username__equal': username
        };

        const result = await this.getTableRows(process.env.BASEROW_PROFILES_TABLE_ID, {
            filters,
            size: 1
        });

        return {
            exists: result.success && result.count > 0,
            profile: result.success && result.rows.length > 0 ? result.rows[0] : null
        };
    }

    // ==================== UTILITÁRIOS ====================

    // Testar conexão com Baserow
    async testConnection() {
        try {
            // Tentar fazer uma requisição simples para verificar a conexão
            const response = await axios.get(
                `${this.baseUrl}/api/database/fields/table/1/`, // Usar table ID 1 como teste
                { headers: this.headers }
            );
            return { success: true, message: 'Conexão com Baserow estabelecida com sucesso' };
        } catch (error) {
            if (error.response?.status === 401) {
                return { success: false, error: 'Token de autenticação inválido' };
            } else if (error.response?.status === 404) {
                return { success: false, error: 'Tabela não encontrada (normal para teste)' };
            } else {
                return { success: false, error: error.response?.data || error.message };
            }
        }
    }

    // Obter estatísticas gerais
    async getStats() {
        try {
            const accessLogResult = await this.getTableRows(process.env.BASEROW_ACCESS_LOG_TABLE_ID, { size: 1 });
            const profilesResult = await this.getTableRows(process.env.BASEROW_PROFILES_TABLE_ID, { size: 1 });
            const webhooksResult = await this.getTableRows(process.env.BASEROW_WEBHOOKS_TABLE_ID, { size: 1 });

            return {
                success: true,
                stats: {
                    totalAccesses: accessLogResult.success ? accessLogResult.count : 0,
                    totalProfiles: profilesResult.success ? profilesResult.count : 0,
                    totalWebhooks: webhooksResult.success ? webhooksResult.count : 0
                }
            };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    // Buscar todas as linhas de uma tabela com nomes de campos
    async getAllTableRows(tableId) {
        try {
            const response = await axios.get(
                `${this.baseUrl}/api/database/rows/table/${tableId}/?user_field_names=true`,
                { headers: this.headers }
            );
            return { 
                success: true, 
                rows: response.data.results,
                count: response.data.count
            };
        } catch (error) {
            console.error('Erro ao buscar todas as linhas da tabela:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Buscar uma linha específica por ID
    async getRow(tableId, rowId) {
        try {
            const response = await axios.get(
                `${this.baseUrl}/api/database/rows/table/${tableId}/${rowId}/?user_field_names=true`,
                { headers: this.headers }
            );
            return { success: true, row: response.data };
        } catch (error) {
            console.error('Erro ao buscar linha:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }

    // Atualizar linha usando PATCH
    async updateRowPatch(tableId, rowId, data) {
        try {
            const response = await axios.patch(
                `${this.baseUrl}/api/database/rows/table/${tableId}/${rowId}/?user_field_names=true`,
                data,
                { headers: this.headers }
            );
            return { success: true, row: response.data };
        } catch (error) {
            console.error('Erro ao atualizar linha:', error.response?.data || error.message);
            return { success: false, error: error.response?.data || error.message };
        }
    }
}

module.exports = BaserowManager;

