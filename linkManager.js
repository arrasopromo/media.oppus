const crypto = require('crypto');

// Função utilitária para normalizar IP (remove ::ffff:)
function normalizeIP(ip) {
    return (ip || '').replace(/^::ffff:/, '');
}

class LinkManager {
    constructor() {
        this.links = new Map();
        this.usageHistory = new Map(); // Map<fingerprint, usageCount>
        this.maxUsesPerFingerprint = 2; // Permitir 2 usos por IP/User-Agent
        this.exceptionIPs = ['179.0.74.243']; // IPs com acesso ilimitado
        this.cleanupInterval = 5 * 60 * 1000; // Limpeza a cada 5 minutos
        this.linkDuration = 10 * 60 * 1000; // 10 minutos
        
        // Iniciar limpeza automática
        this.startCleanupTimer();
    }
    
    // Gerar novo link temporário
    generateLink(req) {
        const id = crypto.randomBytes(6).toString('hex');
        const currentIP = normalizeIP(req.ip || req.connection.remoteAddress || req.headers['x-forwarded-for']);
        const currentUserAgent = req.get('User-Agent') || '';
        const now = Date.now();
        
        const linkData = {
            id: id,
            ip: currentIP,
            userAgent: currentUserAgent,
            createdAt: now,
            expiresAt: now + this.linkDuration,
            accessCount: 0,
            lastAccess: null
        };
        
        this.links.set(id, linkData);
        
        console.log(`[LINK][CREATE] Link gerado: ${id} para IP: ${currentIP} | User-Agent: ${currentUserAgent} | Expira em: ${new Date(linkData.expiresAt).toISOString()}`);
        
        return {
            id: id,
            url: `/u/${id}`,
            expiresAt: linkData.expiresAt,
            expiresIn: this.linkDuration / 1000 // em segundos
        };
    }
    
    // Validar link temporário
    validateLink(id, req) {
        const linkData = this.links.get(id);
        console.log(`[LINK][VALIDATE] Tentando validar link: ${id}`);
        if (!linkData) {
            console.log(`[LINK][VALIDATE] Link não encontrado: ${id}`);
            return { valid: false, reason: 'Link não encontrado' };
        }
        const now = Date.now();
        if (now > linkData.expiresAt) {
            this.links.delete(id);
            console.log(`[LINK][VALIDATE] Link expirado: ${id}`);
            return { valid: false, reason: 'Link expirado' };
        }
        
        // DESABILITAR TEMPORARIAMENTE o limite de uso por fingerprint para acesso às páginas
        // const fingerprint = this.createFingerprint(linkData.ip, linkData.userAgent);
        // const isExceptionIP = this.exceptionIPs.includes(linkData.ip);
        // if (!isExceptionIP) {
        //     const currentUsage = this.usageHistory.get(fingerprint) || 0;
        //     if (currentUsage >= this.maxUsesPerFingerprint) {
        //         console.log(`[LINK][VALIDATE] Limite de uso excedido para fingerprint: ${fingerprint} (${currentUsage}/${this.maxUsesPerFingerprint})`);
        //         return { valid: false, reason: 'Limite de uso excedido' };
        //     }
        //     this.usageHistory.set(fingerprint, currentUsage + 1);
        //     console.log(`[LINK][VALIDATE] Uso registrado para fingerprint: ${fingerprint} (${currentUsage + 1}/${this.maxUsesPerFingerprint})`);
        // } else {
        //     console.log(`[LINK][VALIDATE] IP de exceção detectado: ${linkData.ip} - acesso ilimitado`);
        // }
        
        // Atualizar estatísticas de acesso
        linkData.accessCount++;
        linkData.lastAccess = now;
        
        console.log(`[LINK][VALIDATE] Link válido acessado: ${id} (${linkData.accessCount}ª vez) | Tempo restante: ${(linkData.expiresAt - now) / 1000}s`);
        
        return { 
            valid: true, 
            linkData: linkData,
            remainingTime: linkData.expiresAt - now
        };
    }
    
    // Criar fingerprint único baseado em IP e User-Agent
    createFingerprint(ip, userAgent) {
        const crypto = require('crypto');
        const data = `${ip}:${userAgent}`;
        return crypto.createHash('md5').update(data).digest('hex');
    }
    
    // Invalidar link específico
    invalidateLink(id) {
        const deleted = this.links.delete(id);
        if (deleted) {
            console.log(`Link invalidado manualmente: ${id}`);
        }
        return deleted;
    }
    
    // Obter estatísticas de um link
    getLinkStats(id) {
        const linkData = this.links.get(id);
        if (!linkData) {
            return null;
        }
        
        const now = Date.now();
        return {
            id: linkData.id,
            createdAt: new Date(linkData.createdAt).toISOString(),
            expiresAt: new Date(linkData.expiresAt).toISOString(),
            remainingTime: Math.max(0, linkData.expiresAt - now),
            accessCount: linkData.accessCount,
            lastAccess: linkData.lastAccess ? new Date(linkData.lastAccess).toISOString() : null,
            isExpired: now > linkData.expiresAt
        };
    }
    
    // Obter estatísticas gerais
    getGeneralStats() {
        const now = Date.now();
        let activeLinks = 0;
        let expiredLinks = 0;
        let totalAccesses = 0;
        
        for (const [id, linkData] of this.links) {
            if (now > linkData.expiresAt) {
                expiredLinks++;
            } else {
                activeLinks++;
            }
            totalAccesses += linkData.accessCount;
        }
        
        return {
            totalLinks: this.links.size,
            activeLinks: activeLinks,
            expiredLinks: expiredLinks,
            totalAccesses: totalAccesses,
            cleanupInterval: this.cleanupInterval / 1000,
            linkDuration: this.linkDuration / 1000
        };
    }
    
    // Limpeza automática de links expirados
    cleanup() {
        const now = Date.now();
        let cleanedCount = 0;
        
        for (const [id, linkData] of this.links) {
            if (now > linkData.expiresAt) {
                this.links.delete(id);
                cleanedCount++;
            }
        }
        
        if (cleanedCount > 0) {
            console.log(`Limpeza automática: ${cleanedCount} links expirados removidos`);
        }
        
        return cleanedCount;
    }
    
    // Iniciar timer de limpeza automática
    startCleanupTimer() {
        setInterval(() => {
            this.cleanup();
        }, this.cleanupInterval);
        
        console.log(`Timer de limpeza iniciado (a cada ${this.cleanupInterval / 1000} segundos)`);
    }
    
    // Parar timer de limpeza (para testes ou shutdown)
    stopCleanupTimer() {
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = null;
            console.log('Timer de limpeza parado');
        }
    }
}

module.exports = LinkManager;

