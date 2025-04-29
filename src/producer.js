const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const os = require('os');
const fs = require('fs');
const { promisify } = require('util');
const path = require('path');
const http = require('http');
const { MongoClient } = require('mongodb');

// Configurações
const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'];
const TOPIC = process.env.KAFKA_TOPIC || 'rcs-messages';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10000', 10);
const COMPRESSION = process.env.COMPRESSION || 'snappy';
const CLIENT_ID = process.env.CLIENT_ID || `rcs-producer-${os.hostname()}`;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY || '1000', 10);
const DEAD_LETTER_FILE = process.env.DEAD_LETTER_FILE || './dead_letter_queue.json';
const HTTP_PORT = parseInt(process.env.HTTP_PORT || '3000', 10);

// MongoDB config
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'rcs_messages';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'messages';

// Variáveis globais
let mongoClient;
let messagesCollection;
let messagesSent = 0;
let batchesSent = 0;
let retryAttempts = 0;
let failedMessages = 0;
let lastReportTime = Date.now();
let pendingMessages = [];
let retryQueue = [];
let isProcessingBatch = false;

const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);

/**
 * Conecta ao banco de dados MongoDB
 */
async function connectToDatabase() {
    try {
        console.log('Conectando ao MongoDB...');
        mongoClient = new MongoClient(MONGODB_URI);
        await mongoClient.connect();

        const db = mongoClient.db(DB_NAME);
        messagesCollection = db.collection(COLLECTION_NAME);

        console.log('Conexão com MongoDB estabelecida com sucesso');

        // Cria índices para melhor performance
        await messagesCollection.createIndex({ messageId: 1 }, { unique: true });
        await messagesCollection.createIndex({ timestamp: 1 });
        await messagesCollection.createIndex({ phoneNumber: 1 });
        await messagesCollection.createIndex({ status: 1 });

        return true;
    } catch (error) {
        console.error('Erro ao conectar ao MongoDB:', error);
        return false;
    }
}

/**
 * Criação do cliente Kafka e produtor com configurações otimizadas
 */
const kafka = new Kafka({
    clientId: CLIENT_ID,
    brokers: KAFKA_BROKERS,
    retry: {
        initialRetryTime: 100,
        retries: 8,
        factor: 1.5,
        maxRetryTime: 30000,
    },
    connectionTimeout: 5000,
    logLevel: logLevel.ERROR,
});

const producer = kafka.producer({
    allowAutoTopicCreation: true,
    transactionTimeout: 30000,
    idempotent: true,
    maxInFlightRequests: 5,
});

/**
 * Adiciona mensagens ao banco de dados e à fila de pendentes
 * @param {Array} messages - Mensagens RCS a serem processadas
 * @returns {Promise<Array>} - Array com as mensagens salvas
 */
async function addMessagesToPendingQueue(messages) {
    try {
        if (!messagesCollection) {
            throw new Error('Conexão com o banco de dados não está disponível');
        }

        // Adiciona status e timestamp para as mensagens
        const messagesWithStatus = messages.map(msg => ({
            ...msg,
            status: 'pending',
            created_at: new Date(),
            messageId: msg.messageId || `msg-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`,
            timestamp: msg.timestamp || Date.now(),
        }));

        // Insere as mensagens no MongoDB
        await messagesCollection.insertMany(messagesWithStatus, { ordered: false })
            .catch(err => {
                // Ignora erros de chave duplicada, mas loga outros erros
                if (!err.message.includes('duplicate key')) {
                    console.error('Erro ao inserir mensagens no MongoDB:', err);
                }
            });

        // Adiciona as mensagens à fila pendente
        pendingMessages.push(...messagesWithStatus);

        console.log(`${messagesWithStatus.length} mensagens adicionadas à fila (total pendente: ${pendingMessages.length})`);

        // Inicia o processamento em batch se não estiver em andamento
        if (!isProcessingBatch) {
            processPendingMessages();
        }

        return messagesWithStatus;
    } catch (error) {
        console.error('Erro ao adicionar mensagens à fila pendente:', error);
        throw error;
    }
}

/**
 * Atualiza o status das mensagens no banco de dados
 * @param {Array} messageIds - IDs das mensagens a atualizar
 * @param {string} status - Novo status ('sent', 'failed', etc)
 */
async function updateMessagesStatus(messageIds, status) {
    if (!messagesCollection) {
        return;
    }

    try {
        await messagesCollection.updateMany(
            { messageId: { $in: messageIds } },
            {
                $set: {
                    status: status,
                    updated_at: new Date()
                }
            }
        );
    } catch (error) {
        console.error(`Erro ao atualizar status das mensagens para ${status}:`, error);
    }
}

/**
 * Processa as mensagens pendentes em lotes
 */
async function processPendingMessages() {
    if (pendingMessages.length === 0 || isProcessingBatch) {
        return;
    }

    isProcessingBatch = true;

    try {
        // Pega um batch das mensagens pendentes
        const batchSize = Math.min(BATCH_SIZE, pendingMessages.length);
        const batch = pendingMessages.splice(0, batchSize);

        console.log(`Processando batch de ${batch.length} mensagens pendentes...`);

        // Prepara as mensagens para o Kafka
        const kafkaMessages = prepareKafkaMessages(batch);

        // Envia para o Kafka
        await sendBatch(kafkaMessages);

        // Atualiza o status no banco
        const messageIds = batch.map(msg => msg.messageId);
        await updateMessagesStatus(messageIds, 'sent');

    } catch (error) {
        console.error('Erro ao processar mensagens pendentes:', error);
    } finally {
        isProcessingBatch = false;

        // Se ainda há mensagens pendentes, continua processando
        if (pendingMessages.length > 0) {
            setImmediate(processPendingMessages);
        }
    }
}

/**
 * Converte mensagens RCS em mensagens Kafka
 * @param {Array} messages - Array de mensagens RCS
 * @returns {Array} - Array de mensagens formatadas para Kafka
 */
function prepareKafkaMessages(messages) {
    return messages.map(msg => ({
        key: msg.phoneNumber,
        value: JSON.stringify(msg),
        headers: {
            'message-type': 'rcs',
            'message-id': msg.messageId,
            timestamp: `${msg.timestamp}`
        },
    }));
}

/**
 * Salva mensagens na fila de "dead letter" para reprocessamento futuro
 * @param {Array} messages - Mensagens que falharam após várias tentativas
 */
async function saveToDeadLetterQueue(messages) {
    try {
        // Lê mensagens existentes se o arquivo existir
        let existingMessages = [];
        try {
            const data = await readFileAsync(DEAD_LETTER_FILE, 'utf8');
            existingMessages = JSON.parse(data);
        } catch (err) {
            // Arquivo não existe ou está vazio, inicia com array vazio
        }

        // Adiciona novas mensagens falhas
        const allMessages = [...existingMessages, ...messages];

        // Salva no arquivo
        await writeFileAsync(DEAD_LETTER_FILE, JSON.stringify(allMessages, null, 2));
        console.log(`${messages.length} mensagens salvas na fila de dead letter (Total: ${allMessages.length})`);

        // Atualiza métricas
        failedMessages += messages.length;

        // Atualiza o status das mensagens no banco
        if (messagesCollection) {
            const messageIds = messages.map(msg => msg.messageId);
            await updateMessagesStatus(messageIds, 'failed');
        }
    } catch (error) {
        console.error('Erro ao salvar mensagens na fila de dead letter:', error);
    }
}

/**
 * Tenta reenviar mensagens falhas da fila de reprocessamento
 */
async function processRetryQueue() {
    if (retryQueue.length === 0) {
        return;
    }

    console.log(`Processando ${retryQueue.length} mensagens da fila de retry`);

    // Pega até BATCH_SIZE mensagens da fila de retry
    const messagesToRetry = retryQueue.splice(0, BATCH_SIZE);

    try {
        await sendBatchWithRetry(messagesToRetry, 0);
        console.log(`Reprocessamento concluído com sucesso para ${messagesToRetry.length} mensagens`);

        // Atualiza status no banco para 'sent'
        const messageIds = messagesToRetry.map(msg => {
            const parsed = JSON.parse(msg.value);
            return parsed.messageId;
        });
        await updateMessagesStatus(messageIds, 'sent');
    } catch (error) {
        console.error('Falha no reprocessamento das mensagens:', error);
        // Mensagens que falharam no reprocessamento vão para a dead letter queue
        await saveToDeadLetterQueue(messagesToRetry.map(msg => JSON.parse(msg.value)));
    }
}

/**
 * Carrega e reprocessa mensagens da fila de dead letter
 */
async function reprocessDeadLetterQueue() {
    try {
        // Verifica se o arquivo existe
        if (!fs.existsSync(DEAD_LETTER_FILE)) {
            return;
        }

        // Lê as mensagens
        const data = await readFileAsync(DEAD_LETTER_FILE, 'utf8');
        const deadLetterMessages = JSON.parse(data);

        if (deadLetterMessages.length === 0) {
            return;
        }

        console.log(`Reprocessando ${deadLetterMessages.length} mensagens da fila de dead letter`);

        // Limpa o arquivo da dead letter queue
        await writeFileAsync(DEAD_LETTER_FILE, JSON.stringify([], null, 2));

        // Prepara as mensagens e adiciona à fila de retry
        const kafkaMessages = prepareKafkaMessages(deadLetterMessages);
        retryQueue.push(...kafkaMessages);

        // Atualiza status no banco para 'retrying'
        const messageIds = deadLetterMessages.map(msg => msg.messageId);
        await updateMessagesStatus(messageIds, 'retrying');

        // Processa a fila de retry em lotes
        await processRetryQueue();

    } catch (error) {
        console.error('Erro ao reprocessar mensagens da fila de dead letter:', error);
    }
}

/**
 * Envia um lote de mensagens com mecanismo de retry
 * @param {Array} kafkaMessages - Mensagens formatadas para o Kafka
 * @param {number} retryCount - Contador de tentativas atuais
 */
async function sendBatchWithRetry(kafkaMessages, retryCount = 0) {
    try {
        const compressionType =
            COMPRESSION === 'snappy' ? CompressionTypes.Snappy :
                COMPRESSION === 'gzip' ? CompressionTypes.GZIP :
                    COMPRESSION === 'lz4' ? CompressionTypes.LZ4 :
                        CompressionTypes.None;

        await producer.send({
            topic: TOPIC,
            messages: kafkaMessages,
            compression: compressionType,
            acks: 1,
        });

        messagesSent += kafkaMessages.length;
        batchesSent++;

        // Se foi uma tentativa de retry bem-sucedida, registra
        if (retryCount > 0) {
            console.log(`Retry #${retryCount} bem-sucedido para ${kafkaMessages.length} mensagens`);
        }

        // Log de métricas a cada minuto
        const now = Date.now();
        if (now - lastReportTime > 60000) {
            const elapsedMinutes = (now - lastReportTime) / 60000;
            const rate = Math.round(messagesSent / elapsedMinutes);

            console.log(`Métricas: ${messagesSent} mensagens enviadas em ${batchesSent} lotes`);
            console.log(`Taxa de envio: ${rate} mensagens/minuto`);
            console.log(`Tentativas de retry: ${retryAttempts}, Falhas totais: ${failedMessages}`);

            // Resetar contadores para o próximo período
            messagesSent = 0;
            batchesSent = 0;
            retryAttempts = 0;
            lastReportTime = now;
        }
    } catch (error) {
        console.error(`Erro ao enviar lote (tentativa ${retryCount + 1}/${MAX_RETRIES + 1}):`, error.message);

        // Incrementa contador de tentativas para métricas
        retryAttempts++;

        // Verifica se ainda pode tentar novamente
        if (retryCount < MAX_RETRIES) {
            // Calcula atraso exponencial com jitter para evitar thundering herd
            const delay = RETRY_DELAY * Math.pow(2, retryCount) * (0.5 + Math.random() * 0.5);
            console.log(`Aguardando ${Math.round(delay)}ms antes de tentar novamente...`);

            await new Promise(resolve => setTimeout(resolve, delay));
            return sendBatchWithRetry(kafkaMessages, retryCount + 1);
        } else {
            console.error(`Falha após ${MAX_RETRIES + 1} tentativas. Enviando mensagens para a fila de dead letter.`);

            // Adiciona à fila de reprocessamento para tentativa posterior
            const originalMessages = kafkaMessages.map(msg => JSON.parse(msg.value));
            await saveToDeadLetterQueue(originalMessages);

            // Propaga o erro para ser tratado pelo chamador
            throw new Error(`Falha após ${MAX_RETRIES + 1} tentativas`);
        }
    }
}

/**
 * Envia um lote de mensagens para o Kafka
 * @param {Array} kafkaMessages
 */
async function sendBatch(kafkaMessages) {
    try {
        await sendBatchWithRetry(kafkaMessages, 0);
    } catch (error) {
        console.error('Todas as tentativas de envio falharam:', error.message);
    }
}

/**
 * Inicializa o servidor HTTP para receber payloads
 */
function startHTTPServer() {
    const server = http.createServer(async (req, res) => {
        if (req.method === 'POST' && req.url === '/rcs/send') {
            let body = '';

            req.on('data', chunk => {
                body += chunk.toString();
            });

            req.on('end', async () => {
                try {
                    console.log('Recebido payload para envio de RCS');

                    // Parse do payload JSON
                    const payload = JSON.parse(body);

                    // Verifica se temos um array de mensagens ou uma única mensagem
                    const messages = Array.isArray(payload) ? payload : [payload];

                    // Valida as mensagens
                    const validMessages = messages.filter(msg => {
                        // Verifica campos obrigatórios
                        return msg.phoneNumber && msg.message;
                    });

                    if (validMessages.length === 0) {
                        res.writeHead(400, { 'Content-Type': 'application/json' });
                        res.end(JSON.stringify({
                            success: false,
                            error: 'Payload inválido. É necessário fornecer phoneNumber e message.'
                        }));
                        return;
                    }

                    // Adiciona as mensagens à fila
                    const savedMessages = await addMessagesToPendingQueue(validMessages);

                    // Retorna resposta de sucesso
                    res.writeHead(202, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({
                        success: true,
                        message: `${validMessages.length} mensagens adicionadas à fila de processamento`,
                        messageIds: savedMessages.map(msg => msg.messageId)
                    }));

                } catch (error) {
                    console.error('Erro ao processar payload RCS:', error);
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({
                        success: false,
                        error: 'Erro interno ao processar mensagens'
                    }));
                }
            });
        } else if (req.method === 'GET' && req.url === '/health') {
            // Endpoint de health check
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                status: 'ok',
                queueSize: pendingMessages.length,
                retryQueueSize: retryQueue.length,
                uptime: process.uptime()
            }));
        } else {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Endpoint não encontrado' }));
        }
    });

    server.listen(HTTP_PORT, () => {
        console.log(`Servidor HTTP rodando na porta ${HTTP_PORT}`);
        console.log(`Endpoint para envio de mensagens: http://localhost:${HTTP_PORT}/rcs/send`);
        console.log(`Health check: http://localhost:${HTTP_PORT}/health`);
    });
}

/**
 * Processa mensagens em lotes contínuos (processamento periódico)
 */
function setupPeriodicProcessing() {
    // Processa a fila de retry periodicamente
    setInterval(async () => {
        if (retryQueue.length > 0) {
            await processRetryQueue();
        }
    }, 30000);

    // Reprocessa a dead letter periodicamente
    setInterval(async () => {
        await reprocessDeadLetterQueue();
    }, 120000);

    // Monitora e processa mensagens pendentes
    setInterval(() => {
        if (pendingMessages.length > 0 && !isProcessingBatch) {
            processPendingMessages();
        }
    }, 5000);
}

/**
 * Inicializa o produtor e começa o processamento
 */
async function start() {
    try {
        console.log('Inicializando produtor RCS...');

        // Conecta ao MongoDB
        await connectToDatabase();

        // Conecta ao Kafka
        console.log('Conectando ao Kafka...');
        await producer.connect();
        console.log('Conexão com Kafka estabelecida.');

        console.log(`Configuração de lotes: tamanho ${BATCH_SIZE}`);
        console.log(`Configuração de retentativas: ${MAX_RETRIES} tentativas, delay inicial: ${RETRY_DELAY}ms`);
        console.log(`Dead letter queue: ${DEAD_LETTER_FILE}`);

        // Inicia o servidor HTTP
        startHTTPServer();

        // Configura o processamento periódico
        setupPeriodicProcessing();

        // Tratamento de sinais para encerramento limpo
        process.on('SIGTERM', shutdown);
        process.on('SIGINT', shutdown);
    } catch (error) {
        console.error('Erro ao iniciar produtor:', error);
        process.exit(1);
    }
}

/**
 * Encerra o produtor de forma limpa
 */
async function shutdown() {
    console.log('Encerrando produtor...');
    try {
        // Salva mensagens pendentes para não perder
        if (pendingMessages.length > 0) {
            console.log(`Salvando ${pendingMessages.length} mensagens pendentes na dead letter queue`);
            await saveToDeadLetterQueue(pendingMessages);
        }

        // Salva mensagens da fila de retry
        if (retryQueue.length > 0) {
            console.log(`Salvando ${retryQueue.length} mensagens da fila de retry na dead letter queue`);
            const originalMessages = retryQueue.map(msg => JSON.parse(msg.value));
            await saveToDeadLetterQueue(originalMessages);
        }

        // Desconecta do Kafka
        await producer.disconnect();
        console.log('Produtor Kafka desconectado.');

        // Fecha a conexão com MongoDB
        if (mongoClient) {
            await mongoClient.close();
            console.log('Conexão com MongoDB fechada.');
        }

        process.exit(0);
    } catch (error) {
        console.error('Erro ao desconectar produtor:', error);
        process.exit(1);
    }
}

// Inicia o sistema
module.exports = {
    start,
    shutdown,
    addMessagesToPendingQueue
};

if (require.main === module) {
    start().catch(error => {
        console.error('Erro fatal no produtor RCS:', error);
        process.exit(1);
    });
}