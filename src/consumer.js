/**
 * RCS Message Consumer
 * Responsável pelo processamento das mensagens RCS do Kafka
 */

const { Kafka, logLevel } = require('kafkajs');
const { MongoClient } = require('mongodb');
const os = require('os');

// Configurações
const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'];
const TOPIC = process.env.KAFKA_TOPIC || 'rcs-messages';
const CONSUMER_GROUP = process.env.CONSUMER_GROUP || `rcs-consumer-group`;
const CLIENT_ID = process.env.CONSUMER_CLIENT_ID || `rcs-consumer-${os.hostname()}`;
const MAX_BATCH_SIZE = parseInt(process.env.CONSUMER_BATCH_SIZE || '100', 10);
const MAX_PARALLEL_MESSAGES = parseInt(process.env.MAX_PARALLEL_MESSAGES || '10', 10);

// MongoDB config
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'rcs_messages';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'messages';

// Variáveis globais
let mongoClient;
let messagesCollection;
let consumer;
let isShuttingDown = false;
let processingCount = 0;
let messagesProcessed = 0;
let lastReportTime = Date.now();
let processingMessageIds = new Set();

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

        // Verifica se os índices existem - o producer já deve ter criado
        const indices = await messagesCollection.listIndexes().toArray();
        const hasMessageIdIndex = indices.some(index => index.name === 'messageId_1');
        
        if (!hasMessageIdIndex) {
            console.log('Criando índices necessários...');
            await messagesCollection.createIndex({ messageId: 1 }, { unique: true });
            await messagesCollection.createIndex({ status: 1 });
        }

        return true;
    } catch (error) {
        console.error('Erro ao conectar ao MongoDB:', error);
        return false;
    }
}

/**
 * Cria o cliente Kafka e consumer
 */
function createKafkaConsumer() {
    const kafka = new Kafka({
        clientId: CLIENT_ID,
        brokers: KAFKA_BROKERS,
        retry: {
            initialRetryTime: 300,
            retries: 10,
            maxRetryTime: 30000,
        },
        logLevel: logLevel.ERROR,
    });

    consumer = kafka.consumer({ 
        groupId: CONSUMER_GROUP,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxBytesPerPartition: 10485760, // 10MB
        maxWaitTimeInMs: 5000,
    });
}

/**
 * Processa uma mensagem RCS
 * @param {Object} message - Mensagem do Kafka
 */
async function processMessage(message) {
    try {
        // Extrai o messageId do header
        const messageId = message.headers['message-id']?.toString();
        
        if (!messageId) {
            console.warn('Mensagem sem messageId recebida, ignorando');
            return;
        }
        
        // Evita processar duplicatas
        if (processingMessageIds.has(messageId)) {
            console.warn(`Mensagem ${messageId} já está sendo processada, ignorando`);
            return;
        }
        
        processingMessageIds.add(messageId);
        processingCount++;
        
        // Analisa o payload da mensagem
        const messageData = JSON.parse(message.value.toString());
        
        // Atualiza o status no banco de dados para 'processing'
        if (messagesCollection) {
            await messagesCollection.updateOne(
                { messageId },
                { 
                    $set: { 
                        status: 'processing',
                        processing_started: new Date()
                    } 
                }
            );
        }
        
        // Aqui você implementaria o processamento real da mensagem RCS
        // Por exemplo, enviar para API externa, processar conteúdo, etc.
        
        // Simulação de processamento
        await new Promise(resolve => setTimeout(resolve, 50 + Math.random() * 100));
        
        // Atualiza o status para 'delivered' no banco de dados
        if (messagesCollection) {
            await messagesCollection.updateOne(
                { messageId },
                { 
                    $set: { 
                        status: 'delivered',
                        processed_at: new Date()
                    } 
                }
            );
        }
        
        // Incrementa contador e remove da lista de processamento
        messagesProcessed++;
        processingMessageIds.delete(messageId);
        processingCount--;
        
        // Log de métricas a cada minuto
        const now = Date.now();
        if (now - lastReportTime > 60000) {
            const elapsedMinutes = (now - lastReportTime) / 60000;
            const rate = Math.round(messagesProcessed / elapsedMinutes);
            
            console.log(`Métricas do consumer: ${messagesProcessed} mensagens processadas`);
            console.log(`Taxa de processamento: ${rate} mensagens/minuto`);
            console.log(`Mensagens em processamento: ${processingCount}`);
            
            // Reseta contadores
            messagesProcessed = 0;
            lastReportTime = now;
        }
        
    } catch (error) {
        console.error('Erro ao processar mensagem:', error);
        processingCount--;
        
        if (messageId) {
            processingMessageIds.delete(messageId);
            
            // Marca como falha no banco de dados
            try {
                if (messagesCollection) {
                    await messagesCollection.updateOne(
                        { messageId },
                        { 
                            $set: { 
                                status: 'failed',
                                error: error.message,
                                failed_at: new Date()
                            } 
                        }
                    );
                }
            } catch (dbError) {
                console.error('Erro ao atualizar status da mensagem falha:', dbError);
            }
        }
    }
}

/**
 * Inicia o consumer RCS
 */
async function start() {
    try {
        console.log('Inicializando consumer RCS...');
        
        // Conecta ao MongoDB
        await connectToDatabase();
        
        // Cria e conecta o consumer Kafka
        createKafkaConsumer();
        
        console.log('Conectando ao Kafka...');
        await consumer.connect();
        console.log('Conexão com Kafka estabelecida');
        
        // Subscreve no tópico
        await consumer.subscribe({ 
            topic: TOPIC,
            fromBeginning: false // Processa apenas novas mensagens
        });
        
        console.log(`Iniciando consumo de mensagens do tópico: ${TOPIC}`);
        console.log(`Configuração: batch size ${MAX_BATCH_SIZE}, processamento paralelo: ${MAX_PARALLEL_MESSAGES}`);
        
        // Inicia o processamento de mensagens
        await consumer.run({
            partitionsConsumedConcurrently: 3,
            eachBatchAutoResolve: true,
            autoCommitInterval: 5000,
            autoCommitThreshold: 100,
            
            eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
                // Controla o consumo em lotes
                const messages = batch.messages;
                console.log(`Recebido lote de ${messages.length} mensagens da partição ${batch.partition}`);
                
                // Lista de promessas para processamento em paralelo mas controlado
                const processingPromises = [];
                
                for (let i = 0; i < messages.length; i++) {
                    const message = messages[i];
                    
                    // Espera se já tem muitas mensagens em processamento
                    while (processingCount >= MAX_PARALLEL_MESSAGES && !isShuttingDown) {
                        await new Promise(resolve => setTimeout(resolve, 50));
                        await heartbeat();
                    }
                    
                    // Se está encerrando, para de processar
                    if (isShuttingDown) break;
                    
                    // Processa a mensagem e armazena a promessa
                    processingPromises.push(processMessage(message));
                    
                    // Commit do offset após cada mensagem para evitar reprocessamento
                    resolveOffset(message.offset);
                    
                    // Heartbeat periódico para manter a conexão
                    if (i % 10 === 0) await heartbeat();
                }
                
                // Aguarda todas as promessas concluírem
                await Promise.all(processingPromises);
                
                // Heartbeat final
                await heartbeat();
            }
        });
        
        console.log('Consumer iniciado e aguardando mensagens');
        
    } catch (error) {
        console.error('Erro ao iniciar consumer:', error);
        throw error;
    }
}

/**
 * Encerra o consumer de forma limpa
 */
async function shutdown() {
    console.log('Encerrando consumer RCS...');
    isShuttingDown = true;
    
    try {
        // Aguarda o processamento atual terminar (com timeout)
        const maxWaitTime = 10000; // 10 segundos
        const startTime = Date.now();
        
        while (processingCount > 0) {
            console.log(`Aguardando ${processingCount} mensagens em processamento terminarem...`);
            
            // Timeout de segurança
            if (Date.now() - startTime > maxWaitTime) {
                console.warn('Timeout de espera atingido. Forçando encerramento.');
                break;
            }
            
            await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        // Desconecta o consumer Kafka
        if (consumer) {
            await consumer.disconnect();
            console.log('Consumer Kafka desconectado');
        }
        
        // Fecha a conexão com MongoDB
        if (mongoClient) {
            await mongoClient.close();
            console.log('Conexão com MongoDB fechada');
        }
        
        console.log('Consumer RCS encerrado com sucesso');
    } catch (error) {
        console.error('Erro ao encerrar consumer:', error);
        throw error;
    }
}

// Exporta as funções do módulo
module.exports = {
    start,
    shutdown
};

// Inicia automaticamente se for executado diretamente
if (require.main === module) {
    start().catch(error => {
        console.error('Erro fatal no consumer RCS:', error);
        process.exit(1);
    });
}