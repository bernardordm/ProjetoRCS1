const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const os = require('os');
const fs = require('fs');
const { promisify } = require('util');
const path = require('path');

// Configurações
const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'];
const TOPIC = process.env.KAFKA_TOPIC || 'rcs-messages';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10000', 10);
const COMPRESSION = process.env.COMPRESSION || 'snappy'; // snappy, gzip, lz4
const CLIENT_ID = process.env.CLIENT_ID || `rcs-producer-${os.hostname()}`;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY || '1000', 10);
const DEAD_LETTER_FILE = process.env.DEAD_LETTER_FILE || './dead_letter_queue.json';

// Métricas
let messagesSent = 0;
let batchesSent = 0;
let retryAttempts = 0;
let failedMessages = 0;
let lastReportTime = Date.now();

// Fila de reprocessamento (in-memory)
let retryQueue = [];
const writeFileAsync = promisify(fs.writeFile);
const readFileAsync = promisify(fs.readFile);

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

// Produtor com configurações avançadas
const producer = kafka.producer({
    allowAutoTopicCreation: true,
    transactionTimeout: 30000,
    idempotent: true,
    maxInFlightRequests: 5,
});

/**
 * Carrega mensagens RCS de uma fonte (arquivo, DB, etc)
 * @param {number} batchSize - Tamanho do lote a ser carregado
 * @returns {Promise<Array>} - Array de mensagens para processar
 */
async function loadMessages(batchSize) {
    // Implemente sua lógica de carregamento (ex: arquivo, DB, API)
    // Este é apenas um exemplo; substitua com sua fonte de dados real
    return Array(batchSize).fill().map((_, index) => ({
        phoneNumber: `+55119${Math.floor(10000000 + Math.random() * 90000000)}`,
        message: `Mensagem RCS de teste ${Date.now()}-${index}`,
        timestamp: Date.now(),
        messageId: `msg-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`,
        // Outros campos específicos do RCS podem ser adicionados aqui
    }));
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
            console.log('Nenhuma mensagem na fila de dead letter para reprocessar');
            return;
        }

        // Lê as mensagens
        const data = await readFileAsync(DEAD_LETTER_FILE, 'utf8');
        const deadLetterMessages = JSON.parse(data);

        if (deadLetterMessages.length === 0) {
            console.log('Nenhuma mensagem na fila de dead letter para reprocessar');
            return;
        }

        console.log(`Reprocessando ${deadLetterMessages.length} mensagens da fila de dead letter`);

        // Limpa o arquivo da dead letter queue
        await writeFileAsync(DEAD_LETTER_FILE, JSON.stringify([], null, 2));

        // Prepara as mensagens e adiciona à fila de retry
        const kafkaMessages = prepareKafkaMessages(deadLetterMessages);
        retryQueue.push(...kafkaMessages);

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
 * Processa mensagens em lotes contínuos
 */
async function processBatchesLoop() {
    while (true) {
        try {
            if (retryQueue.length > 0) {
                await processRetryQueue();
            }

            const messages = await loadMessages(BATCH_SIZE);
            if (messages.length === 0) {
                console.log('Sem novas mensagens para processar. Verificando dead letter queue...');

                await reprocessDeadLetterQueue();

                await new Promise(resolve => setTimeout(resolve, 5000));
                continue;
            }

            const kafkaMessages = prepareKafkaMessages(messages);
            await sendBatch(kafkaMessages);

            await new Promise(resolve => setImmediate(resolve));
        } catch (error) {
            console.error('Erro no loop de processamento:', error);
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
}

/**
 * Inicializa o produtor e começa o processamento
 */
async function start() {
    try {
        console.log('Conectando ao Kafka...');
        await producer.connect();
        console.log('Conexão estabelecida.');

        console.log(`Iniciando processamento de lotes (tamanho: ${BATCH_SIZE})...`);
        console.log(`Configuração de retentativas: ${MAX_RETRIES} tentativas, delay inicial: ${RETRY_DELAY}ms`);
        console.log(`Dead letter queue: ${DEAD_LETTER_FILE}`);

        processBatchesLoop();

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
        if (retryQueue.length > 0) {
            console.log(`Salvando ${retryQueue.length} mensagens da fila de retry na dead letter queue`);
            const originalMessages = retryQueue.map(msg => JSON.parse(msg.value));
            await saveToDeadLetterQueue(originalMessages);
        }

        await producer.disconnect();
        console.log('Produtor desconectado.');
        process.exit(0);
    } catch (error) {
        console.error('Erro ao desconectar produtor:', error);
        process.exit(1);
    }
}

// Inicia o sistema
start().catch(console.error);