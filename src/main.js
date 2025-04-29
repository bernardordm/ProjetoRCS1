/**
 * RCS Message System - Arquivo principal
 * Configura e inicia o producer e/ou consumer conforme necessidade
 */

const fs = require('fs');
const path = require('path');
const os = require('os');

// Carrega variáveis de ambiente do arquivo .env se existir
try {
    const envPath = path.join(__dirname, '.env');
    if (fs.existsSync(envPath)) {
        console.log('Carregando configurações do arquivo .env');
        require('dotenv').config({ path: envPath });
    }
} catch (error) {
    console.warn('Aviso: Erro ao carregar arquivo .env:', error.message);
}

// Parse de argumentos de linha de comando
const args = process.argv.slice(2);
const flags = {};

for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--')) {
        const flag = arg.substring(2);
        if (i + 1 < args.length && !args[i + 1].startsWith('--')) {
            flags[flag] = args[i + 1];
            i++;
        } else {
            flags[flag] = true;
        }
    }
}

// Atualiza variáveis de ambiente baseado nos argumentos
if (flags.kafka) process.env.KAFKA_BROKERS = flags.kafka;
if (flags.topic) process.env.KAFKA_TOPIC = flags.topic;
if (flags.batch) process.env.BATCH_SIZE = flags.batch;
if (flags.compression) process.env.COMPRESSION = flags.compression;
if (flags.port) process.env.HTTP_PORT = flags.port;
if (flags.mongo) process.env.MONGODB_URI = flags.mongo;

// Exibe ajuda se solicitado
if (flags.help) {
    console.log(`
  RCS Message System - Sistema de alta performance para mensagens RCS

  Uso: node main.js [opções]

  Opções:
    --mode <producer|consumer|all>  Modo de execução (padrão: all)
    --kafka <brokers>               Lista de brokers Kafka (separados por vírgula)
    --topic <nome>                  Nome do tópico Kafka para enviar mensagens
    --batch <tamanho>               Tamanho do lote de mensagens
    --compression <tipo>            Algoritmo de compressão (snappy, gzip, lz4)
    --port <porta>                  Porta HTTP para o servidor REST
    --mongo <uri>                   URI de conexão com o MongoDB
    --help                          Exibe esta ajuda
  
  Exemplo:
    node main.js --mode producer --kafka localhost:9092 --batch 20000
  `);
    process.exit(0);
}

// Modo de operação do sistema
const mode = flags.mode || 'all';

// Configuração compartilhada (evita duplicação entre producer e consumer)
const sharedConfig = {
    kafkaBrokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'],
    kafkaTopic: process.env.KAFKA_TOPIC || 'rcs-messages',
    mongoDB: {
        uri: process.env.MONGODB_URI || 'mongodb://localhost:27017',
        dbName: process.env.DB_NAME || 'rcs_messages',
        collection: process.env.COLLECTION_NAME || 'messages'
    },
    clientId: process.env.CLIENT_ID || `rcs-system-${os.hostname()}`
};

// Exibe banner
console.log(`
╔═════════════════════════════════════════╗
║           RCS MESSAGE SYSTEM            ║
║    Sistema de envio de alta performance ║
╚═════════════════════════════════════════╝
`);

// Exibe configurações
console.log('Configurações:');
console.log(`- Modo: ${mode}`);
console.log(`- Kafka Brokers: ${sharedConfig.kafkaBrokers.join(',')}`);
console.log(`- Tópico: ${sharedConfig.kafkaTopic}`);
if (mode === 'producer' || mode === 'all') {
    console.log(`- Tamanho do Batch: ${process.env.BATCH_SIZE || '10000'}`);
    console.log(`- Compressão: ${process.env.COMPRESSION || 'snappy'}`);
    console.log(`- Porta HTTP: ${process.env.HTTP_PORT || '3000'}`);
}
console.log(`- MongoDB: ${sharedConfig.mongoDB.uri.replace(/:\/\/([^:]+):[^@]+@/, '://$1:****@') || 'mongodb://localhost:27017'}`);
console.log('');

// Tratamento global de exceções
process.on('uncaughtException', (error) => {
    console.error('\n[ERRO FATAL] Exceção não tratada:', error);
    console.error('O sistema pode estar em estado inconsistente');
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('\n[ALERTA] Promessa rejeitada não tratada:', reason);
});

// Inicia os componentes necessários
async function start() {
    try {
        if (mode === 'producer' || mode === 'all') {
            const producer = require('./producer');
            await producer.start();
            console.log('Producer RCS iniciado com sucesso');
        }
        
        if (mode === 'consumer' || mode === 'all') {
            const consumer = require('./consumer');
            await consumer.start();
            console.log('Consumer RCS iniciado com sucesso');
        }        
        const producer = require('./producer');
        const consumer = require('./consumer');

        console.log('\nSistema RCS em execução. Pressione Ctrl+C para encerrar.');
    } catch (error) {
        console.error('Erro ao iniciar sistema RCS:', error);
        process.exit(1);
    }
}

// Função de encerramento limpo
async function handleShutdown() {
    console.log('\nEncerrando sistema RCS...');

    // Array para armazenar as promessas de shutdown
    const shutdownPromises = [];

    if (mode === 'producer' || mode === 'all') {
        try {
            const producer = require('./producer');
            shutdownPromises.push(producer.shutdown());
        } catch (e) {
            console.error('Erro ao encerrar producer:', e);
        }
    }

    if (mode === 'consumer' || mode === 'all') {
        try {
            const consumer = require('./consumer');
            shutdownPromises.push(consumer.shutdown());
        } catch (e) {
            console.error('Erro ao encerrar consumer:', e);
        }
    }

    try {
        await Promise.all(shutdownPromises);
        console.log('Sistema RCS encerrado com sucesso');
        process.exit(0);
    } catch (error) {
        console.error('Erro durante o encerramento:', error);
        process.exit(1);
    }
}

// Tratamento de sinais para encerramento limpo
process.on('SIGTERM', handleShutdown);
process.on('SIGINT', handleShutdown);

// Inicia o sistema
start().catch(err => {
    console.error('Erro fatal durante inicialização:', err);
    process.exit(1);
});