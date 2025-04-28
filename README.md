# RCS Message Producer

Sistema de alta performance para envio massivo de mensagens RCS usando Node.js e Kafka, com capacidade para processar mais de 10 milhões de mensagens por dia.

## Características

- Processamento em lotes (batches) para máxima performance
- Sistema robusto de retry com backoff exponencial
- Fila de reprocessamento em memória
- Dead letter queue persistente
- Compressão de mensagens configurable (snappy, gzip, lz4)
- Métricas de desempenho em tempo real

## Requisitos

- Node.js 14+
- Kafka 2.0+

## Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/rcs-message-producer.git

# Entre na pasta do projeto
cd rcs-message-producer

# Instale as dependências
npm install
```

## Configuração

O sistema pode ser configurado através de variáveis de ambiente:

| Variável | Descrição | Valor Padrão |
|----------|-----------|--------------|
| KAFKA_BROKERS | Lista de brokers Kafka separados por vírgula | localhost:9092 |
| KAFKA_TOPIC | Tópico Kafka para envio das mensagens | rcs-messages |
| BATCH_SIZE | Tamanho do lote de mensagens | 10000 |
| COMPRESSION | Algoritmo de compressão (snappy, gzip, lz4) | snappy |
| MAX_RETRIES | Número máximo de tentativas de envio | 3 |
| RETRY_DELAY | Delay inicial entre tentativas (ms) | 1000 |
| DEAD_LETTER_FILE | Caminho para o arquivo de dead letter | ./dead_letter_queue.json |

## Uso

```bash
# Iniciar o produtor com configuração padrão
node src/producer.js

# Configurando via variáveis de ambiente
KAFKA_BROKERS=kafka1:9092,kafka2:9092 BATCH_SIZE=20000 node src/producer.js
```

## Performance

Para máxima performance, recomendamos:

- Ajustar o BATCH_SIZE de acordo com o tamanho das mensagens (10-20K)
- Usar compressão 'snappy' para melhor balanceamento entre CPU e rede
- Executar múltiplas instâncias em nós separados para escalar horizontalmente
- Monitorar uso de memória e CPU para otimizar os parâmetros

