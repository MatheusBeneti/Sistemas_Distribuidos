# Sistema de Fábrica Distribuída com Apache Kafka

## 📋 Visão Geral

Este projeto implementa um sistema de fábrica distribuída usando **Apache Kafka** como broker de mensagens para comunicação assíncrona entre os componentes. O sistema simula uma linha de produção de veículos com três estações de trabalho (SOLDAGEM, PINTURA e MONTAGEM) que processam ordens de produção de forma coordenada.

## 🏗️ Arquitetura do Sistema

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ZOOKEEPER     │    │     KAFKA       │    │  CONTROLADOR    │
│   (Coord.)      │◄──►│   (Broker)      │◄──►│   CENTRAL       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                    ┌─────────────────────────────────────────────────┐
                    │              TÓPICOS KAFKA                     │
                    │  • ordens-producao                             │
                    │  • status-robos                                │
                    │  • comandos-robos                              │
                    └─────────────────────────────────────────────────┘
                                │
                                ▼
            ┌───────────────┬───────────────┬───────────────┐
            │   ROBO        │   ROBO        │   ROBO        │
            │  SOLDAGEM     │  PINTURA      │  MONTAGEM     │
            └───────────────┴───────────────┴───────────────┘
```

## 🔧 Componentes Principais

### 1. Controlador Central (ControladorCentralKafka)
- **Porta**: 8083
- **Função**: Coordena o fluxo de produção
- **Responsabilidades**:
  - Cria ordens de produção automaticamente
  - Distribui veículos entre as estações
  - Monitora status dos robôs
  - Exibe estatísticas do sistema em tempo real

### 2. Robôs de Produção (RoboKafka)
- **Tipos**: SOLDAGEM, PINTURA, MONTAGEM
- **Função**: Processam veículos em suas respectivas especialidades
- **Comportamento**:
  - Consomem comandos do tópico `comandos-robos`
  - Simulam tempo de processamento
  - Reportam status via tópico `status-robos`

### 3. Apache Kafka
- **Broker de Mensagens**: Gerencia comunicação assíncrona
- **Tópicos**:
  - `ordens-producao`: Novas ordens criadas pelo controlador
  - `status-robos`: Status e resultados dos robôs
  - `comandos-robos`: Comandos enviados para os robôs

### 4. ZooKeeper
- **Coordenação**: Gerencia metadados do cluster Kafka

## 🚀 Como Executar

### Pré-requisitos
- Docker
- Docker Compose

### Executando o Sistema

1. **Navegue para o diretório do projeto**:
   ```bash
   cd "Etapa 3 Arquitetura por Mensagem e Nomeação de Processos/Fabrica-distribuida"
   ```

2. **Inicie todos os serviços**:
   ```bash
   docker-compose -f Docker-compose.yml up -d
   ```

3. **Verifique se todos os containers estão rodando**:
   ```bash
   docker ps
   ```

4. **Acompanhe os logs em tempo real**:
   
   **Controlador Central**:
   ```bash
   docker logs -f controlador-central
   ```
   
   **Robô de Soldagem**:
   ```bash
   docker logs -f robo-soldagem
   ```
   
   **Robô de Pintura**:
   ```bash
   docker logs -f robo-pintura
   ```
   
   **Robô de Montagem**:
   ```bash
   docker logs -f robo-montagem
   ```

5. **Para parar o sistema**:
   ```bash
   docker-compose -f Docker-compose.yml down
   ```

## 📊 Fluxo de Produção

1. **Geração de Ordens**: O controlador cria ordens de produção automaticamente
2. **Soldagem**: Primeiro estágio - robô processa estrutura do veículo
3. **Pintura**: Segundo estágio - robô aplica pintura no veículo
4. **Montagem**: Estágio final - robô finaliza montagem do veículo
5. **Conclusão**: Veículo é marcado como produzido

## 🔍 Monitoramento

### Logs do Controlador
```
🎯 Iniciando Controlador Central com Kafka
📝 Nova ordem criada: 1
🏭 Processando ordem: 1
🔨 [KAFKA] Ordem 1 enviada para SOLDAGEM
📨 [KAFKA] SOLDAGEM - CONCLUIDO: DISPONIVEL
➡️ [KAFKA] Veículo 1 enviado para PINTURA
=== STATUS DO SISTEMA (KAFKA) ===
🤖 SOLDAGEM: DISPONIVEL
🤖 PINTURA: OCUPADO
🤖 MONTAGEM: DISPONIVEL
🚗 Veículos produzidos: 0
📋 Ordens pendentes: 1
```

### Logs dos Robôs
```
🤖 [SOLDAGEM] Robô iniciado. Aguardando comandos...
🔧 [SOLDAGEM] Iniciando processamento do veículo 1
✅ [SOLDAGEM] Veículo 1 processado com sucesso!
```

## 📦 Dependências Java

### Maven Dependencies (pom.xml)
```xml
<!-- Apache Kafka Client -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.5.0</version>
</dependency>

<!-- Jackson para JSON -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-core</artifactId>
    <version>2.15.2</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
```

## 🌐 Portas Utilizadas

| Serviço           | Porta Host | Porta Container |
|------------------|------------|-----------------|
| ZooKeeper        | 2181       | 2181           |
| Kafka            | 9092       | 9092           |
| Controlador      | 8083       | 8080           |
| Estoque          | 8081       | 8081           |
| Monitor          | 8082       | 8082           |

## 🏗️ Estrutura de Mensagens

### Comando para Robô
```json
{
  "tipo": "PROCESSAR_VEICULO",
  "veiculoId": 1,
  "timestamp": "2025-08-17T14:30:00"
}
```

### Status do Robô
```json
{
  "tipoRobo": "SOLDAGEM",
  "status": "DISPONIVEL",
  "veiculoId": 1,
  "resultado": "CONCLUIDO"
}
```

## 🔧 Configurações do Kafka

### Tópicos Criados Automaticamente
- `ordens-producao:1:1` - 1 partição, 1 réplica
- `status-robos:1:1` - 1 partição, 1 réplica  
- `comandos-robos:1:1` - 1 partição, 1 réplica

### Configurações do Producer
- `bootstrap.servers`: kafka:9092
- `key.serializer`: StringSerializer
- `value.serializer`: StringSerializer
- `acks`: all (garantia de durabilidade)

### Configurações do Consumer
- `bootstrap.servers`: kafka:9092
- `key.deserializer`: StringDeserializer
- `value.deserializer`: StringDeserializer
- `group.id`: controlador-group / robo-group
- `auto.offset.reset`: earliest

## 🎯 Benefícios da Arquitetura Kafka

1. **Desacoplamento**: Componentes não precisam conhecer uns aos outros diretamente
2. **Escalabilidade**: Fácil adição de novos robôs e controladores
3. **Tolerância a Falhas**: Mensagens são persistidas e podem ser reprocessadas
4. **Assíncronia**: Processamento não-bloqueante entre componentes
5. **Rastreabilidade**: Histórico completo de mensagens trocadas
6. **Flexibilidade**: Fácil modificação do fluxo de produção

## 🛠️ Resolução de Problemas

### Container não inicia
```bash
docker logs <container-name>
```

### Kafka não conecta
1. Verifique se ZooKeeper está rodando
2. Confirme se as portas estão liberadas
3. Teste conectividade: `docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092`

### Robôs não recebem comandos
1. Verifique logs do Kafka
2. Confirme se os tópicos foram criados
3. Teste consumer: `docker exec -it kafka kafka-console-consumer.sh --topic comandos-robos --bootstrap-server localhost:9092`

## 📈 Próximos Passos

- [ ] Implementar balanceamento de carga entre robôs do mesmo tipo
- [ ] Adicionar métricas de performance
- [ ] Implementar interface web para monitoramento
- [ ] Adicionar persistência de dados
- [ ] Implementar dead letter queue para falhas
- [ ] Adicionar autenticação e autorização

---

**Desenvolvido como parte do projeto de Sistemas Distribuídos - Etapa 3: Arquitetura por Mensagem e Nomeação de Processos**
