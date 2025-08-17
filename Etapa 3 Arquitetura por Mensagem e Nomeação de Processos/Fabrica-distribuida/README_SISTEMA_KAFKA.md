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

## 🖥️ Interfaces de Monitoramento

### Kafka UI - Monitoramento Visual
- **URL**: http://localhost:8080
- **Funcionalidades**:
  - 📊 Dashboard com métricas em tempo real
  - 📝 Visualização de mensagens nos tópicos
  - 👥 Monitoramento de consumer groups
  - 🔍 Busca e filtros avançados
  - 📈 Gráficos de performance

### Controlador Central - Status do Sistema
- **URL**: http://localhost:8083
- **Funcionalidades**:
  - 📋 Estatísticas de produção
  - 🤖 Status dos robôs em tempo real
  - 🚗 Contador de veículos produzidos

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

| Serviço           | Porta Host | Porta Container | URL de Acesso          |
|------------------|------------|-----------------|------------------------|
| ZooKeeper        | 2181       | 2181           | localhost:2181         |
| Kafka            | 9092       | 9092           | localhost:9092         |
| **Kafka UI**     | **8080**   | **8080**       | **http://localhost:8080** |
| Controlador      | 8083       | 8080           | http://localhost:8083  |
| Estoque          | 8081       | 8081           | http://localhost:8081  |
| Monitor          | 8082       | 8082           | http://localhost:8082  |

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

## 🎯 **Como Funciona o Sistema Kafka - Explicação Didática**

### 📚 **1. CONCEITOS FUNDAMENTAIS**

**Apache Kafka** é um sistema de **streaming de eventos** que funciona como um "correio postal super eficiente":
- **Produtores** (senders) enviam mensagens para **tópicos** (caixas postais)
- **Consumidores** (receivers) leem mensagens dos tópicos
- **Brokers** (agências postais) gerenciam o armazenamento e entrega

### 🏗️ **2. ARQUITETURA DETALHADA DO SISTEMA**

```
┌─────────────────────────────────────────────────────────┐
│                    KAFKA CLUSTER                        │
├─────────────────────────────────────────────────────────┤
│  ZooKeeper (2181) ←→ Kafka Broker (9092)               │
│                                                         │
│  📬 TÓPICOS:                                           │
│  • ordens-producao   (comandos para criar veículos)    │
│  • comandos-robos    (tarefas específicas)             │
│  • status-robos      (estados dos robôs)               │
│                                                         │
│  🖥️ KAFKA UI (8080) - Interface de Monitoramento      │
└─────────────────────────────────────────────────────────┘
                              ↕️
                    ┌─────────────────┐
                    │ CONTROLADOR     │
                    │    CENTRAL      │
                    │   (PRODUCER)    │
                    └─────────────────┘
                              ↕️
        ┌─────────────┬─────────────┬─────────────┐
        │   ROBÔ      │    ROBÔ     │    ROBÔ     │
        │  SOLDAGEM   │   PINTURA   │  MONTAGEM   │
        │ (CONSUMER)  │ (CONSUMER)  │ (CONSUMER)  │
        └─────────────┴─────────────┴─────────────┘
```

### 🔄 **3. FLUXO DE MENSAGENS EM TEMPO REAL**

#### **PASSO 1: Controlador Cria Ordem de Produção**
```java
// ControladorCentralKafka.java
private void criarOrdemProducao() {
    Map<String, Object> ordem = new HashMap<>();
    ordem.put("veiculoId", proximoVeiculoId++);
    ordem.put("tipoInicial", "SOLDAGEM");
    ordem.put("timestamp", System.currentTimeMillis());
    
    // 📤 ENVIA para tópico "ordens-producao"
    enviarMensagem("ordens-producao", "NOVO_VEICULO", ordem);
}
```

#### **PASSO 2: Controlador Distribui Comando**
```java
// Quando um robô está disponível:
private void enviarComandoParaRobo(String tipoRobo, int veiculoId) {
    Map<String, Object> comando = new HashMap<>();
    comando.put("acao", "PROCESSAR");
    comando.put("veiculoId", veiculoId);
    
    // 📤 ENVIA para tópico "comandos-robos" com chave = tipo do robô
    enviarMensagem("comandos-robos", tipoRobo, comando);
}
```

#### **PASSO 3: Robô Recebe e Processa**
```java
// RoboKafka.java
private void consumirComandos() {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
    
    for (ConsumerRecord<String, String> record : records) {
        String tipoDestino = record.key();    // "SOLDAGEM", "PINTURA", etc.
        String comando = record.value();       // JSON com ação e veiculoId
        
        // ✅ SÓ PROCESSA se o comando for para este robô
        if (tipoRobo.equals(tipoDestino)) {
            processarComando(comando);
        }
    }
}
```

#### **PASSO 4: Robô Reporta Status**
```java
private void processarVeiculo(int veiculoId) {
    statusAtual = "OCUPADO";
    
    // 📤 NOTIFICA início do trabalho
    String mensagemInicio = criarMensagemStatus("OCUPADO", "Processando veículo " + veiculoId);
    enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagemInicio);
    
    // 🔧 FAZ O TRABALHO (simula tempo de processamento)
    Thread.sleep(obterTempoProcessamento());
    
    // 📤 NOTIFICA conclusão
    String mensagemConclusao = criarMensagemStatusConcluido(veiculoId);
    enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagemConclusao);
}
```

### 📊 **4. GRUPOS DE CONSUMIDORES (LOAD BALANCING)**

#### **Como Funciona:**
```yaml
Tópico: comandos-robos
├── Partition 0: [msg1, msg2, msg3...]
├── Partition 1: [msg4, msg5, msg6...]
└── Partition 2: [msg7, msg8, msg9...]

Consumer Groups:
├── robo-soldagem-group   (1 robô soldagem)
├── robo-pintura-group    (1 robô pintura)  
└── robo-montagem-group   (1 robô montagem)
```

**Vantagem:** Se você tivesse 3 robôs de soldagem, eles compartilhariam o trabalho automaticamente!

### 🎭 **5. TIPOS DE MENSAGENS TRAFEGANDO**

#### **📬 Tópico: ordens-producao**
```json
{
  "veiculoId": 42,
  "tipoInicial": "SOLDAGEM",
  "timestamp": 1692259200000
}
```

#### **📬 Tópico: comandos-robos**
```json
Key: "SOLDAGEM"
Value: {
  "acao": "PROCESSAR",
  "veiculoId": 42
}
```

#### **📬 Tópico: status-robos**
```json
Key: "SOLDAGEM"
Value: {
  "tipo": "SOLDAGEM",
  "status": "OCUPADO",
  "descricao": "Processando veículo 42",
  "timestamp": 1692259260000
}
```

### 🖥️ **6. INTERFACE DE MONITORAMENTO - KAFKA UI**

**Acesso:** http://localhost:8080

**O que você pode ver:**
- 📊 **Dashboard visual** com métricas do cluster
- 📝 **Visualização de mensagens** em tempo real
- 👥 **Monitoramento de consumer groups**
- 🔍 **Busca e filtros** por tópicos
- 📈 **Gráficos de throughput** e latência

### ⚡ **7. BENEFÍCIOS DO KAFKA**

#### **🔄 Assincronia Total**
- Robôs não precisam esperar resposta do controlador
- Sistema continua funcionando mesmo se um componente falha

#### **📈 Escalabilidade**
- Pode adicionar mais robôs sem alterar código
- Kafka distribui trabalho automaticamente

#### **💾 Durabilidade**
- Mensagens ficam armazenadas no disco
- Se um robô cair, ele pode recuperar mensagens perdidas

#### **🔍 Observabilidade**
- Kafka UI permite ver todo o fluxo em tempo real
- Fácil debug e monitoramento

### 🎯 **8. MULTITHREADING NO ROBÔ**

Cada robô executa **3 threads simultâneas**:

```java
public void iniciar() {
    // Thread 1: Consumir comandos do Kafka
    new Thread(this::consumirComandos, "ConsumeCommands").start();
    
    // Thread 2: Enviar status periódico (a cada 15s)
    new Thread(this::enviarStatusPeriodico, "SendStatus").start();
    
    // Thread 3: Thread principal (manter robô vivo)
    while (ativo) {
        Thread.sleep(1000);
    }
}
```

**Por que isso é importante?**
- ⚡ **Performance**: Robô pode processar e reportar status simultaneamente
- 🔄 **Responsividade**: Não bloqueia recebimento de novos comandos
- 📡 **Monitoramento**: Status enviado constantemente, independente do trabalho

### 🎮 **9. TEMPOS DE PROCESSAMENTO REALÍSTICOS**

```java
private int obterTempoProcessamento() {
    return switch (tipoRobo) {
        case "SOLDAGEM" -> ThreadLocalRandom.current().nextInt(3000, 7000);  // 3-7s
        case "PINTURA" -> ThreadLocalRandom.current().nextInt(4000, 8000);   // 4-8s  
        case "MONTAGEM" -> ThreadLocalRandom.current().nextInt(5000, 10000); // 5-10s
        default -> ThreadLocalRandom.current().nextInt(3000, 6000);
    };
}
```

**Simulação Realística:**
- 🔨 **Soldagem**: Processo mais rápido (estrutura base)
- 🎨 **Pintura**: Processo médio (secagem necessária)
- 🔧 **Montagem**: Processo mais longo (peças complexas)

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
