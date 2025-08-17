# Sistema de Fábrica Distribuída com Apache Kafka

## 📋 Visão Geral

Este projeto implementa um sistema de fábrica distribuída usando **Apache Kafka** como broker de mensagens para comunicação assíncrona entre os componentes. O sistema simula uma linha de produção de veículos com três estações de trabalho (SOLDAGEM, PINTURA e MONTAGEM) que processam ordens de produção de forma coordenada.

## 🚀 Início Rápido

### Para Windows (PowerShell):
```powershell
.\iniciar-fabrica.ps1
```

### Para Linux/macOS (Bash):
```bash
./iniciar-fabrica.sh
```

### Para parar o sistema:
```powershell
.\parar-fabrica.ps1
```

**O script automaticamente:**
- ✅ Verifica se o Docker está rodando
- 🔨 Compila todas as imagens
- 🚀 Inicia todos os serviços na ordem correta  
- 🌐 Abre o monitor visual no navegador
- 📋 Exibe todas as interfaces disponíveis

## 🌐 Interfaces Disponíveis

| Serviço | URL | Descrição |
|---------|-----|-----------|
| 📈 **Monitor da Fábrica** | http://localhost:8082 | Dashboard visual em tempo real |
| 🔍 **Kafka UI** | http://localhost:8080 | Interface para visualizar mensagens |
| 📦 **Estoque** | http://localhost:8081 | API do gerenciador de estoque |
| 🎛️ **Controlador** | http://localhost:8083 | API do controlador central |

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

## 📈 **Análise de Escalabilidade da Arquitetura**

### 🚀 **ESCALABILIDADE HORIZONTAL - CENÁRIOS PRÁTICOS**

#### **Cenário 1: Adicionando Múltiplos Robôs do Mesmo Tipo**

**Situação Atual:**
```yaml
Robôs Atuais:
├── 1x SOLDAGEM
├── 1x PINTURA  
└── 1x MONTAGEM
```

**Cenário Escalado:**
```yaml
Robôs Escalados:
├── 3x SOLDAGEM (robo-soldagem-1, robo-soldagem-2, robo-soldagem-3)
├── 2x PINTURA  (robo-pintura-1, robo-pintura-2)
└── 4x MONTAGEM (robo-montagem-1, robo-montagem-2, robo-montagem-3, robo-montagem-4)
```

#### **🔧 Como Implementar Escalabilidade**

**1. Modificação no Docker-Compose:**
```yaml
# Docker-compose-escalado.yml
services:
  # 3 Robôs de Soldagem
  robo-soldagem-1:
    build: ./robo
    container_name: robo-soldagem-1
    command: java -cp target/classes:target/dependency/* org.example.RoboKafka SOLDAGEM
    depends_on: [kafka]
    networks: [fabrica-net]

  robo-soldagem-2:
    build: ./robo
    container_name: robo-soldagem-2
    command: java -cp target/classes:target/dependency/* org.example.RoboKafka SOLDAGEM
    depends_on: [kafka]
    networks: [fabrica-net]

  robo-soldagem-3:
    build: ./robo
    container_name: robo-soldagem-3
    command: java -cp target/classes:target/dependency/* org.example.RoboKafka SOLDAGEM
    depends_on: [kafka]
    networks: [fabrica-net]

  # 2 Robôs de Pintura
  robo-pintura-1:
    build: ./robo
    container_name: robo-pintura-1
    command: java -cp target/classes:target/dependency/* org.example.RoboKafka PINTURA
    depends_on: [kafka]
    networks: [fabrica-net]

  robo-pintura-2:
    build: ./robo
    container_name: robo-pintura-2
    command: java -cp target/classes:target/dependency/* org.example.RoboKafka PINTURA
    depends_on: [kafka]
    networks: [fabrica-net]
```

**2. Auto-Scaling com Docker Compose:**
```bash
# Escalar automaticamente (3 robôs soldagem, 2 pintura, 4 montagem)
docker-compose up -d --scale robo-soldagem=3 --scale robo-pintura=2 --scale robo-montagem=4
```

#### **⚡ Como o Kafka Gerencia a Escalabilidade**

**1. Load Balancing Automático:**
```java
// Todos os robôs SOLDAGEM pertencem ao mesmo Consumer Group
consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "robo-soldagem-group");

// Kafka distribui mensagens automaticamente entre:
// robo-soldagem-1, robo-soldagem-2, robo-soldagem-3
```

**2. Distribuição de Partições:**
```yaml
Tópico: comandos-robos (3 partições)
├── Partition 0 → robo-soldagem-1
├── Partition 1 → robo-soldagem-2  
└── Partition 2 → robo-soldagem-3

# Cada robô processa mensagens de sua partição
# Trabalho distribuído automaticamente!
```

**3. Tolerância a Falhas:**
```yaml
Cenário de Falha:
├── robo-soldagem-1: ❌ FALHOU
├── robo-soldagem-2: ✅ ATIVO
└── robo-soldagem-3: ✅ ATIVO

Resultado:
# Kafka redistribui partições automaticamente
├── Partition 0 → robo-soldagem-2
├── Partition 1 → robo-soldagem-2  
└── Partition 2 → robo-soldagem-3
```

### 📊 **Performance em Diferentes Cenários**

#### **Teste de Throughput:**

**Cenário Base (3 robôs):**
```
Pipeline: SOLDAGEM(3-7s) → PINTURA(4-8s) → MONTAGEM(5-10s)
Throughput: ~6 veículos/minuto
Gargalo: MONTAGEM (processo mais longo)
```

**Cenário Escalado (9 robôs):**
```
3x SOLDAGEM: 3 veículos processando simultaneamente
2x PINTURA:  2 veículos processando simultaneamente  
4x MONTAGEM: 4 veículos processando simultaneamente

Throughput: ~18-24 veículos/minuto
Melhoria: 3-4x mais eficiente!
```

#### **Análise de Gargalos:**

**1. Identificação de Bottlenecks:**
```java
// No Kafka UI você pode ver:
Consumer Lag por grupo:
├── robo-soldagem-group: 0 mensagens pendentes ✅
├── robo-pintura-group:  15 mensagens pendentes ⚠️  
└── robo-montagem-group: 0 mensagens pendentes ✅

// PINTURA é o gargalo! Precisa de mais robôs.
```

**2. Balanceamento Inteligente:**
```yaml
Configuração Otimizada:
├── 2x SOLDAGEM (rápido, menos robôs necessários)
├── 4x PINTURA  (gargalo, mais robôs necessários)
└── 3x MONTAGEM (médio, robôs moderados)
```

### 🌐 **Escalabilidade de Infraestrutura**

#### **1. Configuração de Partições Kafka:**
```yaml
# Para suportar mais robôs, aumentar partições:
KAFKA_CREATE_TOPICS: "comandos-robos:10:3,status-robos:5:3,ordens-producao:3:3"

# 10 partições = até 10 robôs por tipo processando simultaneamente
# 3 réplicas = tolerância a falhas
```

#### **2. Monitoramento Escalável:**
```yaml
Kafka UI Dashboard mostra:
├── 📊 Throughput por tópico
├── 👥 Consumer groups ativos  
├── 📈 Latência de mensagens
├── ⚠️  Consumer lag por grupo
└── 💾 Utilização de partições
```

### 🎯 **Vantagens da Arquitetura Kafka para Escalabilidade**

#### **1. ✅ Escalabilidade Horizontal Nativa**
- **Zero Código Alterado**: Mesmo JAR roda em N instâncias
- **Auto-Discovery**: Robôs se registram automaticamente
- **Load Balancing**: Kafka distribui trabalho automaticamente

#### **2. ✅ Elasticidade Dinâmica**
```bash
# Aumentar robôs em tempo real (sem parar sistema):
docker-compose up -d --scale robo-soldagem=5

# Diminuir robôs:
docker-compose up -d --scale robo-soldagem=1
```

#### **3. ✅ Observabilidade Completa**
- **Kafka UI**: Monitora toda a pipeline em tempo real
- **Consumer Lag**: Identifica gargalos automaticamente
- **Metrics**: Performance de cada tipo de robô

#### **4. ✅ Tolerância a Falhas**
- **Rebalancing**: Kafka redistribui trabalho automaticamente
- **Persistência**: Mensagens não se perdem
- **Recovery**: Robôs recuperam trabalho pendente ao reiniciar

### 🚀 **Exemplo Prático de Comando de Escalabilidade**

```bash
# Terminal 1: Iniciar sistema escalado
docker-compose up -d --scale robo-soldagem=3 --scale robo-pintura=2 --scale robo-montagem=4

# Terminal 2: Monitorar performance
docker logs -f controlador-central

# Terminal 3: Verificar distribuição de trabalho
curl http://localhost:8080  # Kafka UI

# Terminal 4: Métricas em tempo real
watch -n 2 'docker ps --format "table {{.Names}}\t{{.Status}}"'
```

### 📈 **Limites e Considerações**

#### **1. Limites Práticos:**
- **Partições Kafka**: Máximo de robôs = número de partições
- **Recursos Hardware**: CPU/RAM do host
- **Network Throughput**: Largura de banda disponível

#### **2. Otimizações Recomendadas:**
```yaml
Para Produção:
├── Kafka Cluster: 3+ brokers (alta disponibilidade)
├── Partições: 2x número máximo de robôs esperados
├── Monitoring: Prometheus + Grafana
└── Load Balancer: NGINX para interfaces web
```

### 🏆 **Resultado da Análise**

**✅ Pontos Fortes:**
- Escalabilidade horizontal nativa
- Zero alteração de código necessária
- Load balancing automático
- Tolerância a falhas integrada
- Observabilidade completa

**⚠️ Pontos de Atenção:**
- Configurar partições adequadamente
- Monitorar consumer lag
- Considerar recursos de hardware


## 📈 Próximos Passos


---

**Desenvolvido como parte do projeto de Sistemas Distribuídos - Etapa 3: Arquitetura por Mensagem e Nomeação de Processos**
