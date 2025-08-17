# 🖥️ Monitor Visual da Fábrica Distribuída

## O que é o Monitor Visual?

O **Monitor Visual** é um serviço web em tempo real que oferece uma interface gráfica para acompanhar todo o funcionamento da fábrica distribuída. Ele consome mensagens do Kafka e apresenta informações de forma visual e intuitiva.

## 🎯 Funcionalidades

### 📊 Dashboard em Tempo Real
- **Métricas ao vivo**: Veículos produzidos, ordens processadas, robôs ativos
- **Status dos robôs**: Estado atual de cada robô (Soldagem, Pintura, Montagem)
- **Fluxo de produção**: Visualização do pipeline de produção
- **Log de eventos**: Histórico em tempo real das atividades

### 🔄 Atualização Automática
- Interface atualiza automaticamente a cada 5 segundos
- Consumo de mensagens Kafka em tempo real
- Indicador visual de status "ao vivo"

### 🎨 Interface Moderna
- Design responsivo e moderno
- Gradientes e efeitos visuais
- Ícones intuitivos para cada tipo de robô
- Cores indicativas de status (verde=disponível, laranja=ocupado, etc.)

## 🏗️ Arquitetura Técnica

### Backend Java
```java
MonitorSistema.java
├── Servidor HTTP (porta 3000)
├── Consumer Kafka (tópicos: status-robos, ordens-producao, etc.)
├── API REST endpoints (/api/status, /api/eventos, /api/metricas)
└── Geração dinâmica de HTML
```

### Integração Kafka
O monitor consome mensagens dos seguintes tópicos:
- **status-robos**: Status e atividades dos robôs
- **ordens-producao**: Novas ordens de produção
- **producao-concluida**: Veículos finalizados
- **comandos-robos**: Comandos enviados aos robôs

### Frontend Dinâmico
- HTML5 + CSS3 + JavaScript
- Atualização automática via refresh
- Responsivo para diferentes tamanhos de tela
- Efeitos visuais e animações

## 🚀 Como Usar

### 1. Iniciar o Sistema
```powershell
# Testar apenas o monitor
.\teste-monitor.ps1

# Ou iniciar o sistema completo
docker-compose -f Docker-compose.yml up -d
```

### 2. Acessar as Interfaces
- **Monitor Visual**: http://localhost:3000
- **Kafka UI**: http://localhost:8080

### 3. Funcionalidades Disponíveis

#### 📈 Métricas em Tempo Real
- Contador de veículos produzidos
- Número de ordens processadas  
- Quantidade de robôs ativos
- Status geral do sistema

#### 🤖 Status dos Robôs
Para cada robô, você vê:
- **Tipo**: Soldagem 🔥, Pintura 🎨, Montagem 🔧
- **Status atual**: Disponível, Processando, Ocupado
- **Veículo atual**: ID do veículo sendo processado
- **Última atualização**: Timestamp da última atividade

#### 🔄 Fluxo de Produção Visual
Visualização do pipeline:
```
📝 Ordem → 🔥 Soldagem → 🎨 Pintura → 🔧 Montagem → ✅ Concluído
```

#### 📋 Log de Eventos
Histórico cronológico mostrando:
- Novos veículos iniciados
- Robôs mudando de status
- Mensagens Kafka processadas
- Veículos concluídos

## 🔧 Configuração e Customização

### Porta do Servidor
```java
private static final int PORTA_WEB = 3000;
```

### Intervalo de Atualização
```javascript
setInterval(() => { window.location.reload(); }, 5000); // 5 segundos
```

### Tópicos Kafka Monitorados
```java
consumer.subscribe(Arrays.asList(
    "status-robos", 
    "ordens-producao", 
    "producao-concluida", 
    "comandos-robos"
));
```

## 🎨 Personalização Visual

### Cores do Sistema
- **Verde (#00ff88)**: Status positivo, métricas, disponível
- **Laranja (#ffa500)**: Processando, ocupado
- **Azul (#4dabf7)**: Conectado, ativo
- **Vermelho (#ff6b6b)**: Erro, indisponível

### Ícones dos Robôs
- 🔥 **Soldagem**: Representa o processo de soldagem
- 🎨 **Pintura**: Representa o processo de pintura  
- 🔧 **Montagem**: Representa o processo de montagem

## 📡 API REST

O monitor expõe endpoints REST para integração:

### GET /api/status
Retorna status atual de todos os robôs
```json
{
  "SOLDAGEM": {
    "tipo": "SOLDAGEM",
    "status": "PROCESSANDO", 
    "veiculoAtual": 1234,
    "ultimaAtualizacao": "2025-08-17T14:30:25"
  }
}
```

### GET /api/eventos  
Retorna lista dos eventos recentes
```json
[
  {
    "tipo": "PRODUÇÃO",
    "descricao": "Veículo #1234 concluído",
    "timestamp": "2025-08-17T14:30:25"
  }
]
```

### GET /api/metricas
Retorna métricas gerais do sistema
```json
{
  "veiculosConcluidos": 15,
  "ordensProcessadas": 20,
  "robotsAtivos": 3,
  "ultimaAtualizacao": "2025-08-17T14:30:25"
}
```

## 🛠️ Desenvolvimento e Extensões

### Adicionar Novos Tipos de Robô
1. Atualizar `getRobotIcon()` para novo ícone
2. Adicionar lógica no `processarStatusRobo()`
3. Incluir no CSS as novas classes de status

### Adicionar Novas Métricas
1. Criar novos `AtomicInteger` para contadores
2. Atualizar o endpoint `/api/metricas`
3. Adicionar cards no dashboard HTML

### Melhorar a Interface
1. Editar o método `gerarHTMLDashboard()`
2. Adicionar novos estilos CSS
3. Incluir JavaScript para interatividade

## 🔍 Monitoramento e Debug

### Logs do Monitor
```bash
docker logs -f monitor-sistema
```

### Verificar Conectividade Kafka
```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Testar Endpoints API
```bash
curl http://localhost:3000/api/status
curl http://localhost:3000/api/metricas
```

## 🎉 Benefícios do Monitor Visual

1. **Visibilidade Total**: Veja tudo que está acontecendo na fábrica
2. **Tempo Real**: Informações atualizadas instantaneamente
3. **Fácil Diagnóstico**: Identifique problemas rapidamente
4. **Interface Intuitiva**: Não precisa conhecer Kafka para usar
5. **Histórico de Eventos**: Rastreie atividades passadas
6. **Métricas de Performance**: Acompanhe produtividade
7. **Status Detalhado**: Saiba exatamente o que cada robô está fazendo
8. **Multiplataforma**: Acesse de qualquer navegador

O Monitor Visual transforma dados técnicos do Kafka em uma experiência visual rica e informativa! 🚀
