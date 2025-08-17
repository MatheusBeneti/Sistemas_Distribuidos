# Sistema de Fábrica Distribuída com Apache Kafka

## Visão Geral

Este projeto implementa um sistema de fábrica distribuída utilizando Apache Kafka para comunicação assíncrona entre os componentes. O sistema simula uma linha de produção de veículos com três estações: Soldagem, Pintura e Montagem.

## Arquitetura com Kafka

### Componentes Principais

1. **Controlador Central (ControladorCentralKafka)**: Coordena a produção e gerencia o fluxo de trabalho
2. **Robôs (RoboKafka)**: Executam as operações de produção (Soldagem, Pintura, Montagem)
3. **Apache Kafka**: Sistema de mensageria para comunicação assíncrona
4. **ZooKeeper**: Gerenciamento de configuração do Kafka

### Tópicos Kafka

- `ordens-producao`: Ordens de produção criadas pelo controlador
- `status-robos`: Status e atualizações dos robôs
- `comandos-robos`: Comandos enviados para os robôs
- `producao-concluida`: Notificações de veículos finalizados

## Principais Melhorias

### 1. Comunicação Assíncrona
- **Antes**: Comunicação síncrona via sockets TCP
- **Agora**: Comunicação assíncrona via Kafka, permitindo maior flexibilidade e escalabilidade

### 2. Desacoplamento
- **Antes**: Robôs precisavam conectar diretamente ao controlador
- **Agora**: Comunicação via tópicos Kafka, permitindo independência entre componentes

### 3. Tolerância a Falhas
- **Antes**: Falha na conexão interrompia o processo
- **Agora**: Mensagens persistidas no Kafka garantem entrega mesmo com falhas temporárias

### 4. Escalabilidade
- **Antes**: Limitado pelo número de conexões TCP
- **Agora**: Facilmente escalável adicionando mais consumidores/produtores

## Funcionamento

### Fluxo de Produção

1. **Criação de Ordem**: Controlador cria ordem de produção
2. **Envio para Soldagem**: Comando enviado via Kafka para robô de soldagem
3. **Processamento**: Robô processa e notifica conclusão via Kafka
4. **Fluxo Sequencial**: Veículo passa por Pintura → Montagem
5. **Conclusão**: Notificação final de produção concluída

### Mensagens JSON

#### Comando de Processamento
```json
{
  "acao": "PROCESSAR",
  "veiculoId": 123,
  "tipoRobo": "SOLDAGEM",
  "timestamp": 1672531200000
}
```

#### Status do Robô
```json
{
  "tipo": "SOLDAGEM",
  "status": "OCUPADO",
  "descricao": "Processando veículo 123",
  "timestamp": 1672531200000
}
```

#### Conclusão de Tarefa
```json
{
  "tipo": "SOLDAGEM",
  "status": "DISPONIVEL",
  "descricao": "Processamento concluído",
  "acao": "CONCLUIDO",
  "veiculoId": 123,
  "timestamp": 1672531200000
}
```

## Como Executar

### Pré-requisitos
- Docker
- Docker Compose
- Java 21+
- Maven

### Passos

1. **Construir os projetos**:
```bash
# No diretório controlador
mvn clean compile

# No diretório robo
mvn clean compile
```

2. **Executar o sistema**:
```bash
docker-compose up -d
```

3. **Verificar logs**:
```bash
# Logs do controlador
docker logs controlador-central

# Logs dos robôs
docker logs robo-soldagem
docker logs robo-pintura
docker logs robo-montagem

# Logs do Kafka
docker logs kafka
```

4. **Parar o sistema**:
```bash
docker-compose down
```

## Monitoramento

### Logs do Sistema
- Controlador: Exibe status dos robôs e progresso da produção
- Robôs: Mostram processamento de veículos e comunicação via Kafka
- Kafka: Logs de mensagens e tópicos

### Métricas
- Total de veículos produzidos
- Status em tempo real dos robôs
- Ordens pendentes na fila

## Configurações do Kafka

### Ambiente de Desenvolvimento
- **Bootstrap Servers**: `kafka:9092`
- **Replication Factor**: 1 (adequado para desenvolvimento)
- **Auto Create Topics**: Habilitado
- **Acks**: `all` (garantia de entrega)

### Grupos de Consumidores
- `controlador-group`: Controlador central
- `robo-{tipo}-group`: Cada robô tem seu próprio grupo

## Benefícios da Implementação

1. **Flexibilidade**: Fácil adição de novos tipos de robôs ou estações
2. **Monitoramento**: Rastreamento completo do fluxo de produção
3. **Manutenibilidade**: Código mais limpo e organizados
4. **Resilência**: Sistema continua funcionando mesmo com falhas pontuais
5. **Performance**: Comunicação assíncrona melhora throughput

## Próximos Passos

- Implementar persistência de dados
- Adicionar métricas avançadas de monitoramento
- Implementar balanceamento de carga entre robôs do mesmo tipo
- Adicionar interface web para visualização em tempo real
