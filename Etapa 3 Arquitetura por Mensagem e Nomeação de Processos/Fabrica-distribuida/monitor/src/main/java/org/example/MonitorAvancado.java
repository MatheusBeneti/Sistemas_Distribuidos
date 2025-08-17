package org.example;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorAvancado {
    private static final int PORTA_WEB = 8082;
    private static final String KAFKA_SERVERS = "kafka:9092";
    
    // Dados em tempo real do sistema
    private final AtomicInteger veiculosConcluidos = new AtomicInteger(0);
    private final AtomicInteger ordensProcessadas = new AtomicInteger(0);
    private final AtomicInteger robosAtivos = new AtomicInteger(0);
    
    // Status dos robôs em tempo real
    private final Map<String, RoboStatus> statusRobos = new ConcurrentHashMap<>();
    private final List<EventoSistema> eventosRecentes = Collections.synchronizedList(new ArrayList<>());
    
    private HttpServer servidor;
    private Consumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean ativo = true;
    
    // Classe para armazenar status dos robôs
    private static class RoboStatus {
        String tipo;
        String status;
        String descricao;
        String vehiculoAtual;
        long timestamp;
        long tempoProcessamento = 0;
        
        RoboStatus(String tipo, String status, String descricao) {
            this.tipo = tipo;
            this.status = status;
            this.descricao = descricao;
            this.timestamp = System.currentTimeMillis();
        }
        
        void atualizarVeiculo(String vehiculo) {
            this.vehiculoAtual = vehiculo;
            this.timestamp = System.currentTimeMillis();
        }
        
        void atualizarStatus(String novoStatus) {
            this.status = novoStatus;
            this.timestamp = System.currentTimeMillis();
        }
    }
    
    // Classe para eventos do sistema
    private static class EventoSistema {
        String tipo;
        String descricao;
        long timestamp;
        
        EventoSistema(String tipo, String descricao) {
            this.tipo = tipo;
            this.descricao = descricao;
            this.timestamp = System.currentTimeMillis();
        }
    }
    
    public static void main(String[] args) {
        System.out.println("📈 Monitor Visual da Fábrica - Versão Avançada");
        MonitorAvancado monitor = new MonitorAvancado();
        monitor.iniciar();
    }
    
    public void iniciar() {
        try {
            System.out.println("🔧 [DEBUG] Tentando inicializar Kafka...");
            inicializarKafka();
            
            System.out.println("🔧 [DEBUG] Inicializando servidor web...");
            inicializarServidorWeb();
            
            // Thread para consumir mensagens do Kafka
            System.out.println("🔧 [DEBUG] Iniciando thread Kafka...");
            new Thread(this::consumirMensagensKafka, "KafkaConsumer").start();
            
            // Thread para atualizar estatísticas
            System.out.println("🔧 [DEBUG] Iniciando thread stats...");
            new Thread(this::atualizarEstatisticas, "StatsUpdater").start();
            
            System.out.println("🌐 Monitor Web disponível em: http://localhost:" + PORTA_WEB);
            System.out.println("📊 Dashboard integrado com Kafka iniciado!");
            System.out.println("💡 Monitor visual conectado ao sistema real!");
            
            // Manter o programa rodando
            while (ativo) {
                Thread.sleep(1000);
            }
            
        } catch (Exception e) {
            System.err.println("❌ Erro crítico ao iniciar monitor: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void inicializarKafka() {
        System.out.println("🔧 [DEBUG] Método inicializarKafka() chamado");
        try {
            System.out.println("🔧 [DEBUG] Criando propriedades Kafka...");
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor-group-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            
            System.out.println("🔧 [DEBUG] Criando KafkaConsumer...");
            consumer = new KafkaConsumer<>(props);
            
            System.out.println("🔧 [DEBUG] Criando lista de tópicos...");
            List<String> topics = Arrays.asList("status-robos", "ordens-producao", "producao-concluida");
            
            System.out.println("🔧 [DEBUG] Fazendo subscribe nos tópicos...");
            consumer.subscribe(topics);
            
            System.out.println("📡 Monitor conectado ao Kafka: " + KAFKA_SERVERS);
            System.out.println("🔄 Configurado para ler desde o início (earliest offset)");
            System.out.println("📋 Inscrito nos tópicos: " + topics);
            
            // Teste inicial para verificar se está funcionando
            System.out.println("🔍 Fazendo poll inicial para ativar consumer...");
            ConsumerRecords<String, String> testRecords = consumer.poll(Duration.ofMillis(5000));
            System.out.println("📊 Poll inicial encontrou " + testRecords.count() + " mensagens");
            
            // Processar mensagens encontradas no poll inicial
            for (ConsumerRecord<String, String> record : testRecords) {
                System.out.println("🎯 [INICIAL] Tópico: " + record.topic() + ", Chave: " + record.key());
                processarMensagemKafka(record.topic(), record.key(), record.value());
            }
            
            System.out.println("✅ Kafka inicializado com sucesso!");
            
        } catch (Exception e) {
            System.err.println("⚠️ Erro ao conectar Kafka (modo simulação): " + e.getMessage());
            e.printStackTrace();
            consumer = null; // Garantir que não tente usar um consumer inválido
            // Continuar em modo simulação se Kafka não estiver disponível
        }
    }
    
    private void inicializarServidorWeb() throws IOException {
        servidor = HttpServer.create(new InetSocketAddress(PORTA_WEB), 0);
        
        // Página principal
        servidor.createContext("/", new DashboardHandler());
        
        servidor.setExecutor(null);
        servidor.start();
        
        System.out.println("🖥️ Servidor web iniciado na porta " + PORTA_WEB);
    }
    
    private void consumirMensagensKafka() {
        while (ativo) {
            try {
                if (consumer != null) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        processarMensagemKafka(record.topic(), record.key(), record.value());
                    }
                } else {
                    // Modo simulação se Kafka não disponível
                    Thread.sleep(5000);
                    simularDadosDemo();
                }
                
            } catch (Exception e) {
                System.err.println("❌ Erro ao consumir Kafka (continuando em modo demo): " + e.getMessage());
                try {
                    Thread.sleep(5000);
                    simularDadosDemo();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    private void processarMensagemKafka(String topico, String chave, String valor) {
        try {
            System.out.println("📩 [KAFKA] Recebida mensagem - Tópico: " + topico + ", Chave: " + chave);
            
            switch (topico) {
                case "status-robos":
                    processarStatusRobo(chave, valor);
                    break;
                case "ordens-producao":
                    processarOrdemProducao(chave, valor);
                    break;
                case "producao-concluida":
                    processarProducaoConcluida(chave, valor);
                    break;
                default:
                    System.out.println("⚠️ Tópico desconhecido: " + topico);
            }
        } catch (Exception e) {
            System.err.println("❌ Erro ao processar mensagem: " + e.getMessage());
        }
    }
    
    private void processarStatusRobo(String tipoRobo, String mensagemJson) {
        try {
            Map<String, Object> dados = objectMapper.readValue(mensagemJson, Map.class);
            
            String status = (String) dados.get("status");
            String descricao = (String) dados.get("descricao");
            String acao = (String) dados.get("acao");
            Integer veiculoId = (Integer) dados.get("veiculoId");
            
            RoboStatus roboStatus = statusRobos.getOrDefault(tipoRobo, 
                new RoboStatus(tipoRobo, "DESCONHECIDO", "Inicializando"));
            
            roboStatus.status = status;
            roboStatus.descricao = descricao;
            roboStatus.timestamp = System.currentTimeMillis();
            
            if (veiculoId != null) {
                roboStatus.vehiculoAtual = "VE_" + String.format("%03d", veiculoId);
            }
            
            if ("OCUPADO".equals(status)) {
                roboStatus.tempoProcessamento = System.currentTimeMillis();
            }
            
            statusRobos.put(tipoRobo, roboStatus);
            
            // Atualizar contador de robôs ativos
            long robotsAtivos = statusRobos.values().stream()
                .filter(r -> "OCUPADO".equals(r.status) || "DISPONIVEL".equals(r.status))
                .count();
            robosAtivos.set((int) robotsAtivos);
            
            // Adicionar evento
            if ("CONCLUIDO".equals(acao)) {
                adicionarEvento("PRODUÇÃO", tipoRobo + " concluiu veículo #" + veiculoId);
            } else if ("OCUPADO".equals(status)) {
                adicionarEvento("PROCESSAMENTO", tipoRobo + " iniciou processamento do veículo #" + veiculoId);
            }
            
            System.out.println("📊 [MONITOR] " + tipoRobo + " - " + status + ": " + descricao);
            
        } catch (Exception e) {
            System.err.println("❌ Erro ao processar status do robô: " + e.getMessage());
        }
    }
    
    private void processarOrdemProducao(String ordemId, String mensagemJson) {
        try {
            ordensProcessadas.incrementAndGet();
            adicionarEvento("ORDEM", "Nova ordem de produção #" + ordemId + " recebida");
            System.out.println("📋 [MONITOR] Nova ordem processada: " + ordemId);
        } catch (Exception e) {
            System.err.println("❌ Erro ao processar ordem: " + e.getMessage());
        }
    }
    
    private void processarProducaoConcluida(String veiculoId, String mensagemJson) {
        try {
            veiculosConcluidos.incrementAndGet();
            adicionarEvento("CONCLUSÃO", "Veículo #" + veiculoId + " produzido com sucesso!");
            System.out.println("🎉 [MONITOR] Veículo concluído: " + veiculoId);
        } catch (Exception e) {
            System.err.println("❌ Erro ao processar conclusão: " + e.getMessage());
        }
    }
    
    private void adicionarEvento(String tipo, String descricao) {
        EventoSistema evento = new EventoSistema(tipo, descricao);
        eventosRecentes.add(0, evento); // Adicionar no início
        
        // Manter apenas os últimos 20 eventos
        if (eventosRecentes.size() > 20) {
            eventosRecentes.subList(20, eventosRecentes.size()).clear();
        }
    }
    
    private void simularDadosDemo() {
        // Simular dados quando Kafka não está disponível
        if (statusRobos.isEmpty()) {
            statusRobos.put("SOLDAGEM", new RoboStatus("SOLDAGEM", "OCUPADO", "Processando veículo demo"));
            statusRobos.put("PINTURA", new RoboStatus("PINTURA", "DISPONIVEL", "Aguardando próximo veículo"));
            statusRobos.put("MONTAGEM", new RoboStatus("MONTAGEM", "OCUPADO", "Montando componentes"));
            robosAtivos.set(3);
        }
        
        // Simular progresso ocasional
        if (Math.random() > 0.8) {
            veiculosConcluidos.incrementAndGet();
            adicionarEvento("DEMO", "Veículo simulado concluído (modo demo)");
        }
        
        if (Math.random() > 0.7) {
            ordensProcessadas.incrementAndGet();
            adicionarEvento("DEMO", "Ordem simulada processada (modo demo)");
        }
    }
    
    private String getStatusDisplay(String status) {
        switch (status.toUpperCase()) {
            case "ATIVO": return "ATIVO";
            case "PROCESSANDO": return "PROCESSANDO";
            case "DISPONIVEL": return "DISPONÍVEL";
            case "AGUARDANDO": return "AGUARDANDO";
            case "MANUTENCAO": return "MANUTENÇÃO";
            case "ERRO": return "ERRO";
            default: return "DESCONHECIDO";
        }
    }
    
    private String getStatusColor(String status) {
        switch (status.toUpperCase()) {
            case "ATIVO":
            case "PROCESSANDO": return "ativo";
            case "DISPONIVEL": return "disponivel";
            case "AGUARDANDO": return "aguardando";
            case "MANUTENCAO": return "manutencao";
            case "ERRO": return "erro";
            default: return "offline";
        }
    }
    
    private String getStatusDescription(String tipoRobo, String status) {
        switch (tipoRobo.toUpperCase()) {
            case "SOLDAGEM":
                switch (status.toUpperCase()) {
                    case "ATIVO":
                    case "PROCESSANDO": return "Soldando componentes do veículo";
                    case "DISPONIVEL": return "Pronto para iniciar soldagem";
                    case "AGUARDANDO": return "Aguardando material ou comando";
                    default: return "Status: " + status;
                }
            case "PINTURA":
                switch (status.toUpperCase()) {
                    case "ATIVO":
                    case "PROCESSANDO": return "Aplicando tinta no veículo";
                    case "DISPONIVEL": return "Pronto para pintura";
                    case "AGUARDANDO": return "Aguardando veículo da soldagem";
                    default: return "Status: " + status;
                }
            case "MONTAGEM":
                switch (status.toUpperCase()) {
                    case "ATIVO":
                    case "PROCESSANDO": return "Montando componentes finais";
                    case "DISPONIVEL": return "Pronto para montagem";
                    case "AGUARDANDO": return "Aguardando veículo da pintura";
                    default: return "Status: " + status;
                }
            default:
                return "Status: " + status;
        }
    }
    
    private int calcularEficiencia() {
        if (statusRobos.isEmpty()) return 0;
        
        long robosAtivos = statusRobos.values().stream()
            .mapToLong(status -> {
                String s = status.status.toUpperCase();
                return (s.equals("ATIVO") || s.equals("PROCESSANDO")) ? 1 : 0;
            })
            .sum();
            
        return (int) ((robosAtivos * 100) / statusRobos.size());
    }

    private void atualizarEstatisticas() {
        while (ativo) {
            try {
                Thread.sleep(30000); // A cada 30 segundos
                
                // Log de estatísticas
                System.out.println("📊 [STATS] Veículos: " + veiculosConcluidos.get() + 
                    ", Ordens: " + ordensProcessadas.get() + 
                    ", Robôs ativos: " + robosAtivos.get());
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private class DashboardHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String html = gerarHTMLDashboard();
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, html.getBytes(StandardCharsets.UTF_8).length);
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(html.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
    
    private String gerarHTMLDashboard() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        String agora = LocalDateTime.now().format(formatter);
        
        StringBuilder html = new StringBuilder();
        
        // HTML básico
        html.append("<!DOCTYPE html>");
        html.append("<html lang='pt-BR'>");
        html.append("<head>");
        html.append("<meta charset='UTF-8'>");
        html.append("<meta name='viewport' content='width=device-width, initial-scale=1.0'>");
        html.append("<title>Monitor da Fábrica Distribuída</title>");
        
        // CSS Avançado
        html.append("<style>");
        html.append("body { ");
        html.append("font-family: 'Segoe UI', Arial, sans-serif; ");
        html.append("margin: 0; padding: 20px; ");
        html.append("background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); ");
        html.append("color: white; min-height: 100vh; ");
        html.append("} ");
        
        html.append(".container { max-width: 1200px; margin: 0 auto; } ");
        html.append(".header { text-align: center; margin-bottom: 30px; } ");
        html.append(".header h1 { font-size: 2.5em; margin: 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); } ");
        
        html.append(".metrics { ");
        html.append("display: grid; ");
        html.append("grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); ");
        html.append("gap: 20px; margin-bottom: 30px; ");
        html.append("} ");
        
        html.append(".metric-card { ");
        html.append("background: rgba(255,255,255,0.1); ");
        html.append("padding: 20px; border-radius: 10px; ");
        html.append("text-align: center; ");
        html.append("backdrop-filter: blur(10px); ");
        html.append("transition: transform 0.3s ease; ");
        html.append("} ");
        
        html.append(".metric-card:hover { transform: translateY(-5px); } ");
        html.append(".metric-card h3 { margin: 0 0 10px 0; font-size: 1.2em; } ");
        html.append(".metric-card .value { font-size: 2em; font-weight: bold; color: #00ff88; } ");
        
        html.append(".robots-grid { ");
        html.append("display: grid; ");
        html.append("grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); ");
        html.append("gap: 15px; margin-bottom: 30px; ");
        html.append("} ");
        
        html.append(".robot-card { ");
        html.append("background: rgba(255,255,255,0.1); ");
        html.append("padding: 20px; border-radius: 10px; ");
        html.append("backdrop-filter: blur(10px); ");
        html.append("border-left: 4px solid #00ff88; ");
        html.append("} ");
        
        html.append(".robot-card h4 { ");
        html.append("margin: 0 0 15px 0; font-size: 1.3em; ");
        html.append("display: flex; align-items: center; ");
        html.append("} ");
        
        html.append(".robot-icon { font-size: 1.5em; margin-right: 10px; } ");
        html.append(".status { ");
        html.append("padding: 5px 15px; border-radius: 20px; ");
        html.append("font-size: 0.9em; font-weight: bold; ");
        html.append("display: inline-block; margin: 5px 0; ");
        html.append("} ");
        
        html.append(".status.disponivel { background: #00ff88; color: #000; } ");
        html.append(".status.processando { background: #ffa500; color: #000; } ");
        html.append(".status.ativo { background: #4dabf7; color: #000; } ");
        html.append(".status.manutencao { background: #ff6b6b; color: #fff; } ");
        
        html.append(".flow-diagram { ");
        html.append("background: rgba(255,255,255,0.1); ");
        html.append("padding: 30px; border-radius: 10px; ");
        html.append("text-align: center; margin-bottom: 30px; ");
        html.append("backdrop-filter: blur(10px); ");
        html.append("} ");
        
        html.append(".flow-step { ");
        html.append("display: inline-block; ");
        html.append("background: rgba(255,255,255,0.2); ");
        html.append("padding: 20px; margin: 10px; ");
        html.append("border-radius: 10px; min-width: 120px; ");
        html.append("transition: transform 0.3s ease; ");
        html.append("} ");
        
        html.append(".flow-step:hover { transform: scale(1.05); } ");
        html.append(".arrow { font-size: 2em; color: #00ff88; margin: 0 10px; } ");
        
        html.append(".live-indicator { ");
        html.append("position: fixed; top: 20px; right: 20px; ");
        html.append("background: rgba(0,0,0,0.7); ");
        html.append("padding: 10px 20px; border-radius: 25px; ");
        html.append("display: flex; align-items: center; ");
        html.append("} ");
        
        html.append(".pulse { ");
        html.append("width: 10px; height: 10px; ");
        html.append("background: #00ff88; border-radius: 50%; ");
        html.append("margin-right: 10px; ");
        html.append("animation: pulse 2s infinite; ");
        html.append("} ");
        
        html.append("@keyframes pulse { ");
        html.append("0% { transform: scale(1); opacity: 1; } ");
        html.append("50% { transform: scale(1.2); opacity: 0.7; } ");
        html.append("100% { transform: scale(1); opacity: 1; } ");
        html.append("} ");
        
        html.append(".footer-info { ");
        html.append("background: rgba(255,255,255,0.1); ");
        html.append("padding: 20px; border-radius: 10px; ");
        html.append("text-align: center; margin-top: 30px; ");
        html.append("} ");
        
        html.append("</style>");
        html.append("</head>");
        html.append("<body>");
        
        // Indicador ao vivo
        html.append("<div class='live-indicator'>");
        html.append("<div class='pulse'></div>");
        html.append("<span>SISTEMA ATIVO</span>");
        html.append("</div>");
        
        // Container principal
        html.append("<div class='container'>");
        
        // Header
        html.append("<div class='header'>");
        html.append("<h1>🏭 Monitor da Fábrica Distribuída</h1>");
        html.append("<p>Sistema de Monitoramento Visual Avançado - Integração Kafka</p>");
        html.append("</div>");
        
        // Métricas principais
        html.append("<div class='metrics'>");
        
        html.append("<div class='metric-card'>");
        html.append("<h3>🚗 Veículos Produzidos</h3>");
        html.append("<div class='value'>").append(veiculosConcluidos.get()).append("</div>");
        html.append("</div>");
        
        html.append("<div class='metric-card'>");
        html.append("<h3>📋 Ordens Processadas</h3>");
        html.append("<div class='value'>").append(ordensProcessadas.get()).append("</div>");
        html.append("</div>");
        
        html.append("<div class='metric-card'>");
        html.append("<h3>🤖 Robôs Ativos</h3>");
        html.append("<div class='value'>").append(robosAtivos.get()).append("</div>");
        html.append("</div>");
        
        html.append("<div class='metric-card'>");
        html.append("<h3>📊 Eficiência</h3>");
        html.append("<div class='value'>").append(calcularEficiencia()).append("%</div>");
        html.append("</div>");
        
        html.append("</div>");
        
        // Fluxo de produção dinâmico
        html.append("<div class='flow-diagram'>");
        html.append("<h3>🔄 Fluxo de Produção em Tempo Real</h3>");
        html.append("<div style='display: flex; justify-content: center; align-items: center; flex-wrap: wrap;'>");
        
        html.append("<div class='flow-step'>");
        html.append("<div style='font-size: 2em;'>📝</div>");
        html.append("<div><strong>Ordem</strong></div>");
        html.append("<div>").append(ordensProcessadas.get()).append(" processadas</div>");
        html.append("</div>");
        
        html.append("<div class='arrow'>→</div>");
        
        // Status dinâmico da soldagem
        RoboStatus soldagem = statusRobos.get("SOLDAGEM");
        html.append("<div class='flow-step'>");
        html.append("<div style='font-size: 2em;'>🔥</div>");
        html.append("<div><strong>Soldagem</strong></div>");
        html.append("<div>").append(soldagem != null ? getStatusDisplay(soldagem.status) : "Offline").append("</div>");
        html.append("</div>");
        
        html.append("<div class='arrow'>→</div>");
        
        // Status dinâmico da pintura
        RoboStatus pintura = statusRobos.get("PINTURA");
        html.append("<div class='flow-step'>");
        html.append("<div style='font-size: 2em;'>🎨</div>");
        html.append("<div><strong>Pintura</strong></div>");
        html.append("<div>").append(pintura != null ? getStatusDisplay(pintura.status) : "Offline").append("</div>");
        html.append("</div>");
        
        html.append("<div class='arrow'>→</div>");
        
        // Status dinâmico da montagem
        RoboStatus montagem = statusRobos.get("MONTAGEM");
        html.append("<div class='flow-step'>");
        html.append("<div style='font-size: 2em;'>🔧</div>");
        html.append("<div><strong>Montagem</strong></div>");
        html.append("<div>").append(montagem != null ? getStatusDisplay(montagem.status) : "Offline").append("</div>");
        html.append("</div>");
        
        html.append("<div class='arrow'>→</div>");
        
        html.append("<div class='flow-step'>");
        html.append("<div style='font-size: 2em;'>✅</div>");
        html.append("<div><strong>Concluído</strong></div>");
        html.append("<div>Entregue</div>");
        html.append("</div>");
        
        html.append("</div>");
        html.append("</div>");
        
        // Status dos robôs em tempo real
        html.append("<h3 style='margin: 30px 0 20px 0;'>🤖 Status dos Robôs em Tempo Real</h3>");
        html.append("<div class='robots-grid'>");
        
        // Status dinâmico dos robôs
        String[] tiposRobo = {"SOLDAGEM", "PINTURA", "MONTAGEM"};
        String[] icones = {"🔥", "🎨", "🔧"};
        String[] descricoes = {"Soldagem", "Pintura", "Montagem"};
        
        for (int i = 0; i < tiposRobo.length; i++) {
            String tipo = tiposRobo[i];
            RoboStatus status = statusRobos.get(tipo);
            
            html.append("<div class='robot-card'>");
            html.append("<h4><span class='robot-icon'>").append(icones[i]).append("</span>Robô de ").append(descricoes[i]).append(" #0").append(i+1).append("</h4>");
            
            if (status != null) {
                html.append("<div class='status ").append(getStatusColor(status.status)).append("'>").append(getStatusDisplay(status.status)).append("</div>");
                
                if (status.vehiculoAtual != null && !status.vehiculoAtual.isEmpty()) {
                    html.append("<p><strong>Veículo Atual:</strong> ").append(status.vehiculoAtual).append("</p>");
                }
                
                html.append("<p><strong>Status:</strong> ").append(getStatusDescription(tipo, status.status)).append("</p>");
                
                // Tempo desde última atualização
                long tempoDecorrido = System.currentTimeMillis() - status.timestamp;
                long segundos = tempoDecorrido / 1000;
                html.append("<p><strong>Última Atualização:</strong> ");
                if (segundos < 60) {
                    html.append(segundos).append("s atrás");
                } else {
                    html.append(segundos / 60).append("min atrás");
                }
                html.append("</p>");
                
                html.append("<p><strong>Timestamp:</strong> ");
                html.append(new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(status.timestamp)));
                html.append("</p>");
                
            } else {
                html.append("<div class='status offline'>OFFLINE</div>");
                html.append("<p><strong>Status:</strong> Robô não conectado</p>");
                html.append("<p><strong>Última Atualização:</strong> Desconhecida</p>");
            }
            
            html.append("</div>");
        }
        
        html.append("</div>");
        
        // Eventos Recentes do Sistema
        html.append("<h3 style='margin: 30px 0 20px 0;'>📋 Eventos Recentes do Sistema</h3>");
        html.append("<div class='events-container' style='background: #1a1a1a; border-radius: 10px; padding: 20px; border: 1px solid #333; max-height: 400px; overflow-y: auto;'>");
        
        if (!eventosRecentes.isEmpty()) {
            synchronized (eventosRecentes) {
                for (EventoSistema evento : eventosRecentes) {
                    html.append("<div class='event-item' style='border-bottom: 1px solid #333; padding: 10px 0; margin-bottom: 10px;'>");
                    html.append("<div style='display: flex; justify-content: space-between; align-items: flex-start;'>");
                    html.append("<div>");
                    html.append("<strong style='color: #00ff88;'>").append(evento.tipo).append(":</strong> ");
                    html.append("<span style='color: #ffffff;'>").append(evento.descricao).append("</span>");
                    html.append("</div>");
                    html.append("<div style='color: #888; font-size: 0.9em; white-space: nowrap; margin-left: 20px;'>");
                    html.append(new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date(evento.timestamp)));
                    html.append("</div>");
                    html.append("</div>");
                    html.append("</div>");
                }
            }
        } else {
            html.append("<div style='text-align: center; color: #666; padding: 20px;'>");
            html.append("Nenhum evento registrado ainda. Aguardando mensagens dos robôs...");
            html.append("</div>");
        }
        
        html.append("</div>");
        
        // Informações do sistema
        html.append("<div class='footer-info'>");
        html.append("<h3>💡 Monitor Visual Avançado da Fábrica</h3>");
        html.append("<p>Sistema completo de monitoramento em tempo real com integração Kafka.</p>");
        html.append("<p><strong>Funcionalidades:</strong> Métricas dinâmicas, Status em tempo real, Interface responsiva, Atualizações automáticas</p>");
        html.append("<p><strong>Tecnologias:</strong> Java 21, Docker, Apache Kafka, HTML5, CSS3</p>");
        html.append("<p><strong>Última atualização do sistema:</strong> ").append(agora).append("</p>");
        html.append("<p style='margin-top: 20px;'>");
        html.append("<a href='javascript:location.reload()' style='color: #00ff88; text-decoration: none; font-weight: bold;'>🔄 Atualizar Dados</a>");
        html.append(" | ");
        html.append("<span style='color: #00ff88;'>⚡ Auto-refresh em 10s</span>");
        html.append("</p>");
        html.append("</div>");
        
        html.append("</div>");
        
        // JavaScript para auto-refresh
        html.append("<script>");
        html.append("setTimeout(() => { location.reload(); }, 10000);");
        html.append("</script>");
        
        html.append("</body>");
        html.append("</html>");
        
        return html.toString();
    }
}
