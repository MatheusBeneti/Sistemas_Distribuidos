package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;

public class ControladorCentralKafka {
    private static final int PORTA_CONTROLADOR = 8080;
    private static final String KAFKA_SERVERS = "kafka:9092";
    private static final String TOPICO_ORDENS = "ordens-producao";
    private static final String TOPICO_STATUS = "status-robos";
    private static final String TOPICO_COMANDOS = "comandos-robos";
    
    private final Map<String, String> statusRobos = new ConcurrentHashMap<>();
    private final Queue<OrdemProducao> filaProducao = new ConcurrentLinkedQueue<>();
    private final AtomicInteger contadorVeiculos = new AtomicInteger(0);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private ServerSocket serverSocket;
    private volatile boolean ativo = true;
    
    public static void main(String[] args) {
        System.out.println("🎯 Iniciando Controlador Central com Kafka");
        ControladorCentralKafka controlador = new ControladorCentralKafka();
        controlador.iniciar();
    }
    
    public void iniciar() {
        try {
            inicializarKafka();
            inicializarServidor();
            
            // Threads para diferentes responsabilidades
            new Thread(this::aceitarConexoesRobos, "AcceptConnections").start();
            new Thread(this::consumirStatusRobos, "ConsumeStatus").start();
            new Thread(this::processarOrdens, "ProcessOrders").start();
            new Thread(this::monitorarSistema, "MonitorSystem").start();
            
            // Simular ordens de produção
            simularOrdens();
            
        } catch (Exception e) {
            System.err.println("❌ Erro ao iniciar controlador: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void inicializarKafka() {
        // Configurar Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producer = new KafkaProducer<>(producerProps);
        
        // Configurar Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "controlador-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(consumerProps);
        
        consumer.subscribe(Arrays.asList(TOPICO_STATUS));
        
        System.out.println("📡 Kafka configurado - Servidor: " + KAFKA_SERVERS);
    }
    
    private void inicializarServidor() throws IOException {
        serverSocket = new ServerSocket(PORTA_CONTROLADOR);
        System.out.println("🎯 Controlador Central rodando na porta " + PORTA_CONTROLADOR);
        System.out.println("🌐 Hostname: " + InetAddress.getLocalHost().getHostName());
        System.out.println("📍 IP: " + InetAddress.getLocalHost().getHostAddress());
    }
    
    private void aceitarConexoesRobos() {
        while (ativo) {
            try {
                Socket socketRobo = serverSocket.accept();
                String enderecoRobo = socketRobo.getInetAddress().getHostAddress();
                System.out.println("🤝 Nova conexão de: " + enderecoRobo);
                
                new Thread(() -> gerenciarRoboLegado(socketRobo), "ManageRobotLegacy-" + enderecoRobo).start();
                
            } catch (IOException e) {
                if (ativo) {
                    System.err.println("❌ Erro ao aceitar conexão: " + e.getMessage());
                }
            }
        }
    }
    
    // Manter compatibilidade com robôs que ainda usam socket
    private void gerenciarRoboLegado(Socket socketRobo) {
        try (BufferedReader entrada = new BufferedReader(new InputStreamReader(socketRobo.getInputStream()));
             PrintWriter saida = new PrintWriter(socketRobo.getOutputStream(), true)) {
            
            String registro = entrada.readLine();
            if (registro != null) {
                String[] partes = registro.split(",");
                String tipoRobo = partes[0];
                String hostRobo = partes[1];
                
                statusRobos.put(tipoRobo, "CONECTADO");
                System.out.println("✅ Robô conectado via socket - Tipo: " + tipoRobo + ", Host: " + hostRobo);
                
                // Notificar via Kafka que um robô se conectou
                enviarMensagemKafka(TOPICO_STATUS, tipoRobo, 
                    criarMensagemStatus(tipoRobo, "CONECTADO", "Conectado via socket"));
                
                String mensagem;
                while ((mensagem = entrada.readLine()) != null) {
                    processarMensagemRoboViaKafka(tipoRobo, mensagem);
                }
            }
            
        } catch (IOException e) {
            System.err.println("❌ Erro na comunicação com robô: " + e.getMessage());
        }
    }
    
    private void consumirStatusRobos() {
        while (ativo) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    String tipoRobo = record.key();
                    String mensagem = record.value();
                    
                    processarMensagemStatusKafka(tipoRobo, mensagem);
                }
                
            } catch (Exception e) {
                System.err.println("❌ Erro ao consumir mensagens do Kafka: " + e.getMessage());
            }
        }
    }
    
    private void processarMensagemStatusKafka(String tipoRobo, String mensagem) {
        try {
            Map<String, Object> statusMsg = objectMapper.readValue(mensagem, Map.class);
            String acao = (String) statusMsg.get("acao");
            String status = (String) statusMsg.get("status");
            
            statusRobos.put(tipoRobo, status);
            System.out.println("📨 [KAFKA] " + tipoRobo + " - " + acao + ": " + status);
            
            if ("CONCLUIDO".equals(acao)) {
                Integer veiculoId = (Integer) statusMsg.get("veiculoId");
                if (veiculoId != null) {
                    enviarParaProximaEtapaViaKafka(veiculoId, tipoRobo);
                }
            }
            
        } catch (Exception e) {
            System.err.println("❌ Erro ao processar mensagem do Kafka: " + e.getMessage());
        }
    }
    
    private void processarMensagemRoboViaKafka(String tipoRobo, String mensagem) {
        System.out.println("📨 [SOCKET->KAFKA] " + tipoRobo + ": " + mensagem);
        
        // Converter mensagem do socket para formato Kafka
        if (mensagem.startsWith("CONCLUIDO")) {
            String[] partes = mensagem.split(",");
            int veiculoId = Integer.parseInt(partes[1]);
            
            String mensagemKafka = criarMensagemStatus(tipoRobo, "DISPONIVEL", "Tarefa concluída", veiculoId, "CONCLUIDO");
            enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagemKafka);
            
        } else if (mensagem.startsWith("STATUS")) {
            String[] partes = mensagem.split(",");
            String status = partes[1];
            
            String mensagemKafka = criarMensagemStatus(tipoRobo, status, "Status atualizado");
            enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagemKafka);
        }
    }
    
    private void enviarParaProximaEtapaViaKafka(int veiculoId, String tipoAtual) {
        String proximoTipo = obterProximoTipo(tipoAtual);
        
        if (proximoTipo != null) {
            String comando = criarComandoProcessar(veiculoId, proximoTipo);
            enviarMensagemKafka(TOPICO_COMANDOS, proximoTipo, comando);
            System.out.println("➡️ [KAFKA] Veículo " + veiculoId + " enviado para " + proximoTipo);
        } else {
            contadorVeiculos.incrementAndGet();
            System.out.println("🎉 Veículo " + veiculoId + " PRODUZIDO! Total: " + contadorVeiculos.get());
            
            // Notificar conclusão via Kafka
            String mensagemFinal = criarMensagemProducaoConcluida(veiculoId);
            enviarMensagemKafka("producao-concluida", String.valueOf(veiculoId), mensagemFinal);
        }
    }
    
    private String obterProximoTipo(String tipoAtual) {
        return switch (tipoAtual) {
            case "SOLDAGEM" -> "PINTURA";
            case "PINTURA" -> "MONTAGEM";
            case "MONTAGEM" -> null;
            default -> null;
        };
    }
    
    private void processarOrdens() {
        while (ativo) {
            OrdemProducao ordem = filaProducao.poll();
            if (ordem != null) {
                System.out.println("🏭 Processando ordem: " + ordem.getId());
                
                // Enviar ordem via Kafka
                String comando = criarComandoProcessar(ordem.getId(), "SOLDAGEM");
                enviarMensagemKafka(TOPICO_COMANDOS, "SOLDAGEM", comando);
                enviarMensagemKafka(TOPICO_ORDENS, String.valueOf(ordem.getId()), 
                    criarMensagemOrdem(ordem));
                
                System.out.println("🔨 [KAFKA] Ordem " + ordem.getId() + " enviada para SOLDAGEM");
            }
            
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private void monitorarSistema() {
        while (ativo) {
            try {
                System.out.println("\n=== STATUS DO SISTEMA (KAFKA) ===");
                for (Map.Entry<String, String> entry : statusRobos.entrySet()) {
                    System.out.println("🤖 " + entry.getKey() + ": " + entry.getValue());
                }
                System.out.println("🚗 Veículos produzidos: " + contadorVeiculos.get());
                System.out.println("📋 Ordens pendentes: " + filaProducao.size());
                
                Thread.sleep(10000);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private void simularOrdens() {
        new Thread(() -> {
            try {
                // Aguardar um pouco para o Kafka estar pronto
                Thread.sleep(5000);
                
                for (int i = 1; i <= 20; i++) {
                    OrdemProducao ordem = new OrdemProducao(i, "Modelo-" + i);
                    filaProducao.offer(ordem);
                    System.out.println("📝 Nova ordem criada: " + i);
                    
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "SimulateOrders").start();
    }
    
    private void enviarMensagemKafka(String topico, String chave, String valor) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topico, chave, valor);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("❌ Erro ao enviar mensagem para Kafka: " + exception.getMessage());
                }
            });
        } catch (Exception e) {
            System.err.println("❌ Erro ao enviar mensagem para Kafka: " + e.getMessage());
        }
    }
    
    private String criarMensagemStatus(String tipo, String status, String descricao) {
        return criarMensagemStatus(tipo, status, descricao, null, null);
    }
    
    private String criarMensagemStatus(String tipo, String status, String descricao, Integer veiculoId, String acao) {
        try {
            Map<String, Object> mensagem = new HashMap<>();
            mensagem.put("tipo", tipo);
            mensagem.put("status", status);
            mensagem.put("descricao", descricao);
            mensagem.put("timestamp", System.currentTimeMillis());
            if (veiculoId != null) mensagem.put("veiculoId", veiculoId);
            if (acao != null) mensagem.put("acao", acao);
            
            return objectMapper.writeValueAsString(mensagem);
        } catch (Exception e) {
            return "{\"erro\":\"Falha ao criar mensagem\"}";
        }
    }
    
    private String criarComandoProcessar(int veiculoId, String tipoRobo) {
        try {
            Map<String, Object> comando = new HashMap<>();
            comando.put("acao", "PROCESSAR");
            comando.put("veiculoId", veiculoId);
            comando.put("tipoRobo", tipoRobo);
            comando.put("timestamp", System.currentTimeMillis());
            
            return objectMapper.writeValueAsString(comando);
        } catch (Exception e) {
            return "{\"erro\":\"Falha ao criar comando\"}";
        }
    }
    
    private String criarMensagemOrdem(OrdemProducao ordem) {
        try {
            Map<String, Object> mensagem = new HashMap<>();
            mensagem.put("id", ordem.getId());
            mensagem.put("modelo", ordem.getModelo());
            mensagem.put("status", "INICIADA");
            mensagem.put("timestamp", System.currentTimeMillis());
            
            return objectMapper.writeValueAsString(mensagem);
        } catch (Exception e) {
            return "{\"erro\":\"Falha ao criar mensagem de ordem\"}";
        }
    }
    
    private String criarMensagemProducaoConcluida(int veiculoId) {
        try {
            Map<String, Object> mensagem = new HashMap<>();
            mensagem.put("veiculoId", veiculoId);
            mensagem.put("status", "CONCLUIDO");
            mensagem.put("timestampConclusao", System.currentTimeMillis());
            mensagem.put("totalProducao", contadorVeiculos.get());
            
            return objectMapper.writeValueAsString(mensagem);
        } catch (Exception e) {
            return "{\"erro\":\"Falha ao criar mensagem de conclusão\"}";
        }
    }
    
    public void parar() {
        ativo = false;
        try {
            if (producer != null) producer.close();
            if (consumer != null) consumer.close();
            if (serverSocket != null) serverSocket.close();
        } catch (Exception e) {
            System.err.println("❌ Erro ao parar controlador: " + e.getMessage());
        }
    }
}
