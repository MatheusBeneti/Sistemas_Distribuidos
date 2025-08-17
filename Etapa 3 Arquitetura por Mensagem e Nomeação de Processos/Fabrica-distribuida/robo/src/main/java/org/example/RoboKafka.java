package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class RoboKafka {
    private static final String KAFKA_SERVERS = "kafka:9092";
    private static final String TOPICO_COMANDOS = "comandos-robos";
    private static final String TOPICO_STATUS = "status-robos";
    
    private final String tipoRobo;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private volatile boolean ativo = true;
    private String statusAtual = "INICIANDO";
    
    public RoboKafka(String tipoRobo) {
        this.tipoRobo = tipoRobo;
    }
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("❌ Uso: java RoboKafka <TIPO_ROBO>");
            System.err.println("   Tipos válidos: SOLDAGEM, PINTURA, MONTAGEM");
            System.exit(1);
        }
        
        String tipoRobo = args[0].toUpperCase();
        System.out.println("🤖 Iniciando Robô " + tipoRobo + " com Kafka");
        
        RoboKafka robo = new RoboKafka(tipoRobo);
        robo.iniciar();
    }
    
    public void iniciar() {
        try {
            inicializarKafka();
            
            // Threads para diferentes responsabilidades
            new Thread(this::consumirComandos, "ConsumeCommands").start();
            new Thread(this::enviarStatusPeriodico, "SendStatus").start();
            
            // Registrar robô
            registrarRobo();
            
            // Manter robô vivo
            while (ativo) {
                Thread.sleep(1000);
            }
            
        } catch (Exception e) {
            System.err.println("❌ Erro ao iniciar robô: " + e.getMessage());
            e.printStackTrace();
        } finally {
            fecharRecursos();
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
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "robo-" + tipoRobo.toLowerCase() + "-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(consumerProps);
        
        consumer.subscribe(Arrays.asList(TOPICO_COMANDOS));
        
        System.out.println("📡 [" + tipoRobo + "] Kafka configurado - Servidor: " + KAFKA_SERVERS);
    }
    
    private void registrarRobo() {
        statusAtual = "DISPONIVEL";
        String mensagem = criarMensagemStatus("DISPONIVEL", "Robô " + tipoRobo + " iniciado e pronto");
        enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagem);
        System.out.println("✅ [" + tipoRobo + "] Robô registrado via Kafka");
    }
    
    private void consumirComandos() {
        while (ativo) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    String tipoDestino = record.key();
                    String comando = record.value();
                    
                    // Verificar se o comando é para este robô
                    if (tipoRobo.equals(tipoDestino)) {
                        processarComando(comando);
                    }
                }
                
            } catch (Exception e) {
                System.err.println("❌ [" + tipoRobo + "] Erro ao consumir comandos: " + e.getMessage());
            }
        }
    }
    
    private void processarComando(String comandoJson) {
        try {
            Map<String, Object> comando = objectMapper.readValue(comandoJson, Map.class);
            String acao = (String) comando.get("acao");
            
            if ("PROCESSAR".equals(acao)) {
                Integer veiculoId = (Integer) comando.get("veiculoId");
                if (veiculoId != null) {
                    processarVeiculo(veiculoId);
                }
            }
            
        } catch (Exception e) {
            System.err.println("❌ [" + tipoRobo + "] Erro ao processar comando: " + e.getMessage());
        }
    }
    
    private void processarVeiculo(int veiculoId) {
        try {
            statusAtual = "OCUPADO";
            System.out.println("🔧 [" + tipoRobo + "] Iniciando processamento do veículo " + veiculoId);
            
            // Notificar início do processamento
            String mensagemInicio = criarMensagemStatus("OCUPADO", "Processando veículo " + veiculoId);
            enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagemInicio);
            
            // Simular tempo de processamento (variável por tipo)
            int tempoProcessamento = obterTempoProcessamento();
            Thread.sleep(tempoProcessamento);
            
            statusAtual = "DISPONIVEL";
            System.out.println("✅ [" + tipoRobo + "] Veículo " + veiculoId + " processado com sucesso!");
            
            // Notificar conclusão
            String mensagemConclusao = criarMensagemStatusConcluido(veiculoId);
            enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagemConclusao);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("❌ [" + tipoRobo + "] Processamento interrompido");
        } catch (Exception e) {
            System.err.println("❌ [" + tipoRobo + "] Erro no processamento: " + e.getMessage());
            statusAtual = "ERRO";
        }
    }
    
    private void enviarStatusPeriodico() {
        while (ativo) {
            try {
                Thread.sleep(15000); // Enviar status a cada 15 segundos
                
                String mensagem = criarMensagemStatus(statusAtual, "Status periódico");
                enviarMensagemKafka(TOPICO_STATUS, tipoRobo, mensagem);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private int obterTempoProcessamento() {
        // Tempos de processamento diferentes por tipo de robô
        return switch (tipoRobo) {
            case "SOLDAGEM" -> ThreadLocalRandom.current().nextInt(3000, 7000);
            case "PINTURA" -> ThreadLocalRandom.current().nextInt(4000, 8000);
            case "MONTAGEM" -> ThreadLocalRandom.current().nextInt(5000, 10000);
            default -> ThreadLocalRandom.current().nextInt(3000, 6000);
        };
    }
    
    private void enviarMensagemKafka(String topico, String chave, String valor) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topico, chave, valor);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("❌ [" + tipoRobo + "] Erro ao enviar mensagem: " + exception.getMessage());
                }
            });
        } catch (Exception e) {
            System.err.println("❌ [" + tipoRobo + "] Erro ao enviar mensagem: " + e.getMessage());
        }
    }
    
    private String criarMensagemStatus(String status, String descricao) {
        try {
            Map<String, Object> mensagem = new HashMap<>();
            mensagem.put("tipo", tipoRobo);
            mensagem.put("status", status);
            mensagem.put("descricao", descricao);
            mensagem.put("timestamp", System.currentTimeMillis());
            
            return objectMapper.writeValueAsString(mensagem);
        } catch (Exception e) {
            return "{\"erro\":\"Falha ao criar mensagem de status\"}";
        }
    }
    
    private String criarMensagemStatusConcluido(int veiculoId) {
        try {
            Map<String, Object> mensagem = new HashMap<>();
            mensagem.put("tipo", tipoRobo);
            mensagem.put("status", "DISPONIVEL");
            mensagem.put("descricao", "Processamento concluído");
            mensagem.put("acao", "CONCLUIDO");
            mensagem.put("veiculoId", veiculoId);
            mensagem.put("timestamp", System.currentTimeMillis());
            
            return objectMapper.writeValueAsString(mensagem);
        } catch (Exception e) {
            return "{\"erro\":\"Falha ao criar mensagem de conclusão\"}";
        }
    }
    
    private void fecharRecursos() {
        ativo = false;
        try {
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }
            System.out.println("🔌 [" + tipoRobo + "] Recursos do Kafka fechados");
        } catch (Exception e) {
            System.err.println("❌ [" + tipoRobo + "] Erro ao fechar recursos: " + e.getMessage());
        }
    }
}
