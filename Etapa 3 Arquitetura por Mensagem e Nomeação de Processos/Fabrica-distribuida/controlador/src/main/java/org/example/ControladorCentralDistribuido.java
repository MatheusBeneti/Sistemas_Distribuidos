package org.example;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ControladorCentralDistribuido {
    private static final int PORTA_CONTROLADOR = 8080;
    private final Map<String, ConexaoRobo> robosConectados = new ConcurrentHashMap<>();
    private final Queue<OrdemProducao> filaProducao = new ConcurrentLinkedQueue<>();
    private final AtomicInteger contadorVeiculos = new AtomicInteger(0);
    private ServerSocket serverSocket;
    private volatile boolean ativo = true;
    
    public static void main(String[] args) {
        System.out.println("🎯 Iniciando Controlador Central em Container");
        ControladorCentralDistribuido controlador = new ControladorCentralDistribuido();
        controlador.iniciar();
    }
    
    public void iniciar() {
        try {
            serverSocket = new ServerSocket(PORTA_CONTROLADOR);
            System.out.println("🎯 Controlador Central rodando no container na porta " + PORTA_CONTROLADOR);
            System.out.println("🌐 Hostname: " + InetAddress.getLocalHost().getHostName());
            System.out.println("📍 IP: " + InetAddress.getLocalHost().getHostAddress());
            
            // Threads para diferentes responsabilidades
            new Thread(this::aceitarConexoesRobos, "AcceptConnections").start();
            new Thread(this::processarOrdens, "ProcessOrders").start();
            new Thread(this::monitorarRobos, "MonitorRobots").start();
            
            // Simular ordens de produção
            simularOrdens();
            
        } catch (IOException e) {
            System.err.println("❌ Erro ao iniciar controlador: " + e.getMessage());
        }
    }
    
    private void aceitarConexoesRobos() {
        while (ativo) {
            try {
                Socket socketRobo = serverSocket.accept();
                String enderecoRobo = socketRobo.getInetAddress().getHostAddress();
                System.out.println("🤝 Nova conexão de: " + enderecoRobo);
                
                new Thread(() -> gerenciarRobo(socketRobo), "ManageRobot-" + enderecoRobo).start();
                
            } catch (IOException e) {
                if (ativo) {
                    System.err.println("❌ Erro ao aceitar conexão: " + e.getMessage());
                }
            }
        }
    }
    
    private void gerenciarRobo(Socket socketRobo) {
        try (BufferedReader entrada = new BufferedReader(new InputStreamReader(socketRobo.getInputStream()));
             PrintWriter saida = new PrintWriter(socketRobo.getOutputStream(), true)) {
            
            // Receber registro do robô
            String registro = entrada.readLine();
            if (registro != null) {
                String[] partes = registro.split(",");
                String tipoRobo = partes[0];
                String hostRobo = partes[1];
                
                ConexaoRobo conexao = new ConexaoRobo(tipoRobo, hostRobo, socketRobo, saida);
                robosConectados.put(tipoRobo, conexao);
                
                System.out.println("✅ Robô registrado - Tipo: " + tipoRobo + ", Host: " + hostRobo);
                System.out.println("🔢 Total de robôs conectados: " + robosConectados.size());
                
                // Escutar mensagens do robô
                String mensagem;
                while ((mensagem = entrada.readLine()) != null) {
                    processarMensagemRobo(tipoRobo, mensagem);
                }
            }
            
        } catch (IOException e) {
            System.err.println("❌ Erro na comunicação com robô: " + e.getMessage());
        }
    }
    
    private void processarMensagemRobo(String tipoRobo, String mensagem) {
        System.out.println("📨 [" + tipoRobo + "] " + mensagem);
        
        if (mensagem.startsWith("CONCLUIDO")) {
            String[] partes = mensagem.split(",");
            int veiculoId = Integer.parseInt(partes[1]);
            enviarParaProximaEtapa(veiculoId, tipoRobo);
        } else if (mensagem.startsWith("STATUS")) {
            String[] partes = mensagem.split(",");
            String status = partes[1];
            atualizarStatusRobo(tipoRobo, status);
        }
    }
    
    private void enviarParaProximaEtapa(int veiculoId, String tipoAtual) {
        String proximoTipo = obterProximoTipo(tipoAtual);
        
        if (proximoTipo != null) {
            ConexaoRobo proximoRobo = robosConectados.get(proximoTipo);
            if (proximoRobo != null) {
                proximoRobo.enviarComando("PROCESSAR," + veiculoId);
                System.out.println("➡️ Veículo " + veiculoId + " enviado para " + proximoTipo);
            } else {
                System.out.println("⚠️ Robô " + proximoTipo + " não disponível");
            }
        } else {
            contadorVeiculos.incrementAndGet();
            System.out.println("🎉 Veículo " + veiculoId + " PRODUZIDO! Total: " + contadorVeiculos.get());
        }
    }
    
    private String obterProximoTipo(String tipoAtual) {
        switch (tipoAtual) {
            case "SOLDAGEM": return "PINTURA";
            case "PINTURA": return "MONTAGEM";
            case "MONTAGEM": return null;
            default: return null;
        }
    }
    
    private void atualizarStatusRobo(String tipo, String status) {
        ConexaoRobo robo = robosConectados.get(tipo);
        if (robo != null) {
            robo.setStatus(status);
        }
    }
    
    private void processarOrdens() {
        while (ativo) {
            OrdemProducao ordem = filaProducao.poll();
            if (ordem != null) {
                System.out.println("🏭 Processando ordem: " + ordem.getId());
                
                ConexaoRobo roboSoldagem = robosConectados.get("SOLDAGEM");
                if (roboSoldagem != null) {
                    roboSoldagem.enviarComando("PROCESSAR," + ordem.getId());
                    System.out.println("🔨 Ordem " + ordem.getId() + " enviada para SOLDAGEM");
                } else {
                    System.out.println("⚠️ Robô SOLDAGEM não disponível");
                }
            }
            
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private void monitorarRobos() {
        while (ativo) {
            try {
                System.out.println("\n=== STATUS DOS ROBÔS ===");
                for (Map.Entry<String, ConexaoRobo> entry : robosConectados.entrySet()) {
                    ConexaoRobo robo = entry.getValue();
                    System.out.println("🤖 " + entry.getKey() + " (" + robo.getHost() + "): " + robo.getStatus());
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
            for (int i = 1; i <= 20; i++) {
                filaProducao.offer(new OrdemProducao(i, "Modelo-" + i));
                System.out.println("📝 Nova ordem criada: " + i);
                
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "SimulateOrders").start();
    }
}

// ConexaoRobo.java
class ConexaoRobo {
    private final String tipo;
    private final String host;
    private final Socket socket;
    private final PrintWriter saida;
    private String status = "DESCONHECIDO";
    
    public ConexaoRobo(String tipo, String host, Socket socket, PrintWriter saida) {
        this.tipo = tipo;
        this.host = host;
        this.socket = socket;
        this.saida = saida;
    }
    
    public void enviarComando(String comando) {
        saida.println(comando);
    }
    
    public String getTipo() { return tipo; }
    public String getHost() { return host; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}

// OrdemProducao.java
class OrdemProducao {
    private final int id;
    private final String modelo;
    
    public OrdemProducao(int id, String modelo) {
        this.id = id;
        this.modelo = modelo;
    }
    
    public int getId() { return id; }
    public String getModelo() { return modelo; }
}