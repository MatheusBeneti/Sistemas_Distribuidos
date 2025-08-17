package org.example;

import java.io.*;
import java.net.*;
import java.util.Random;

public class RoboDistribuido {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Uso: java RoboDistribuido <TIPO> <HOST_CONTROLADOR> <PORTA>");
            return;
        }

        String tipo = args[0], host = args[1];
        int porta = Integer.parseInt(args[2]);

        Socket socket = new Socket(host, porta);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        out.println(tipo + "," + InetAddress.getLocalHost().getHostName());
        out.println("STATUS,LIVRE");

        String comando;
        while ((comando = in.readLine()) != null) {
            if (comando.startsWith("PROCESSAR")) {
                int id = Integer.parseInt(comando.split(",")[1]);
                out.println("STATUS,OCUPADO");
                Thread.sleep(3000 + new Random().nextInt(3000));
                out.println("CONCLUIDO," + id);
                out.println("STATUS,LIVRE");
            }
        }
    }
}
