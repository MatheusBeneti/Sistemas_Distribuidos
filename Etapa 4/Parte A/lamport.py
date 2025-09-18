import time
import threading
from typing import List, Tuple
import random


class Processo:
    
    def __init__(self, id_processo: int, nome: str):
        self.id = id_processo
        self.nome = nome
        self.relogio_lamport = 0  # Relógio lógico
        self.mensagens_recebidas = []  # Lista ordenada de mensagens
    
    def evento_interno(self):
        """Incrementa relógio para evento interno"""
        self.relogio_lamport += 1
        print(f"[{self.nome}] Evento interno → Relógio: {self.relogio_lamport}")
        return self.relogio_lamport
    
    def enviar_mensagem(self, conteudo: str):
        """Prepara mensagem para envio"""
        self.relogio_lamport += 1
        timestamp = self.relogio_lamport
        print(f"  📤 [{self.nome}] Enviando: '{conteudo}' com timestamp {timestamp}")
        return (self.id, conteudo, timestamp)
    
    def receber_mensagem(self, mensagem):
        """Recebe mensagem e atualiza relógio"""
        remetente, conteudo, timestamp_msg = mensagem
        
        # Guardar valor antigo do relógio
        relogio_anterior = self.relogio_lamport
        
        # Algoritmo de Lamport: max(relógio_local, timestamp_msg) + 1
        self.relogio_lamport = max(self.relogio_lamport, timestamp_msg) + 1
        
        # Guardar mensagem
        self.mensagens_recebidas.append(mensagem)
        
        print(f"    📥 [{self.nome}] Recebeu '{conteudo}' de P{remetente} "
              f"(timestamp:{timestamp_msg}) → Relógio: {relogio_anterior} → {self.relogio_lamport}")
    
    def ordenar_mensagens(self, todos_processos=None):
        """Ordena mensagens por timestamp de Lamport"""
        # Ordenar por timestamp, depois por ID do processo (desempate)
        self.mensagens_recebidas.sort(key=lambda msg: (msg[2], msg[0]))
        
        print(f"  📋 [{self.nome}] Mensagens ordenadas por timestamp:")
        if not self.mensagens_recebidas:
            print(f"     (nenhuma mensagem recebida)")
        else:
            for i, (remetente, conteudo, timestamp) in enumerate(self.mensagens_recebidas, 1):
                # Tentar encontrar nome do remetente
                remetente_nome = f"P{remetente}"
                if todos_processos:
                    for p in todos_processos:
                        if p.id == remetente:
                            remetente_nome = p.nome
                            break
                
                print(f"     {i}. [{timestamp:2d}] {remetente_nome}: '{conteudo}'")


class SimuladorRede:
    """Simula rede com latências diferentes"""
    
    def __init__(self, processos: List[Processo]):
        self.processos = processos
        self.latencias = self._criar_latencias()
        self._mostrar_matriz_latencias()
    
    def _criar_latencias(self):
        """Cria matriz de latências aleatórias"""
        latencias = {}
        for i in range(len(self.processos)):
            for j in range(len(self.processos)):
                if i != j:
                    # Latência entre 0.1 e 1.0 segundos
                    latencias[(i, j)] = random.uniform(0.1, 1.0)
        return latencias
    
    def _mostrar_matriz_latencias(self):
        """Mostra a matriz de latências da rede"""
        print("\n🌐 LATÊNCIAS DA REDE (segundos):")
        print("     ", end="")
        for j in range(len(self.processos)):
            print(f"{self.processos[j].nome:>8}", end="")
        print()
        
        for i in range(len(self.processos)):
            print(f"{self.processos[i].nome:>4}: ", end="")
            for j in range(len(self.processos)):
                if i == j:
                    print("    --  ", end="")
                else:
                    lat = self.latencias.get((i, j), 0)
                    print(f"  {lat:.2f}  ", end="")
            print()
    
    def broadcast(self, mensagem, remetente_id: int):
        """Envia mensagem para todos os outros processos"""
        remetente_nome = self.processos[remetente_id].nome
        destinatarios = []
        
        for processo in self.processos:
            if processo.id != remetente_id:
                latencia = self.latencias.get((remetente_id, processo.id), 0.1)
                destinatarios.append((processo.nome, latencia))
                
                # Simular latência com thread
                def entregar_com_delay(p, msg, delay):
                    time.sleep(delay)
                    p.receber_mensagem(msg)
                
                thread = threading.Thread(
                    target=entregar_com_delay,
                    args=(processo, mensagem, latencia)
                )
                thread.daemon = True
                thread.start()
        
        # Mostrar para quem está sendo enviado de forma mais clara
        destinatarios_str = ", ".join([f"{nome}({lat:.2f}s)" for nome, lat in destinatarios])
        print(f"    📡 Broadcasting para: {destinatarios_str}")


def cenario_chat_basico():
    """Cenário principal: chat com diferentes latências"""
    print("🔥 CENÁRIO: CHAT COM RELÓGIOS DE LAMPORT")
    print("=" * 50)
    print("Demonstrando ordenação de mensagens com diferentes latências de rede")
    
    # Criar 3 processos
    alice = Processo(0, "Alice")
    bob = Processo(1, "Bob")  
    carol = Processo(2, "Carol")
    processos = [alice, bob, carol]
    
    # Criar rede
    rede = SimuladorRede(processos)
    
    print("\n📋 SEQUÊNCIA DE ENVIOS:")
    
    # Alice envia primeira mensagem
    print("\n1️⃣ Alice inicia a conversa:")
    msg1 = alice.enviar_mensagem("Olá pessoal!")
    rede.broadcast(msg1, alice.id)
    
    time.sleep(0.3)
    
    # Bob responde
    print("\n2️⃣ Bob responde:")
    msg2 = bob.enviar_mensagem("Oi Alice!")
    rede.broadcast(msg2, bob.id)
    
    time.sleep(0.3)
    
    # Carol também responde
    print("\n3️⃣ Carol entra na conversa:")
    msg3 = carol.enviar_mensagem("Bom dia!")
    rede.broadcast(msg3, carol.id)
    
    # Esperar todas as entregas
    print(f"\n⏳ Aguardando todas as mensagens serem entregues (2s)...")
    time.sleep(2.5)
    
    # Mostrar resultados ordenados
    print(f"\n📊 RESULTADO FINAL - MENSAGENS ORDENADAS POR TIMESTAMP:")
    print("=" * 60)
    print("(As mensagens foram ordenadas pelos relógios de Lamport, não pela ordem de chegada)")
    
    for processo in processos:
        print()
        processo.ordenar_mensagens(processos)
    
    # Verificar se ordenação está correta
    print(f"\n✅ VERIFICAÇÃO DA ORDENAÇÃO:")
    print("-" * 30)
    for processo in processos:
        timestamps = [msg[2] for msg in processo.mensagens_recebidas]
        if timestamps == sorted(timestamps):
            print(f"  ✓ {processo.nome}: Ordenação CORRETA (timestamps crescentes)")
        else:
            print(f"  ✗ {processo.nome}: Ordenação INCORRETA")
    
    print(f"\n💡 OBSERVAÇÃO:")
    print(f"  - Apesar das latências diferentes, todos os processos ordenaram as mensagens corretamente")
    print(f"  - Os relógios de Lamport garantem ordem causal mesmo com atrasos de rede")


def cenario_concorrente():
    """Cenário adicional: mensagens simultâneas"""
    print(f"\n\n🚀 CENÁRIO: MENSAGENS SIMULTÂNEAS")
    print("=" * 40)
    print("Demonstrando como Lamport resolve ordenação quando mensagens são enviadas simultaneamente")
    
    # Criar processos
    p1 = Processo(0, "Pedro")
    p2 = Processo(1, "Maria") 
    p3 = Processo(2, "João")
    processos = [p1, p2, p3]
    
    rede = SimuladorRede(processos)
    
    print("\n📋 ENVIO SIMULTÂNEO:")
    print("Todos os processos vão enviar mensagens ao mesmo tempo")
    
    # Todos enviam simultaneamente
    def enviar_simultaneo(processo, conteudo):
        msg = processo.enviar_mensagem(conteudo)
        rede.broadcast(msg, processo.id)
    
    threads = [
        threading.Thread(target=enviar_simultaneo, args=(p1, "Primeira mensagem!")),
        threading.Thread(target=enviar_simultaneo, args=(p2, "Segunda mensagem!")),
        threading.Thread(target=enviar_simultaneo, args=(p3, "Terceira mensagem!"))
    ]
    
    print("\n⚡ Disparando todas as mensagens simultaneamente...")
    
    # Disparar todas ao mesmo tempo
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    
    # Aguardar entregas
    print(f"\n⏳ Aguardando todas as entregas (2.5s)...")
    time.sleep(2.5)
    
    # Mostrar resultados
    print(f"\n📊 RESULTADO - ORDENAÇÃO DE MENSAGENS SIMULTÂNEAS:")
    print("=" * 55)
    print("(Quando timestamps são iguais, o desempate é feito pelo ID do processo)")
    
    for processo in processos:
        print()
        processo.ordenar_mensagens(processos)



if __name__ == "__main__":
    print("🎯 SISTEMA DE CHAT COM RELÓGIOS DE LAMPORT")
    print("=" * 55)
    print("Demonstração prática de ordenação temporal em sistemas distribuídos")
    print("Os relógios de Lamport garantem ordem lógica independente das latências de rede")
    
    cenario_chat_basico()
    cenario_concorrente()
    
    print(f"\n🏁 DEMONSTRAÇÃO COMPLETA!")
