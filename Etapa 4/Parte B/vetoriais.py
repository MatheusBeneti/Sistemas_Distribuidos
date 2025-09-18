
from typing import List


class ProcessoVetorial:
    """Processo com relógio vetorial - versão minimalista"""
    
    def __init__(self, id_processo: int, total_processos: int):
        self.id = id_processo
        self.nome = f"P{id_processo}"
        self.total_processos = total_processos
        
        # Relógio vetorial: array de zeros
        self.relogio_vetorial = [0] * total_processos
        
        # Lista de eventos para análise
        self.eventos = []
    
    def evento_interno(self, descricao: str):
        """Executa evento interno"""
        # Incrementar próprio relógio
        self.relogio_vetorial[self.id] += 1
        
        # Salvar evento
        evento = {
            'tipo': 'interno',
            'descricao': descricao,
            'vetor': self.relogio_vetorial.copy()
        }
        self.eventos.append(evento)
        
        print(f"[{self.nome}] {descricao} → VC{self.relogio_vetorial}")
        return evento
    
    def enviar_mensagem(self, para_processo: int, mensagem: str):
        """Envia mensagem para outro processo"""
        # Incrementar próprio relógio
        self.relogio_vetorial[self.id] += 1
        
        # Salvar evento de envio
        evento = {
            'tipo': 'envio',
            'descricao': f"Enviando para {para_processo}: {mensagem}",
            'vetor': self.relogio_vetorial.copy(),
            'destinatario': para_processo
        }
        self.eventos.append(evento)
        
        print(f"[{self.nome}] Enviando para P{para_processo}: {mensagem} → VC{self.relogio_vetorial}")
        return evento
    
    def receber_mensagem(self, evento_envio):
        """Recebe mensagem de outro processo"""
        vetor_mensagem = evento_envio['vetor']
        
        # Algoritmo do relógio vetorial:
        # Para cada posição i:
        # - Se i == meu_id: incrementar
        # - Senão: pegar max(meu_vetor[i], vetor_mensagem[i])
        for i in range(self.total_processos):
            if i == self.id:
                self.relogio_vetorial[i] += 1
            else:
                self.relogio_vetorial[i] = max(self.relogio_vetorial[i], vetor_mensagem[i])
        
        # Salvar evento de recebimento
        evento = {
            'tipo': 'recebimento',
            'descricao': f"Recebendo de P{evento_envio['destinatario']}: {evento_envio['descricao']}",
            'vetor': self.relogio_vetorial.copy()
        }
        self.eventos.append(evento)
        
        print(f"[{self.nome}] Recebeu mensagem → VC{self.relogio_vetorial}")
        return evento


def comparar_vetores(vc1: List[int], vc2: List[int]) -> str:
    """
    Função principal: compara dois relógios vetoriais
    Retorna: 'antes', 'depois' ou 'concorrente'
    """
    
    # Verificar se vc1 ≤ vc2 (vc1 happened-before vc2)
    vc1_menor_igual = all(vc1[i] <= vc2[i] for i in range(len(vc1)))
    vc1_estritamente_menor = any(vc1[i] < vc2[i] for i in range(len(vc1)))
    
    # Verificar se vc2 ≤ vc1 (vc2 happened-before vc1)
    vc2_menor_igual = all(vc2[i] <= vc1[i] for i in range(len(vc2)))
    vc2_estritamente_menor = any(vc2[i] < vc1[i] for i in range(len(vc2)))
    
    if vc1_menor_igual and vc1_estritamente_menor:
        return 'antes'
    elif vc2_menor_igual and vc2_estritamente_menor:
        return 'depois'
    else:
        return 'concorrente'


def cenario_1_causalidade():
    """Cenário 1: Demonstra cadeia causal A → B"""
    print("🔥 CENÁRIO 1: CADEIA CAUSAL")
    print("=" * 30)
    print("Evento A em P1 → envia mensagem → Evento B em P2")
    
    # Criar 2 processos
    p1 = ProcessoVetorial(1, 3)
    p2 = ProcessoVetorial(2, 3)
    
    print(f"\n📋 Executando eventos:")
    
    # 1. Evento A em P1
    evento_a = p1.evento_interno("Evento A - processamento")
    
    # 2. P1 envia mensagem para P2
    envio = p1.enviar_mensagem(2, "dados do Evento A")
    
    # 3. P2 recebe mensagem (Evento B)
    evento_b = p2.receber_mensagem(envio)
    
    # Análise da causalidade
    print(f"\n🔍 ANÁLISE DE CAUSALIDADE:")
    print(f"Evento A: VC{evento_a['vetor']}")
    print(f"Evento B: VC{evento_b['vetor']}")
    
    relacao = comparar_vetores(evento_a['vetor'], evento_b['vetor'])
    
    if relacao == 'antes':
        print(f"✅ RESULTADO: Evento A aconteceu ANTES de Evento B")
    else:
        print(f"❌ ERRO: Relação inesperada: {relacao}")
    
    return evento_a, evento_b


def cenario_2_concorrencia():
    """Cenário 2: Demonstra eventos concorrentes X || Y"""
    print(f"\n\n🚀 CENÁRIO 2: EVENTOS CONCORRENTES")
    print("=" * 35)
    print("Evento X em P1 e Evento Y em P3 (independentes)")
    
    # Criar processos
    p1 = ProcessoVetorial(1, 4)
    p3 = ProcessoVetorial(3, 4)
    
    print(f"\n📋 Executando eventos independentes:")
    
    # Evento X em P1
    evento_x = p1.evento_interno("Evento X - operação independente")
    
    # Evento Y em P3 (sem comunicação com P1)
    evento_y = p3.evento_interno("Evento Y - operação independente") 
    
    # Análise da concorrência
    print(f"\n🔍 ANÁLISE DE CONCORRÊNCIA:")
    print(f"Evento X: VC{evento_x['vetor']}")
    print(f"Evento Y: VC{evento_y['vetor']}")
    
    relacao = comparar_vetores(evento_x['vetor'], evento_y['vetor'])
    
    if relacao == 'concorrente':
        print(f"✅ RESULTADO: O Evento X e o Evento Y são CONCORRENTES")
    else:
        print(f"❌ ERRO: Relação inesperada: {relacao}")
    
    return evento_x, evento_y


def demonstrar_comparacoes():
    """Demonstra diferentes tipos de comparação"""
    print(f"\n\n📊 EXEMPLOS DE COMPARAÇÃO DE VETORES")
    print("=" * 40)
    
    # Exemplos claros
    exemplos = [
        ([1, 0, 0], [1, 1, 0], "Causal: primeiro antes do segundo"),
        ([2, 1, 0], [1, 1, 1], "Concorrentes: sem relação causal"),
        ([3, 2, 1], [2, 1, 0], "Causal: primeiro depois do segundo"),
        ([1, 1, 1], [1, 1, 1], "Simultâneos: mesmos vetores"),
    ]
    
    for i, (vc1, vc2, descricao) in enumerate(exemplos, 1):
        relacao = comparar_vetores(vc1, vc2)
        print(f"\nExemplo {i}: {descricao}")
        print(f"  VC1: {vc1}")
        print(f"  VC2: {vc2}")
        print(f"  Relação: {relacao.upper()}")


def main():
    """Função principal - executa todos os cenários"""
    print("🎯 Analisador de Causalidade - Relógios Vetoriais Simplificado")
    print("=" * 65)
    print("Demonstra detecção de causalidade e concorrência em sistemas distribuídos")
    
    # Executar cenários obrigatórios
    cenario_1_causalidade()
    cenario_2_concorrencia()
    
    # Demonstrações adicionais
    demonstrar_comparacoes()
    
    print(f"\n🏁 DEMONSTRAÇÃO COMPLETA!")
    print(f"✅ Relógios vetoriais detectam causalidade precisa")
    print(f"✅ Função comparar_vetores identifica concorrência")
    print(f"✅ Algoritmo funciona corretamente")


if __name__ == "__main__":
    main()
