# 🏭 Fábrica Distribuída - Sistema Completo

Sistema de produção distribuída com **Apache Kafka**, **Docker** e **Monitor Visual** em tempo real.

## 🚀 Como Usar (Início Rápido)

### 1️⃣ **Iniciar o Sistema Completo**
```powershell
.\iniciar-fabrica.ps1
```

### 2️⃣ **Parar o Sistema**
```powershell
.\parar-fabrica.ps1
```

**É só isso! O script faz tudo automaticamente! 🎉**

## 🌐 Interfaces Disponíveis

Após executar o script, você terá acesso a:

| 🔗 Interface | 📍 URL | 📝 Descrição |
|-------------|--------|-------------|
| **📈 Monitor da Fábrica** | [localhost:8082](http://localhost:8082) | **Dashboard visual em tempo real** |
| **🔍 Kafka UI** | [localhost:8080](http://localhost:8080) | Interface para mensagens Kafka |
| **📦 Estoque** | [localhost:8081](http://localhost:8081) | API do gerenciador de estoque |
| **🎛️ Controlador** | [localhost:8083](http://localhost:8083) | API do controlador central |

## 🎯 O que o Sistema Faz

### 🤖 **3 Robôs Automatizados**
- **🔥 Robô de Soldagem**: Processa chassis
- **🎨 Robô de Pintura**: Aplica acabamento
- **🔧 Robô de Montagem**: Finaliza veículos

### 📊 **Monitor Visual Avançado**
- Métricas em tempo real
- Status dos robôs atualizado automaticamente
- Interface moderna e responsiva
- Fluxo de produção visual

### ⚡ **Comunicação via Kafka**
- Mensagens assíncronas entre componentes
- Filas de ordens de produção
- Status updates em tempo real

## 🛠️ Tecnologias

- **Java 21** - Linguagem principal
- **Apache Kafka** - Sistema de mensageria
- **Docker & Docker Compose** - Containerização
- **HTML5/CSS3/JavaScript** - Interface web
- **Maven** - Gerenciamento de dependências

## 📁 Estrutura do Projeto

```
Fabrica-distribuida/
├── 🚀 iniciar-fabrica.ps1    # Script principal para Windows
├── 🚀 iniciar-fabrica.sh     # Script para Linux/macOS  
├── 🛑 parar-fabrica.ps1      # Script para parar sistema
├── 📋 README.md              # Este arquivo
├── 🐳 docker-compose.yml     # Configuração Docker
├── controlador/              # Controlador central
├── robo/                     # Código dos robôs
├── estoque/                  # Gerenciador de estoque
└── monitor/                  # Monitor visual
    └── MonitorAvancado.java  # ⭐ Interface principal
```

## 🔧 Comandos Úteis

```powershell
# Ver logs de um serviço específico
docker-compose logs monitor
docker-compose logs controlador

# Ver status dos containers
docker-compose ps

# Reiniciar apenas o monitor
docker-compose restart monitor

# Compilar apenas um serviço
docker-compose build monitor
```

## 🎉 Recursos do Monitor

- ✅ **Métricas Dinâmicas**: Veículos produzidos, ordens processadas
- ✅ **Status em Tempo Real**: Estado atual de cada robô
- ✅ **Fluxo Visual**: Acompanhe o processo completo
- ✅ **Auto-Refresh**: Atualização automática a cada 10 segundos
- ✅ **Design Responsivo**: Funciona em qualquer dispositivo
- ✅ **Animações CSS**: Interface moderna e intuitiva

---

## 📞 Suporte

**Problema com Docker?**
- Verifique se o Docker Desktop está rodando
- Execute: `docker --version`

**Script não executa?**
- No PowerShell: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`

**Monitor não abre?**
- Aguarde alguns segundos após executar o script
- Acesse manualmente: http://localhost:8082

---

🏭 **Sistema de Fábrica Distribuída** - Desenvolvido para Sistemas Distribuídos
