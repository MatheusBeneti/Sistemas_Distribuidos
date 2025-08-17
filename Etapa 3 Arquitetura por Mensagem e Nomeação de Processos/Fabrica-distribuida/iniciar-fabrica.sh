#!/bin/bash

# ==================================================
# 🏭 FABRICA DISTRIBUÍDA - SCRIPT DE INICIALIZAÇÃO
# ==================================================
# Sistema completo com Kafka + Monitor Visual
# Desenvolvido para Sistemas Distribuídos
# ==================================================

echo "🏭 ======================================"
echo "   FÁBRICA DISTRIBUÍDA - INICIALIZANDO"
echo "======================================="
echo ""

# Verificar se Docker está rodando
echo "🔍 Verificando Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker não está rodando!"
    echo "Por favor, inicie o Docker Desktop e tente novamente."
    exit 1
fi
echo "✅ Docker está funcionando"
echo ""

# Parar containers existentes (se houver)
echo "🛑 Parando containers existentes..."
docker-compose down --remove-orphans > /dev/null 2>&1
echo "✅ Containers parados"
echo ""

# Compilar e construir as imagens
echo "🔨 Compilando e construindo imagens..."
echo "   📦 Construindo controlador..."
docker-compose build controlador

echo "   📦 Construindo robôs..."
docker-compose build robo-soldagem robo-pintura robo-montagem

echo "   📦 Construindo estoque..."
docker-compose build estoque

echo "   📦 Construindo monitor..."
docker-compose build monitor

echo "✅ Todas as imagens construídas com sucesso"
echo ""

# Iniciar os serviços
echo "🚀 Iniciando serviços da fábrica..."
echo "   📊 Iniciando Kafka e ZooKeeper..."
docker-compose up -d zookeeper kafka

echo "   ⏳ Aguardando Kafka inicializar (15 segundos)..."
sleep 15

echo "   🏢 Iniciando serviços principais..."
docker-compose up -d controlador estoque

echo "   ⏳ Aguardando controlador inicializar (10 segundos)..."
sleep 10

echo "   🤖 Iniciando robôs..."
docker-compose up -d robo-soldagem robo-pintura robo-montagem

echo "   📈 Iniciando monitor visual..."
docker-compose up -d monitor

echo "   🖥️  Iniciando interface Kafka UI..."
docker-compose up -d kafka-ui

echo ""
echo "✅ SISTEMA INICIALIZADO COM SUCESSO!"
echo ""

# Verificar status dos containers
echo "📋 Status dos containers:"
docker-compose ps

echo ""
echo "🌐 =================================="
echo "       INTERFACES DISPONÍVEIS"
echo "===================================="
echo "📈 Monitor da Fábrica:  http://localhost:8082"
echo "🔍 Kafka UI:            http://localhost:8080"
echo "📦 Estoque:             http://localhost:8081"
echo "🎛️  Controlador:        http://localhost:8083"
echo ""

# Aguardar um pouco para o monitor ficar pronto
echo "⏳ Aguardando monitor inicializar..."
sleep 5

# Tentar abrir o monitor no navegador
echo "🖥️  Tentando abrir o monitor no navegador..."

# Detectar sistema operacional e abrir navegador
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    # Windows
    start http://localhost:8082
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    open http://localhost:8082
else
    # Linux
    xdg-open http://localhost:8082 2>/dev/null || echo "⚠️  Abra manualmente: http://localhost:8082"
fi

echo ""
echo "🎉 =================================="
echo "     FÁBRICA PRONTA PARA USO!"
echo "===================================="
echo ""
echo "📖 Para parar o sistema:"
echo "   docker-compose down"
echo ""
echo "📖 Para visualizar logs:"
echo "   docker-compose logs [serviço]"
echo ""
echo "📖 Serviços disponíveis:"
echo "   - controlador"
echo "   - robo-soldagem"
echo "   - robo-pintura"
echo "   - robo-montagem"
echo "   - estoque"
echo "   - monitor"
echo "   - kafka"
echo "   - zookeeper"
echo "   - kafka-ui"
echo ""
echo "🏭 Sistema de Fábrica Distribuída rodando!"
