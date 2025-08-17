# ==================================================
# FABRICA DISTRIBUIDA - SCRIPT DE INICIALIZACAO
# ==================================================
# Sistema completo com Kafka + Monitor Visual
# Desenvolvido para Sistemas Distribuidos

# .\iniciar-fabrica.ps1
# ==================================================

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "   FABRICA DISTRIBUIDA - INICIALIZANDO" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Verificar se Docker esta rodando
Write-Host "Verificando Docker..." -ForegroundColor Blue
try {
    docker info | Out-Null
    Write-Host "Docker esta funcionando" -ForegroundColor Green
} catch {
    Write-Host "Docker nao esta rodando!" -ForegroundColor Red
    Write-Host "Por favor, inicie o Docker Desktop e tente novamente." -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Parar containers existentes (se houver)
Write-Host "Parando containers existentes..." -ForegroundColor Blue
docker-compose down --remove-orphans *>$null
Write-Host "Containers parados" -ForegroundColor Green
Write-Host ""

# Compilar e construir as imagens
Write-Host "Compilando e construindo imagens..." -ForegroundColor Blue
Write-Host "   Construindo controlador..." -ForegroundColor Yellow
docker-compose build controlador

Write-Host "   Construindo robos..." -ForegroundColor Yellow
docker-compose build robo-soldagem robo-pintura robo-montagem

Write-Host "   Construindo estoque..." -ForegroundColor Yellow
docker-compose build estoque

Write-Host "   Construindo monitor..." -ForegroundColor Yellow
docker-compose build monitor

Write-Host "Todas as imagens construidas com sucesso" -ForegroundColor Green
Write-Host ""

# Iniciar os servicos
Write-Host "Iniciando servicos da fabrica..." -ForegroundColor Blue
Write-Host "   Iniciando Kafka e ZooKeeper..." -ForegroundColor Yellow
docker-compose up -d zookeeper kafka

Write-Host "   Aguardando Kafka inicializar (15 segundos)..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

Write-Host "   Iniciando servicos principais..." -ForegroundColor Yellow
docker-compose up -d controlador estoque

Write-Host "   Aguardando controlador inicializar (10 segundos)..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "   Iniciando robos..." -ForegroundColor Yellow
docker-compose up -d robo-soldagem robo-pintura robo-montagem

Write-Host "   Iniciando monitor visual..." -ForegroundColor Yellow
docker-compose up -d monitor

Write-Host "   Iniciando interface Kafka UI..." -ForegroundColor Yellow
docker-compose up -d kafka-ui

Write-Host ""
Write-Host "SISTEMA INICIALIZADO COM SUCESSO!" -ForegroundColor Green
Write-Host ""

# Verificar status dos containers
Write-Host "Status dos containers:" -ForegroundColor Blue
docker-compose ps

Write-Host ""
Write-Host "==================================" -ForegroundColor Cyan
Write-Host "       INTERFACES DISPONIVEIS" -ForegroundColor Yellow
Write-Host "==================================" -ForegroundColor Cyan
Write-Host "Monitor da Fabrica:  http://localhost:8082" -ForegroundColor White
Write-Host "Kafka UI:            http://localhost:8080" -ForegroundColor White
Write-Host "Estoque:             http://localhost:8081" -ForegroundColor White
Write-Host "Controlador:         http://localhost:8083" -ForegroundColor White
Write-Host ""

# Aguardar um pouco para o monitor ficar pronto
Write-Host "Aguardando monitor inicializar..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Abrir o monitor no navegador
Write-Host "Abrindo monitor no navegador..." -ForegroundColor Blue
Start-Process "http://localhost:8082"

Write-Host ""
Write-Host "==================================" -ForegroundColor Green
Write-Host "     FABRICA PRONTA PARA USO!" -ForegroundColor Yellow
Write-Host "==================================" -ForegroundColor Green
Write-Host ""
Write-Host "Para parar o sistema:" -ForegroundColor Blue
Write-Host "   docker-compose down" -ForegroundColor White
Write-Host ""
Write-Host "Para visualizar logs:" -ForegroundColor Blue
Write-Host "   docker-compose logs [servico]" -ForegroundColor White
Write-Host ""
Write-Host "Servicos disponiveis:" -ForegroundColor Blue
Write-Host "   - controlador" -ForegroundColor White
Write-Host "   - robo-soldagem" -ForegroundColor White
Write-Host "   - robo-pintura" -ForegroundColor White
Write-Host "   - robo-montagem" -ForegroundColor White
Write-Host "   - estoque" -ForegroundColor White
Write-Host "   - monitor" -ForegroundColor White
Write-Host "   - kafka" -ForegroundColor White
Write-Host "   - zookeeper" -ForegroundColor White
Write-Host "   - kafka-ui" -ForegroundColor White
Write-Host ""
Write-Host "Sistema de Fabrica Distribuida rodando!" -ForegroundColor Green

# Manter a janela aberta
Write-Host ""
Write-Host "Pressione qualquer tecla para fechar..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
