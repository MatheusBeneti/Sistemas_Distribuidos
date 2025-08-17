# ==================================================
# FABRICA DISTRIBUIDA - SCRIPT DE PARADA
# ==================================================
# Para parar todos os servicos da fabrica
# ==================================================

Write-Host "====================================" -ForegroundColor Cyan
Write-Host "   PARANDO FABRICA DISTRIBUIDA" -ForegroundColor Red
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Parando todos os containers..." -ForegroundColor Blue
docker-compose down --remove-orphans

Write-Host ""
Write-Host "Limpando recursos nao utilizados..." -ForegroundColor Blue
docker system prune -f *>$null

Write-Host ""
Write-Host "Sistema parado com sucesso!" -ForegroundColor Green
Write-Host ""
Write-Host "Para reiniciar, execute: .\iniciar-fabrica.ps1" -ForegroundColor Yellow

Write-Host ""
Write-Host "Pressione qualquer tecla para fechar..." -ForegroundColor Gray
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
