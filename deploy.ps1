# deploy.ps1 — Reset lich su chay roi build + start Docker
# Cach dung: .\deploy.ps1

$ErrorActionPreference = "Stop"

# 1. Xoa lich su chay (job_his.xml → reset ve rong)
$histFile = Join-Path $PSScriptRoot "scripts\job_his.xml"
$emptyXml = "<?xml version='1.0' encoding='utf-8'?>`n<histories />"

if (Test-Path $histFile) {
    Set-Content -Path $histFile -Value $emptyXml -Encoding UTF8
    Write-Host "[OK] Da xoa lich su: $histFile"
} else {
    # File chua ton tai → tao moi de Docker mount vao khong bi loi
    $scriptsDir = Split-Path $histFile
    if (-not (Test-Path $scriptsDir)) { New-Item -ItemType Directory -Path $scriptsDir | Out-Null }
    Set-Content -Path $histFile -Value $emptyXml -Encoding UTF8
    Write-Host "[OK] Tao moi file lich su: $histFile"
}

# 2. Build va chay Docker
Write-Host "[..] Dang build Docker..."
docker compose up -d --build

Write-Host "[DONE] Ung dung da chay tai http://localhost:5000"
