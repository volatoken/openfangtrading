param(
    [switch]$RunDemo,
    [string]$Symbol = "BTCUSDT",
    [int]$Cycles = 10,
    [switch]$InstallFaiss,
    [switch]$SkipVenv
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-SystemPython {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return @{
            Exe = "py"
            Prefix = @("-3")
        }
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @{
            Exe = "python"
            Prefix = @()
        }
    }
    throw "Khong tim thay Python. Cai Python 3.10+ roi chay lai setup.ps1."
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$python = Resolve-SystemPython
$pythonExe = ""

if (-not $SkipVenv) {
    $venvPython = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
    if (-not (Test-Path $venvPython)) {
        Write-Host "[setup] Tao virtual environment (.venv)..."
        & $python.Exe @($python.Prefix + @("-m", "venv", ".venv"))
    } else {
        Write-Host "[setup] Da ton tai .venv, bo qua tao moi."
    }
    $pythonExe = $venvPython
} else {
    Write-Host "[setup] Dang dung Python he thong (khong tao .venv)."
    if ($python.Exe -eq "py") {
        throw "Khi -SkipVenv duoc bat, vui long dam bao lenh 'python' co san trong PATH."
    }
    $pythonExe = "python"
}

Write-Host "[setup] Nang cap pip..."
& $pythonExe -m pip install --upgrade pip

Write-Host "[setup] Cai dependencies tu requirements.txt..."
& $pythonExe -m pip install -r requirements.txt

if ($InstallFaiss) {
    Write-Host "[setup] Cai them faiss-cpu (tuy chon)..."
    & $pythonExe -m pip install faiss-cpu
}

if ($RunDemo) {
    Write-Host "[setup] Chay demo OpenFang..."
    & $pythonExe -m openfang_memory_evolution.app --symbol $Symbol --cycles $Cycles
}

Write-Host ""
Write-Host "[done] Setup hoan tat."
if (-not $SkipVenv) {
    Write-Host "Kich hoat venv: .\\.venv\\Scripts\\Activate.ps1"
}
Write-Host "Chay app: $pythonExe -m openfang_memory_evolution.app --symbol BTCUSDT --cycles 10"
