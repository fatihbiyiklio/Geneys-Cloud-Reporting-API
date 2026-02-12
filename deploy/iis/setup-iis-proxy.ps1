param(
    [string]$SiteName = "GeneysReporting",
    [string]$HostName = "",
    [string]$PhysicalPath = "C:\\inetpub\\geneys-proxy",
    [int]$AppPort = 8501,
    [switch]$EnableHttps,
    [string]$CertThumbprint = "",
    [switch]$OpenFirewall
)

$ErrorActionPreference = "Stop"

function Assert-Admin {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Bu script Administrator olarak calistirilmalidir."
    }
}

function Assert-IisModule {
    Import-Module WebAdministration -ErrorAction Stop
}

function Enable-ArrProxy {
    $appcmd = Join-Path $env:windir "System32\\inetsrv\\appcmd.exe"
    if (-not (Test-Path $appcmd)) {
        throw "appcmd bulunamadi: $appcmd"
    }

    try {
        & $appcmd set config -section:system.webServer/proxy /enabled:"True" /commit:apphost | Out-Null
    }
    catch {
        throw "ARR Proxy ayari yapilamadi. URL Rewrite + ARR kurulu oldugundan emin olun."
    }
}

function Ensure-PhysicalPath([string]$Path) {
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Ensure-AppPool([string]$PoolName) {
    if (-not (Test-Path "IIS:\\AppPools\\$PoolName")) {
        New-WebAppPool -Name $PoolName | Out-Null
    }

    Set-ItemProperty "IIS:\\AppPools\\$PoolName" -Name managedRuntimeVersion -Value ""
    Set-ItemProperty "IIS:\\AppPools\\$PoolName" -Name processModel.identityType -Value "ApplicationPoolIdentity"
}

function Ensure-Site([string]$Name, [string]$Path, [string]$Pool, [string]$HostHeader) {
    $existing = Get-Website -Name $Name -ErrorAction SilentlyContinue
    if ($null -eq $existing) {
        if ([string]::IsNullOrWhiteSpace($HostHeader)) {
            New-Website -Name $Name -PhysicalPath $Path -Port 80 -ApplicationPool $Pool | Out-Null
        }
        else {
            New-Website -Name $Name -PhysicalPath $Path -Port 80 -HostHeader $HostHeader -ApplicationPool $Pool | Out-Null
        }
    }
    else {
        Set-ItemProperty "IIS:\\Sites\\$Name" -Name physicalPath -Value $Path
        Set-ItemProperty "IIS:\\Sites\\$Name" -Name applicationPool -Value $Pool
    }
}

function Ensure-WebConfig([string]$TargetPath, [int]$Port) {
    $templatePath = Join-Path $PSScriptRoot "web.config.template"
    if (-not (Test-Path $templatePath)) {
        throw "Template dosyasi bulunamadi: $templatePath"
    }

    $template = Get-Content -Path $templatePath -Raw -Encoding UTF8
    $content = $template -replace "__APP_PORT__", [string]$Port

    $webConfigPath = Join-Path $TargetPath "web.config"
    Set-Content -Path $webConfigPath -Value $content -Encoding UTF8
}

function Ensure-HttpsBinding([string]$Name, [string]$HostHeader, [string]$Thumbprint) {
    if ([string]::IsNullOrWhiteSpace($Thumbprint)) {
        throw "-EnableHttps kullanildiginda -CertThumbprint zorunludur."
    }

    $binding = Get-WebBinding -Name $Name -Protocol https -Port 443 -HostHeader $HostHeader -ErrorAction SilentlyContinue
    if ($null -eq $binding) {
        New-WebBinding -Name $Name -Protocol https -Port 443 -HostHeader $HostHeader | Out-Null
        $binding = Get-WebBinding -Name $Name -Protocol https -Port 443 -HostHeader $HostHeader
    }

    $binding.AddSslCertificate($Thumbprint, "My")
}

function Ensure-Firewall {
    if (-not (Get-NetFirewallRule -DisplayName "Geneys IIS HTTP Inbound" -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName "Geneys IIS HTTP Inbound" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 80 | Out-Null
    }

    if (-not (Get-NetFirewallRule -DisplayName "Geneys IIS HTTPS Inbound" -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName "Geneys IIS HTTPS Inbound" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 443 | Out-Null
    }
}

Assert-Admin
Assert-IisModule
Ensure-PhysicalPath -Path $PhysicalPath
Enable-ArrProxy
Ensure-AppPool -PoolName $SiteName
Ensure-Site -Name $SiteName -Path $PhysicalPath -Pool $SiteName -HostHeader $HostName
Ensure-WebConfig -TargetPath $PhysicalPath -Port $AppPort

if ($EnableHttps) {
    Ensure-HttpsBinding -Name $SiteName -HostHeader $HostName -Thumbprint $CertThumbprint
}

if ($OpenFirewall) {
    Ensure-Firewall
}

Write-Host "IIS reverse proxy kurulumu tamamlandi."
Write-Host "Site: $SiteName"
Write-Host "Host: $HostName"
Write-Host "Target: http://127.0.0.1:$AppPort"
