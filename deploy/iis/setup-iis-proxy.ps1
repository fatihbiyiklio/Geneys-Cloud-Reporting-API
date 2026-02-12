param(
    [string]$SiteName = "GeneysReporting",
    [string]$HostName = "",
    [string]$PhysicalPath = "C:\\inetpub\\geneys-proxy",
    [int]$AppPort = 8501,
    [int]$HttpPort = 80,
    [int]$HttpsPort = 443,
    [switch]$EnableHttps,
    [string]$CertThumbprint = "",
    [switch]$OpenFirewall,
    [switch]$ShowBindings
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

function Ensure-Site([string]$Name, [string]$Path, [string]$Pool, [string]$HostHeader, [int]$Port) {
    $existing = Get-Website -Name $Name -ErrorAction SilentlyContinue
    if ($null -eq $existing) {
        if ([string]::IsNullOrWhiteSpace($HostHeader)) {
            New-Website -Name $Name -PhysicalPath $Path -Port $Port -ApplicationPool $Pool | Out-Null
        }
        else {
            New-Website -Name $Name -PhysicalPath $Path -Port $Port -HostHeader $HostHeader -ApplicationPool $Pool | Out-Null
        }
    }
    else {
        Set-ItemProperty "IIS:\\Sites\\$Name" -Name physicalPath -Value $Path
        Set-ItemProperty "IIS:\\Sites\\$Name" -Name applicationPool -Value $Pool
    }
}

function Get-SiteBindingSummary([string]$Name, [string]$Protocol) {
    $bindings = @(Get-WebBinding -Name $Name -Protocol $Protocol -ErrorAction SilentlyContinue)
    if ($bindings.Count -eq 0) {
        return "(none)"
    }
    return ($bindings | ForEach-Object { $_.bindingInformation } | Sort-Object) -join ", "
}

function Ensure-HttpBinding([string]$Name, [string]$HostHeader, [int]$Port) {
    $bindings = @(Get-WebBinding -Name $Name -Protocol "http" -ErrorAction SilentlyContinue)
    $desired = "*:$Port:$HostHeader"

    $hasDesired = $false
    foreach ($b in $bindings) {
        if ($b.bindingInformation -eq $desired) {
            $hasDesired = $true
            break
        }
    }

    if (-not $hasDesired) {
        try {
            if ([string]::IsNullOrWhiteSpace($HostHeader)) {
                New-WebBinding -Name $Name -Protocol http -Port $Port | Out-Null
            }
            else {
                New-WebBinding -Name $Name -Protocol http -Port $Port -HostHeader $HostHeader | Out-Null
            }
        }
        catch {
            $current = Get-SiteBindingSummary -Name $Name -Protocol "http"
            throw "HTTP binding eklenemedi. Site: $Name, hedef: $desired, mevcut: $current"
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($HostHeader)) {
        $wildcard = @($bindings | Where-Object { $_.bindingInformation -eq "*:$Port:" })
        if ($wildcard.Count -gt 0) {
            Write-Warning "Site '$Name' icin wildcard http binding (*:$Port:) var. Host header bazli kullanim icin kaldirmayi degerlendirin."
        }
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

function Ensure-HttpsBinding([string]$Name, [string]$HostHeader, [string]$Thumbprint, [int]$Port) {
    if ([string]::IsNullOrWhiteSpace($Thumbprint)) {
        throw "-EnableHttps kullanildiginda -CertThumbprint zorunludur."
    }

    $binding = Get-WebBinding -Name $Name -Protocol https -Port $Port -HostHeader $HostHeader -ErrorAction SilentlyContinue
    if ($null -eq $binding) {
        if ([string]::IsNullOrWhiteSpace($HostHeader)) {
            New-WebBinding -Name $Name -Protocol https -Port $Port | Out-Null
        }
        else {
            New-WebBinding -Name $Name -Protocol https -Port $Port -HostHeader $HostHeader -SslFlags 1 | Out-Null
        }
        $binding = Get-WebBinding -Name $Name -Protocol https -Port $Port -HostHeader $HostHeader
    }

    $binding.AddSslCertificate($Thumbprint, "My")
}

function Ensure-Firewall([int]$Http, [int]$Https) {
    if (-not (Get-NetFirewallRule -DisplayName "Geneys IIS HTTP Inbound" -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName "Geneys IIS HTTP Inbound" -Direction Inbound -Action Allow -Protocol TCP -LocalPort $Http | Out-Null
    }

    if (-not (Get-NetFirewallRule -DisplayName "Geneys IIS HTTPS Inbound" -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName "Geneys IIS HTTPS Inbound" -Direction Inbound -Action Allow -Protocol TCP -LocalPort $Https | Out-Null
    }
}

function Show-IisBindings {
    $appcmd = Join-Path $env:windir "System32\\inetsrv\\appcmd.exe"
    if (Test-Path $appcmd) {
        Write-Host ""
        Write-Host "Tum IIS site bindingleri:"
        & $appcmd list site /text:name,bindings
        return
    }

    Get-Website | ForEach-Object {
        Write-Host ("{0}: {1}" -f $_.Name, $_.Bindings.Collection.bindingInformation)
    }
}

Assert-Admin
Assert-IisModule
Ensure-PhysicalPath -Path $PhysicalPath
Enable-ArrProxy
Ensure-AppPool -PoolName $SiteName
Ensure-Site -Name $SiteName -Path $PhysicalPath -Pool $SiteName -HostHeader $HostName -Port $HttpPort
Ensure-HttpBinding -Name $SiteName -HostHeader $HostName -Port $HttpPort
Ensure-WebConfig -TargetPath $PhysicalPath -Port $AppPort

if ($EnableHttps) {
    Ensure-HttpsBinding -Name $SiteName -HostHeader $HostName -Thumbprint $CertThumbprint -Port $HttpsPort
}

if ($OpenFirewall) {
    Ensure-Firewall -Http $HttpPort -Https $HttpsPort
}

if ($ShowBindings) {
    Show-IisBindings
}

Write-Host "IIS reverse proxy kurulumu tamamlandi."
Write-Host "Site: $SiteName"
Write-Host "Host: $HostName"
Write-Host "HTTP: $HttpPort"
if ($EnableHttps) { Write-Host "HTTPS: $HttpsPort" }
Write-Host "Target: http://127.0.0.1:$AppPort"
