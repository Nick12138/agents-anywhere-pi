# 把 PiAbyss 已安装的 anywhere-cli 里的 connector 包换成指向本地仓库的目录联接（junction）。
#
# 作用：以后改 D:\...\agents-anywhere-pi\connector\connector\**.py 立刻就是运行时代码，
# 只要在 PiAbyss 里「停止 → 启动」一下就生效，不用再点「重装」。
#
# 用法（在仓库根目录）：
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup-local-connector.ps1
#   powershell ... -File .\scripts\setup-local-connector.ps1 -UseLocalSource
#
# -UseLocalSource 会顺便把 PiAbyss 的 connectorSource 改成这个本地目录（之后点「重装」也从本地装）。
#
# 前提：先在 PiAbyss → 设置 → 远程控制 里完成一次「重装」（远程 git 源即可），
#       并在重装前把 connector 停掉（否则文件被占用）。

[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [switch]$UseLocalSource,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

$connectorDir = Join-Path $RepoRoot "connector"
$packageDir = Join-Path $connectorDir "connector"
if (-not (Test-Path (Join-Path $packageDir "__init__.py"))) {
    throw "没找到 $packageDir，-RepoRoot 指向的应该是 agents-anywhere-pi 仓库根目录。"
}

$installDir = Join-Path $env:USERPROFILE ".pi\agent\piabyss\remote"
$toolDir = Join-Path $installDir "tools\anywhere-cli"
if (-not (Test-Path $toolDir)) {
    throw "没找到 $toolDir。请先在 PiAbyss → 远程控制 → 重装（connectorSource 用 git+https 源）。"
}

# site-packages 可能在 Lib 下，也可能在 Lib\python3.x 下
$sitePackages = $null
foreach ($candidate in @((Join-Path $toolDir "Lib\site-packages"), (Join-Path $toolDir "lib"))) {
    if (Test-Path $candidate) { $sitePackages = $candidate; break }
}
if (-not $sitePackages) {
    $libDir = Join-Path $toolDir "Lib"
    if (Test-Path $libDir) {
        $sitePackages = Get-ChildItem $libDir -Directory |
            Where-Object { $_.Name -like "python*" } |
            ForEach-Object { Join-Path $_.FullName "site-packages" } |
            Where-Object { Test-Path $_ } |
            Select-Object -First 1
    }
}
if (-not $sitePackages) {
    throw "没找到 site-packages（$toolDir 里没有 Lib\site-packages）。"
}

$target = Join-Path $sitePackages "connector"
if (Test-Path $target) {
    $item = Get-Item $target -Force
    $isLink = [bool]($item.Attributes -band [IO.FileAttributes]::ReparsePoint)
    if ($isLink -and $item.Target -and ($item.Target -contains $packageDir -or $item.Target -eq $packageDir)) {
        Write-Host "已经指向本地仓库，无需处理：$target -> $packageDir"
    }
    else {
        $running = Get-Process -Name "anywhere-cli" -ErrorAction SilentlyContinue
        if ($running -and -not $Force) {
            throw "connector 正在运行（pid=$($running.Id -join ',')）。请先在 PiAbyss 里「停止」，再执行本脚本。"
        }
        Write-Host "移除旧的 connector 目录：$target"
        Remove-Item $target -Recurse -Force
        cmd /c mklink /J "$target" "$packageDir" | Out-Null
        Write-Host "已创建目录联接：$target -> $packageDir"
    }
}
else {
    cmd /c mklink /J "$target" "$packageDir" | Out-Null
    Write-Host "已创建目录联接：$target -> $packageDir"
}

if ($UseLocalSource) {
    $settingsPath = Join-Path $installDir "settings.json"
    if (Test-Path $settingsPath) {
        $settings = Get-Content $settingsPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $settings.connectorSource = $connectorDir
        # 必须写成无 BOM 的 UTF-8：PiAbyss 用 JSON.parse 读，BOM 会让它解析失败并退回默认设置。
        $json = ($settings | ConvertTo-Json -Depth 10) + "`n"
        [IO.File]::WriteAllText($settingsPath, $json, (New-Object System.Text.UTF8Encoding($false)))
        Write-Host "connectorSource 已改为：$connectorDir"
    }
    else {
        Write-Warning "没找到 $settingsPath，跳过 connectorSource（先在 PiAbyss 里打开一次远程控制页再试）。"
    }
}

Write-Host ""
Write-Host "完成。接下来：PiAbyss → 设置 → 远程控制 → 启动。"
Write-Host "以后改本地代码只需「停止 → 启动」，不要再点「重装」（重装会把联接换回普通拷贝）。"
