$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

function Stop-GradleAndKotlinDaemons {
    Write-Step "Stopping Gradle daemons"
    try {
        & .\gradlew.bat --stop | Out-Host
    } catch {
        Write-Host "gradlew --stop failed, continuing..." -ForegroundColor Yellow
    }

    Write-Step "Stopping Java Gradle/Kotlin daemon processes"
    try {
        $javaProcs = Get-CimInstance Win32_Process -Filter "Name='java.exe' OR Name='javaw.exe'"
        foreach ($p in $javaProcs) {
            $cmd = [string]$p.CommandLine
            if ($cmd -match "GradleDaemon|KotlinCompileDaemon|org\.gradle\.launcher\.daemon") {
                try {
                    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
                } catch { }
            }
        }
    } catch {
        Write-Host "Could not inspect Java processes, continuing..." -ForegroundColor Yellow
    }
}

function Clear-BuildLocks {
    param(
        [Parameter(Mandatory = $true)][string]$PathToClear
    )
    if (!(Test-Path $PathToClear)) { return }

    Write-Step "Clearing attributes on $PathToClear"
    cmd /c attrib -R "$PathToClear\*" /S /D | Out-Null

    Write-Step "Deleting build directory with retries"
    $maxTries = 8
    for ($i = 1; $i -le $maxTries; $i++) {
        try {
            if (Test-Path $PathToClear) {
                Remove-Item $PathToClear -Recurse -Force -ErrorAction Stop
            }
            Write-Host "Build directory removed." -ForegroundColor Green
            return
        } catch {
            Write-Host "Try $i/$maxTries failed, waiting..." -ForegroundColor Yellow
            Start-Sleep -Seconds 2
            cmd /c attrib -R "$PathToClear\*" /S /D | Out-Null
        }
    }
    throw "Unable to delete $PathToClear after retries."
}

$javaHome = "C:\Program Files\Android\Android Studio\jbr"
if (Test-Path "$javaHome\bin\java.exe") {
    $env:JAVA_HOME = $javaHome
    $env:Path = "$javaHome\bin;$env:Path"
}

Stop-GradleAndKotlinDaemons
Clear-BuildLocks -PathToClear ".\app\build"

Write-Step "Building debug APK"
& .\gradlew.bat --no-daemon --stacktrace assembleDebug | Out-Host

Write-Step "Done"
