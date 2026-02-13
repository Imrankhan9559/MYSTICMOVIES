$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$javaHome = "C:\Program Files\Android\Android Studio\jbr"
if (Test-Path "$javaHome\bin\java.exe") {
    $env:JAVA_HOME = $javaHome
    $env:Path = "$javaHome\bin;$env:Path"
}

Write-Host "Stopping Gradle daemons..."
& .\gradlew.bat --stop | Out-Host

Write-Host "Clearing read-only flags on app/build..."
if (Test-Path ".\app\build") {
    cmd /c attrib -R ".\app\build\*" /S /D | Out-Host
}

Write-Host "Building debug APK..."
& .\gradlew.bat assembleDebug | Out-Host

Write-Host "Done."
