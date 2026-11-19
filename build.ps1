# build.ps1 - cross-compile zhdata (v2rayN first-class core) for all target platforms
# Binary name is exactly "zhdata" so the Windows process name matches CoreExes = ["zhdata"].
# Output: dist/zhdata-<os>-<arch>[.exe]   (release zips use the amd64 -> 64 mapping, see release.ps1)
$ErrorActionPreference = "Stop"
$env:CGO_ENABLED = "0"
$targets = @(
    @{GOOS="windows"; GOARCH="amd64"},
    @{GOOS="windows"; GOARCH="arm64"},
    @{GOOS="darwin";  GOARCH="amd64"},
    @{GOOS="darwin";  GOARCH="arm64"},
    @{GOOS="linux";   GOARCH="amd64"},
    @{GOOS="linux";   GOARCH="arm64"}
)
New-Item -ItemType Directory -Force -Path dist | Out-Null
foreach ($t in $targets) {
    $ext = if ($t.GOOS -eq "windows") { ".exe" } else { "" }
    $out = "dist/zhdata-$($t.GOOS)-$($t.GOARCH)$ext"
    $env:GOOS = $t.GOOS
    $env:GOARCH = $t.GOARCH
    go build -trimpath -ldflags "-s -w" -o $out .
    Write-Host "built $out"
}
$env:GOOS = ""; $env:GOARCH = ""
