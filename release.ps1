# release.ps1 - build all six platforms and package v2rayN-compatible release zips.
#
# v2rayN core-update contract (zhdata registered as a first-class core):
#   - asset names are exactly zhdata-windows-64.zip / zhdata-windows-arm64.zip /
#     zhdata-linux-64.zip  / zhdata-linux-arm64.zip  / zhdata-macos-64.zip  / zhdata-macos-arm64.zip
#     (GOOS darwin -> "macos", GOARCH amd64 -> "64")
#   - every zip has a SINGLE top-level directory "zhdata/" containing "zhdata(.exe)",
#     so unzipping into bin/ yields exactly bin/zhdata/zhdata(.exe).
# Run: pwsh -File release.ps1
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$root  = $PSScriptRoot
$dist  = Join-Path $root "dist"
$rel   = Join-Path $dist "release"
$stage = Join-Path ([System.IO.Path]::GetTempPath()) ("zhdata-stage-" + [guid]::NewGuid().ToString("N"))

# 1) full six-platform build
& (Join-Path $root "build.ps1")

New-Item -ItemType Directory -Force -Path $rel | Out-Null

$targets = @(
    @{GOOS="windows"; GOARCH="amd64"; Asset="windows-64"},
    @{GOOS="windows"; GOARCH="arm64"; Asset="windows-arm64"},
    @{GOOS="linux";   GOARCH="amd64"; Asset="linux-64"},
    @{GOOS="linux";   GOARCH="arm64"; Asset="linux-arm64"},
    @{GOOS="darwin";  GOARCH="amd64"; Asset="macos-64"},
    @{GOOS="darwin";  GOARCH="arm64"; Asset="macos-arm64"}
)

# unix mode 0100755 in the upper 16 bits of the zip entry's external attributes
# (0o100755 = 0x81ED, shifted << 16 = 0x81ED0000; -shl on Int32 wraps, no cast overflow).
$UnixMode0755 = [int]0x81ED -shl 16

function New-CoreZip {
    param(
        [Parameter(Mandatory = $true)][string] $BinPath,
        [Parameter(Mandatory = $true)][string] $ZipPath,
        [Parameter(Mandatory = $true)][string] $EntryName,
        [Parameter(Mandatory = $true)][bool]   $IsWindowsBinary
    )
    if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
    $fileStream = [System.IO.File]::Open($ZipPath, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write)
    try {
        $zip = New-Object System.IO.Compression.ZipArchive($fileStream, [System.IO.Compression.ZipArchiveMode]::Create)
        try {
            $entry = $zip.CreateEntry($EntryName, [System.IO.Compression.CompressionLevel]::Optimal)
            if (-not $IsWindowsBinary) { $entry.ExternalAttributes = $UnixMode0755 }
            $in  = [System.IO.File]::OpenRead($BinPath)
            $out = $entry.Open()
            try { $in.CopyTo($out) } finally { $out.Dispose(); $in.Dispose() }
        } finally { $zip.Dispose() }
    } finally { $fileStream.Dispose() }
}

try {
    foreach ($t in $targets) {
        $ext = if ($t.GOOS -eq "windows") { ".exe" } else { "" }
        $bin = Join-Path $dist ("zhdata-{0}-{1}{2}" -f $t.GOOS, $t.GOARCH, $ext)
        if (-not (Test-Path $bin)) { throw "missing build output: $bin" }

        # staging layout: <stage>/zhdata/zhdata(.exe)
        $pkgDir = Join-Path $stage $t.Asset
        New-Item -ItemType Directory -Force -Path (Join-Path $pkgDir "zhdata") | Out-Null
        Copy-Item $bin (Join-Path $pkgDir ("zhdata/zhdata{0}" -f $ext)) -Force

        $zipPath   = Join-Path $rel ("zhdata-{0}.zip" -f $t.Asset)
        $entryName = "zhdata/zhdata{0}" -f $ext
        New-CoreZip -BinPath $bin -ZipPath $zipPath -EntryName $entryName -IsWindowsBinary ($t.GOOS -eq "windows")

        Write-Host ("packed {0}  (entry: {1})" -f $zipPath, $entryName)
    }
} finally {
    Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "release zips in dist/release/:"
Get-ChildItem $rel -Filter "zhdata-*.zip" | ForEach-Object { Write-Host ("  {0}  {1:N0} bytes" -f $_.Name, $_.Length) }
