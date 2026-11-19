#!/usr/bin/env bash
# build.sh - same matrix as build.ps1 (run from repo root on any dev machine)
# Binary name is exactly "zhdata" (v2rayN CoreExes); output dist/zhdata-<os>-<arch>[.exe].
set -e
export CGO_ENABLED=0
mkdir -p dist
for t in windows/amd64 windows/arm64 darwin/amd64 darwin/arm64 linux/amd64 linux/arm64; do
  os="${t%/*}"; arch="${t#*/}"
  ext=""; [ "$os" = "windows" ] && ext=".exe"
  GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags "-s -w" -o "dist/zhdata-$os-$arch$ext" .
  echo "built dist/zhdata-$os-$arch$ext"
done
