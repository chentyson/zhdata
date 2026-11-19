#!/usr/bin/env bash
# release.sh - build all six platforms and package v2rayN-compatible release zips.
#
# v2rayN core-update contract (zhdata registered as a first-class core):
#   - asset names are exactly zhdata-windows-64.zip / zhdata-windows-arm64.zip /
#     zhdata-linux-64.zip  / zhdata-linux-arm64.zip  / zhdata-macos-64.zip  / zhdata-macos-arm64.zip
#     (GOOS darwin -> "macos", GOARCH amd64 -> "64")
#   - every zip has a SINGLE top-level directory "zhdata/" containing "zhdata(.exe)",
#     so unzipping into bin/ yields exactly bin/zhdata/zhdata(.exe).
# Run: bash release.sh   (used by .github/workflows/release.yml on ubuntu-latest)
set -e
cd "$(cd "$(dirname "$0")" && pwd)"

# 1) full six-platform build
bash build.sh

mkdir -p dist/release
stage="$(mktemp -d "${TMPDIR:-/tmp}/zhdata-stage.XXXXXX")"
trap 'rm -rf "$stage"' EXIT

# "<goos>/<goarch>/<asset-suffix>"
for t in windows/amd64/windows-64 windows/arm64/windows-arm64 \
         linux/amd64/linux-64   linux/arm64/linux-arm64 \
         darwin/amd64/macos-64  darwin/arm64/macos-arm64; do
  os="${t%%/*}"; rest="${t#*/}"; arch="${rest%%/*}"; asset="${rest#*/}"
  ext=""; [ "$os" = "windows" ] && ext=".exe"
  bin="dist/zhdata-$os-$arch$ext"
  [ -f "$bin" ] || { echo "missing build output: $bin" >&2; exit 1; }

  # staging layout: <stage>/zhdata/zhdata(.exe)
  pkg="$stage/pkg"; rm -rf "$pkg"; mkdir -p "$pkg/zhdata"
  cp "$bin" "$pkg/zhdata/zhdata$ext"
  chmod 755 "$pkg/zhdata/zhdata$ext"

  zipfile="$PWD/dist/release/zhdata-$asset.zip"
  rm -f "$zipfile"
  if command -v zip >/dev/null 2>&1; then
    # ubuntu-latest has zip: preserves the 0755 mode from chmod above
    ( cd "$pkg" && zip -X -q -r "$zipfile" "zhdata" )
  elif command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1; then
    # fallback: write the entry by hand so the unix mode is deterministic (0100755)
    py="$(command -v python3 || command -v python)"
    "$py" - "$bin" "$zipfile" "zhdata/zhdata$ext" <<'PY'
import sys, zipfile
src, dst, name = sys.argv[1], sys.argv[2], sys.argv[3]
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
    zi = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    zi.compress_type = zipfile.ZIP_DEFLATED
    zi.external_attr = (0o100755 << 16)
    with open(src, "rb") as f:
        z.writestr(zi, f.read())
PY
  else
    echo "no zip/python3 available to build $zipfile" >&2
    exit 1
  fi
  echo "packed dist/release/zhdata-$asset.zip  (entry: zhdata/zhdata$ext)"
done

echo
echo "release zips in dist/release/:"
ls -l dist/release/zhdata-*.zip
