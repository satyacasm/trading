#!/usr/bin/env bash
# Re-download format samples into data/raw/_recon/ (gitignored).
set -euo pipefail
OUT="$(git rev-parse --show-toplevel)/data/raw/_recon"; mkdir -p "$OUT"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
D="${1:-20260813}"
JAR="$(mktemp)"
curl -s -c "$JAR" -A "$UA" "https://www.nseindia.com" -o /dev/null
for SEG in cm fo; do
  U=$(echo "$SEG" | tr '[:lower:]' '[:upper:]')
  curl -s -b "$JAR" -A "$UA" -H "Referer: https://www.nseindia.com/" \
    "https://nsearchives.nseindia.com/content/${SEG}/BhavCopy_NSE_${U}_0_0_0_${D}_F_0000.csv.zip" \
    -o "$OUT/nse_${SEG}_udiff_${D}.zip"
done
curl -sL -A "$UA" -H "Referer: https://www.bseindia.com/" \
  "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_${D}_F_0000.CSV" \
  -o "$OUT/bse_cm_udiff_${D}.csv"
curl -sL "https://portal.amfiindia.com/spages/NAVAll.txt" -o "$OUT/amfi_navall_${D}.txt"
echo "samples written to $OUT"
