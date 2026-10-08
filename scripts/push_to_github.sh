#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <git-repository-url>"
  exit 1
fi
git init
git add .
git commit -m "Build university chapters medallion data product"
git branch -M main
git remote add origin "$1"
git push -u origin main
