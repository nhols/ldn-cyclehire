#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

upload_dir="${R2_UPLOAD_DIR:-/tmp/cyclehire-r2-upload}"
r2_bucket="${R2_BUCKET:-ldn-cyclehire-data}"
r2_account_id="${R2_ACCOUNT_ID:-}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-${R2_ACCESS_KEY_ID:-}}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-${R2_SECRET_ACCESS_KEY:-}}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}"

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Install it with: brew install awscli" >&2
  exit 1
fi

if [[ -z "$r2_account_id" ]]; then
  echo "R2_ACCOUNT_ID is required. Add it to .env or pass R2_ACCOUNT_ID=..." >&2
  exit 1
fi

if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
  echo "R2_ACCESS_KEY_ID is required in .env" >&2
  exit 1
fi

if [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
  echo "R2_SECRET_ACCESS_KEY is required in .env" >&2
  exit 1
fi

if [[ ! -d "$upload_dir" ]]; then
  echo "Upload directory not found: $upload_dir" >&2
  exit 1
fi

if [[ ! -f "$upload_dir/manifest.json" ]]; then
  echo "Prepared manifest not found: $upload_dir/manifest.json" >&2
  exit 1
fi

endpoint="https://${r2_account_id}.r2.cloudflarestorage.com"
target="s3://${r2_bucket}"

aws s3 sync "$upload_dir" "$target" \
  --endpoint-url "$endpoint" \
  --content-type application/json \
  --content-encoding gzip \
  --cache-control "public, max-age=31536000, immutable" \
  --only-show-errors

aws s3 cp "$upload_dir/manifest.json" "${target}/manifest.json" \
  --endpoint-url "$endpoint" \
  --content-type application/json \
  --content-encoding gzip \
  --cache-control "public, max-age=300" \
  --only-show-errors

echo "Uploaded $upload_dir to $target"
