#!/usr/bin/env bash
set -euo pipefail

cdn_dir="${CDN_DIR:-data/cdn}"
r2_bucket="${R2_BUCKET:-ldn-cyclehire-data}"
jobs="${JOBS:-32}"

if [[ ! -d "$cdn_dir" ]]; then
  echo "CDN directory not found: $cdn_dir" >&2
  exit 1
fi

if [[ ! -f "$cdn_dir/manifest.json.gz" ]]; then
  echo "Compressed manifest not found: $cdn_dir/manifest.json.gz" >&2
  exit 1
fi

export cdn_dir r2_bucket

find "$cdn_dir" -name '*.json.gz' -type f ! -name 'manifest.json.gz' -print0 |
  xargs -0 -n 1 -P "$jobs" bash -c '
    file="$1"
    key="${file#"$cdn_dir"/}"
    key="${key%.gz}"

    echo "Uploading $key"

    npx wrangler r2 object put "$r2_bucket/$key" \
      --remote \
      --file "$file" \
      --content-type application/json \
      --content-encoding gzip \
      --cache-control "public, max-age=31536000, immutable"
  ' _

# Upload manifest last
npx wrangler r2 object put "$r2_bucket/manifest.json" \
  --remote \
  --file "$cdn_dir/manifest.json.gz" \
  --content-type application/json \
  --content-encoding gzip \
  --cache-control "public, max-age=300"