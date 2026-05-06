#!/usr/bin/env bash
set -euo pipefail

cdn_dir="${CDN_DIR:-data/cdn}"
upload_dir="${R2_UPLOAD_DIR:-/tmp/cyclehire-r2-upload}"

if [[ ! -d "$cdn_dir" ]]; then
  echo "CDN directory not found: $cdn_dir" >&2
  exit 1
fi

if [[ ! -f "$cdn_dir/manifest.json.gz" ]]; then
  echo "Compressed manifest not found: $cdn_dir/manifest.json.gz" >&2
  exit 1
fi

if [[ -z "$upload_dir" || "$upload_dir" == "/" || "$upload_dir" == "$cdn_dir" ]]; then
  echo "Refusing unsafe upload directory: $upload_dir" >&2
  exit 1
fi

rm -rf "$upload_dir"
mkdir -p "$upload_dir"

count=0
while IFS= read -r -d '' file; do
  key="${file#"$cdn_dir"/}"
  key="${key%.gz}"
  dest="$upload_dir/$key"

  mkdir -p "$(dirname "$dest")"
  cp "$file" "$dest"
  count=$((count + 1))
done < <(find "$cdn_dir" -name '*.json.gz' -type f -print0)

echo "Prepared $count gzip-encoded JSON objects in $upload_dir"
du -sh "$upload_dir"
