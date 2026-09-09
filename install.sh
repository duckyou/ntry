#!/usr/bin/env bash
set -euo pipefail

repo=duckyou/ntry
install_dir=${NTRY_INSTALL_DIR:-"${HOME:?HOME must be set}/.local/bin"}
version=${NTRY_VERSION:-latest}

case "$(uname -s)/$(uname -m)" in
  Linux/x86_64) asset=ntry-linux-x86_64.tar.gz ;;
  Darwin/arm64) asset=ntry-macos-arm64.tar.gz ;;
  *)
    printf 'ntry: unsupported platform: %s/%s\n' "$(uname -s)" "$(uname -m)" >&2
    exit 1
    ;;
esac

case "$version" in
  latest) base_url="https://github.com/$repo/releases/latest/download" ;;
  v[0-9]*)
    case "$version" in
      *[!A-Za-z0-9._-]*)
        printf 'ntry: invalid version: %s\n' "$version" >&2
        exit 1
        ;;
    esac
    base_url="https://github.com/$repo/releases/download/$version"
    ;;
  *)
    printf 'ntry: version must be latest or start with v: %s\n' "$version" >&2
    exit 1
    ;;
esac

for command in curl tar mktemp; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf 'ntry: required command not found: %s\n' "$command" >&2
    exit 1
  fi
done

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/ntry-install.XXXXXX")
tmp_binary=
cleanup() {
  rm -rf "$tmp_dir"
  if [[ -n "$tmp_binary" ]]; then
    rm -f "$tmp_binary"
  fi
}
trap cleanup EXIT

curl -fsSL "$base_url/$asset" -o "$tmp_dir/$asset"
curl -fsSL "$base_url/SHA256SUMS" -o "$tmp_dir/SHA256SUMS"

expected=$(
  while read -r checksum filename; do
    if [[ ${filename#\*} == "$asset" ]]; then
      printf '%s\n' "$checksum"
    fi
  done < "$tmp_dir/SHA256SUMS"
)

if [[ ! $expected =~ ^[[:xdigit:]]{64}$ ]]; then
  printf 'ntry: no valid checksum found for %s\n' "$asset" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp_dir/$asset")
else
  actual=$(shasum -a 256 "$tmp_dir/$asset")
fi
actual=${actual%% *}

if [[ $actual != "$expected" ]]; then
  printf 'ntry: checksum verification failed for %s\n' "$asset" >&2
  exit 1
fi

tar -xzf "$tmp_dir/$asset" -C "$tmp_dir" ntry
mkdir -p "$install_dir"
tmp_binary=$(mktemp "$install_dir/.ntry.XXXXXX")
install -m 755 "$tmp_dir/ntry" "$tmp_binary"
mv -f "$tmp_binary" "$install_dir/ntry"
tmp_binary=

printf 'Installed ntry to %s/ntry\n' "$install_dir"
case ":${PATH:-}:" in
  *":$install_dir:"*) ;;
  *) printf 'Warning: %s is not on PATH.\n' "$install_dir" >&2 ;;
esac
