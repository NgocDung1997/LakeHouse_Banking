#!/usr/bin/env bash
# Generate Trino authentication secrets from .env (run from the project root, Git Bash / Linux / macOS):
#   bash scripts/trino_auth_setup.sh
#
# Creates (git-ignored) in conf/trino/secrets/:
#   trino.pem    self-signed TLS cert + key for HTTPS on :8443 (only if missing)
#   password.db  bcrypt "user:hash" line for TRINO_USER / TRINO_PASSWORD (always regenerated)
# and fills TRINO_SHARED_SECRET in .env if it is still a placeholder.
#
# Then apply with:  docker compose up -d --force-recreate trino-dungafk
set -euo pipefail
export MSYS_NO_PATHCONV=1   # stop Git Bash from rewriting "/CN=..." into a Windows path

cd "$(dirname "$0")/.."
[ -f .env ] || { echo "ERROR: .env not found (copy .env.example first)"; exit 1; }

get_env() { grep -E "^$1=" .env | tail -1 | cut -d= -f2-; }
TRINO_USER="$(get_env TRINO_USER)"
TRINO_PASSWORD="$(get_env TRINO_PASSWORD)"
SECRET="$(get_env TRINO_SHARED_SECRET)"
[ -n "$TRINO_USER" ] && [ -n "$TRINO_PASSWORD" ] || { echo "ERROR: TRINO_USER / TRINO_PASSWORD missing in .env"; exit 1; }

# 1. Shared secret for internal communication
if [ -z "$SECRET" ] || [ "$SECRET" = "__GENERATED__" ] || [ "$SECRET" = "change-me" ]; then
  NEW_SECRET="$(openssl rand -base64 32 | tr -d '\n=+/')"
  sed -i "s|^TRINO_SHARED_SECRET=.*|TRINO_SHARED_SECRET=${NEW_SECRET}|" .env
  echo "✔ TRINO_SHARED_SECRET generated in .env"
fi

mkdir -p conf/trino/secrets

# 2. Self-signed certificate (valid 10 years) for localhost and the container name
# (relative paths only: with MSYS_NO_PATHCONV, Windows openssl cannot resolve /tmp)
if [ ! -f conf/trino/secrets/trino.pem ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout conf/trino/secrets/key.tmp -out conf/trino/secrets/cert.tmp -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:trino-dungafk,IP:127.0.0.1" 2>/dev/null
  cat conf/trino/secrets/cert.tmp conf/trino/secrets/key.tmp > conf/trino/secrets/trino.pem
  rm -f conf/trino/secrets/cert.tmp conf/trino/secrets/key.tmp
  echo "✔ conf/trino/secrets/trino.pem created"
fi

# 3. Password file (bcrypt, cost 10) — htpasswd from the official httpd image
HTTPD_IMAGE=httpd:2.4-alpine
had_image=0; docker image inspect "$HTTPD_IMAGE" >/dev/null 2>&1 && had_image=1
docker run --rm "$HTTPD_IMAGE" htpasswd -nbB -C 10 "$TRINO_USER" "$TRINO_PASSWORD" \
  | tr -d '\r' | grep -v '^$' > conf/trino/secrets/password.db
echo "✔ conf/trino/secrets/password.db written for user '$TRINO_USER'"
# Remove the helper image again if this script pulled it (keeps Docker storage clean)
[ "$had_image" = 1 ] || docker image rm "$HTTPD_IMAGE" >/dev/null && echo "✔ removed helper image $HTTPD_IMAGE"
