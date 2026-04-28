#!/usr/bin/env bash
#
# falcon-creds.sh — pull AWS credentials via `falcon credentials exec`
# and emit `export KEY=VALUE` lines for eval in the current shell.
#
# Steps:
#   1. Ensure the `dev` falcon context is active   (`falcon context apply dev`).
#   2. Run `falcon credentials exec` to capture AWS_*/VAULT_* vars.
#   3. If that fails because there is no approved credential (expired /
#      not yet requested), run `falcon credentials request` and retry.
#   4. Print the vars as shell-safe `export` lines.
#
# Note: AWS_SESSION_TOKEN is ~1 KB and falcon's output path line-wraps
# at terminal width, which corrupts the token on the way back to the
# parent shell. To avoid that, we base64-encode each value inside the
# subshell and decode here before printing.
#
# Usage:
#   eval "$(./deploy/aws/falcon-creds.sh)"
#   aws sts get-caller-identity           # now works
#   ./deploy/aws/ecr-push.sh              # now works
#
set -euo pipefail

log() { printf '[falcon-creds] %s\n' "$*" >&2; }

command -v falcon >/dev/null 2>&1 || {
  log "error: 'falcon' CLI not found on PATH."
  log "       install from https://sfdc.co/falcon-paved-path"
  exit 127
}

# ---- 1. apply dev context (idempotent) -----------------------------------
log "applying falcon context 'dev'..."
falcon context apply dev >&2 || {
  log "error: 'falcon context apply dev' failed"
  exit 1
}

# ---- snippet run inside the falcon-exec subshell -------------------------
# Emits "KEY B64VALUE" pairs, one per line. Using base64 because
# falcon wraps stdout and would otherwise insert literal newlines
# mid-value into long tokens (AWS_SESSION_TOKEN is ~1 KB).
read -r -d '' EMIT <<'INNER' || true
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN \
         AWS_SECURITY_TOKEN AWS_DEFAULT_REGION AWS_REGION \
         AWS_ACCOUNT_ID VAULT_ADDR VAULT_TOKEN VAULT_NAMESPACE; do
  val="${!v-}"
  [ -z "$val" ] && continue
  # -w 0 flag differs between GNU coreutils and BSD base64; use tr to be safe.
  b64=$(printf %s "$val" | base64 | tr -d '\r\n ')
  printf 'CRED %s %s\n' "$v" "$b64"
done
exit
INNER

run_exec() {
  SHELL=/bin/bash falcon credentials exec <<EOF
$EMIT
EOF
}

# ---- 2. try existing approved credential ---------------------------------
log "calling 'falcon credentials exec'..."
out=$(run_exec 2>/tmp/falcon-creds.err) || out=""

if ! printf '%s\n' "$out" | grep -qE '^CRED AWS_'; then
  # ---- 3. no approved creds -> request + retry --------------------------
  log "no approved credentials (or expired); calling 'falcon credentials request'..."
  cat /tmp/falcon-creds.err >&2 || true
  falcon credentials request >&2 || {
    log "error: 'falcon credentials request' failed"
    exit 1
  }
  log "retrying 'falcon credentials exec'..."
  out=$(run_exec) || {
    log "error: 'falcon credentials exec' still failing after request"
    exit 1
  }
fi

# ---- 4. decode and emit export lines -------------------------------------
# falcon prepends ANSI colour escapes to stdout lines and wraps long
# lines at terminal width. Strip ANSI, then grep for 'CRED KEY B64' —
# the b64 body may have been split across wrap boundaries, so also
# collapse whitespace between the key name and the next 'CRED' marker.
clean=$(printf '%s\n' "$out" \
  | sed $'s/\x1B\\[[0-9;]*[a-zA-Z]//g')

found=0
# Pull each CRED record, one per line, even if falcon wrapped the b64.
while IFS= read -r record; do
  # record looks like:  CRED AWS_SESSION_TOKEN <b64...>
  key=$(awk '{print $2}' <<<"$record")
  b64=$(awk '{for (i=3; i<=NF; i++) printf "%s", $i}' <<<"$record")
  [[ -z "$key" || -z "$b64" ]] && continue
  val=$(printf %s "$b64" | base64 -d 2>/dev/null) || continue
  [[ -z "$val" ]] && continue
  esc=$(printf %s "$val" | sed "s/'/'\\\\''/g")
  printf "export %s='%s'\n" "$key" "$esc"
  found=1
done < <(
  # Join wrapped lines: start a new logical record at each 'CRED '
  # marker by prepending a null delimiter, then split on it.
  awk '
    /CRED [A-Z_]+/ {
      # find first occurrence of CRED on this line and keep from there
      idx = index($0, "CRED ")
      if (idx > 0) {
        if (buf != "") print buf
        buf = substr($0, idx)
        next
      }
    }
    { buf = buf $0 }
    END { if (buf != "") print buf }
  ' <<<"$clean"
)

if [[ "$found" -eq 0 ]]; then
  log "error: no AWS credentials found in falcon output"
  exit 1
fi

if [[ "$found" -eq 0 ]]; then
  log "error: no AWS credentials found in falcon output"
  exit 1
fi
