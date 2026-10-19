#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
UNIT_FILE="$UNIT_DIR/claude-discord-bridge.service"

if [[ ! -f "$ROOT/.env" ]]; then
  echo "Missing $ROOT/.env. Copy .env.example there and configure it first." >&2
  exit 1
fi

cargo build --release --manifest-path "$ROOT/Cargo.toml"
mkdir -p "$UNIT_DIR"
cat > "$UNIT_FILE" <<EOF
[Unit]
Description=Claude Code Discord bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT
EnvironmentFile=$ROOT/.env
Environment=HOME=$HOME
Environment=PATH=$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=$ROOT/target/release/claude-discord-bridge
Restart=on-failure
RestartSec=5
# tmux owns the detached Claude sessions; stopping the bridge must not kill them.
KillMode=process

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now claude-discord-bridge.service
echo "Installed and started claude-discord-bridge.service"
echo "Logs: journalctl --user -u claude-discord-bridge.service -f"
