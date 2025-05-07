#!/usr/bin/env bash
# prepare.sh — discover host settings and write .env

cat > .env <<EOF
# host user/group mapped into the container:
HOST_UID=$(id -u)
HOST_GID=$(id -g)

# where Pulse stores its socket:
XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}

# pick the first “input” source (your mic) by default:
PULSE_SOURCE=$(pactl info \
  | awk -F": " '/Default Source/ { print $2 }')
EOF

echo ".env written:"
cat .env
