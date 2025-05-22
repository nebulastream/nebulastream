# exit if any command (even inside a pipe) fails or an undefined variable is used.
set -euo pipefail

# cd to root of git repo
cd "$(git rev-parse --show-toplevel)"

#cmake --build $1 --target nes-single-node-worker systest -j

mydir=$(mktemp -d "${TMPDIR:-/tmp/}$(basename $0).XXXXXXXXXXXX")
cat > ${mydir}/.env <<EOF
  SINGLE_NODE_BUILD_DIR=$(pwd)/$1/nes-single-node-worker
  SYSTEST_BUILD_DIR=$(pwd)/$1/nes-systests/systest
EOF

docker compose --project-directory . --env-file ${mydir}/.env -f nes-systests/remote/docker-compose.yaml up --build --abort-on-container-exit --exit-code-from systest