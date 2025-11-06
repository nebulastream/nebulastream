{ pkgs, defaultPackage }:

pkgs.writeShellApplication {
  name = "nes-distributed-up";
  runtimeInputs = [
    pkgs.coreutils
    pkgs.yq-go
  ];
  text = ''
    set -euo pipefail

    NES_WORKER_BIN=""

    resolve_bins() {
      # 1) Prefer locally built binaries in common CMake build dirs
      for dir in \
        cmake-build-debug \
        cmake-build-nix \
        cmake-build-release \
        cmake-build-relwithdebinfo \
        build \
        build-nix
      do
        cand="$PWD/$dir/nes-single-node-worker/nes-single-node-worker"
        if [ -x "$cand" ]; then
          NES_WORKER_BIN="$cand"
          return
        fi
      done

      # 2) If caller provided an out path, use it
      if [ -n "''${NES_OUT-}" ] && [ -x "$NES_OUT/bin/nes-single-node-worker" ]; then
        NES_WORKER_BIN="$NES_OUT/bin/nes-single-node-worker"
        return
      fi

      # 3) If caller provided an explicit path, use it
      if [ -n "''${NEBULASTREAM-}" ] && [ -x "$NEBULASTREAM" ]; then
        NES_WORKER_BIN="$NEBULASTREAM"
        return
      fi

      # 4) Last resort: build via Nix package
      NES_OUT=$(nix build .#default --print-out-paths)
      NES_WORKER_BIN="$NES_OUT/bin/nes-single-node-worker"
    }

    usage() {
      cat >&2 <<USAGE
Usage: nes-distributed-up --topology <file> [--workdir DIR] [--bind-addr ADDR] \
       [--no-rewrite] \
       [--startup-timeout SECONDS] [--port-offset N]

Starts workers in the background and waits for health, then prints WORKDIR=... on stdout.
USAGE
    }

    TOPOLOGY=""
    WORKDIR=""
    BIND_ADDR="127.0.0.1"
    REWRITE=1
    STARTUP_TIMEOUT=30
    PORT_OFFSET=0

    while [ "$#" -gt 0 ]; do
      case "$1" in
        --topology|-c)
          TOPOLOGY="$2"; shift 2;;
        --workdir)
          WORKDIR="$2"; shift 2;;
        --bind-addr)
          BIND_ADDR="$2"; shift 2;;
        --no-rewrite)
          REWRITE=0; shift;;
        --startup-timeout)
          STARTUP_TIMEOUT="$2"; shift 2;;
        --port-offset)
          PORT_OFFSET="$2"; shift 2;;
        -h|--help)
          usage; exit 0;;
        *)
          echo "Unknown option: $1" >&2; usage; exit 2;;
      esac
    done

    # Validate numeric port offset
    if ! [[ "$PORT_OFFSET" =~ ^[0-9]+$ ]]; then
      echo "nes-distributed-up: --port-offset must be a non-negative integer" >&2
      exit 2
    fi

    if [ -z "$TOPOLOGY" ]; then
      echo "nes-distributed-up: --topology is required" >&2
      usage; exit 2
    fi
    if [ ! -f "$TOPOLOGY" ]; then
      echo "nes-distributed-up: topology file not found: $TOPOLOGY" >&2
      exit 2
    fi

    if [ -z "$WORKDIR" ]; then
      choose_base() {
        if [ -n "''${NES_DISTRIBUTED_WORKDIR_BASE-}" ]; then
          printf '%s\n' "$NES_DISTRIBUTED_WORKDIR_BASE"; return
        fi
        # Use the systest default working_dir under a local CMake build directory
        for dir in \
          cmake-build-debug \
          cmake-build-nix \
          cmake-build-release \
          cmake-build-relwithdebinfo
        do
          if [ -d "$PWD/$dir/nes-systests" ]; then
            printf '%s\n' "$PWD/$dir/nes-systests/working-dir"; return
          fi
        done
        printf '%s\n' "$PWD/cmake-build-debug/nes-systests/working-dir"
      }
      BASE_DIR=$(choose_base)
      mkdir -p "$BASE_DIR"
      WORKDIR=$(mktemp -d -p "$BASE_DIR" nes.XXXXXX)
    else
      mkdir -p "$WORKDIR"
    fi
    mkdir -p "$WORKDIR/logs"
    echo "nes-distributed-up: working dir: $WORKDIR"

    LOCAL_TOPOLOGY="$WORKDIR/topology.local.yaml"

    if [ "$REWRITE" -eq 1 ]; then
      echo "nes-distributed-up: rewriting topology hosts to $BIND_ADDR"
      BIND_ADDR="$BIND_ADDR" yq e '
        (.workers[].host) |= sub("^[^:]+", strenv(BIND_ADDR)) |
        (.workers[].grpc) |= sub("^[^:]+", strenv(BIND_ADDR)) |
        (.workers[].downstream[]?) |= sub("^[^:]+", strenv(BIND_ADDR)) |
        (.allow_source_placement[]?) |= sub("^[^:]+", strenv(BIND_ADDR)) |
        (.allow_sink_placement[]?) |= sub("^[^:]+", strenv(BIND_ADDR))
      ' "$TOPOLOGY" >"$LOCAL_TOPOLOGY"
    else
      cp "$TOPOLOGY" "$LOCAL_TOPOLOGY"
    fi

    # Extract worker endpoints
    mapfile -t HOSTS < <(yq e -r '.workers[].host' "$LOCAL_TOPOLOGY")
    mapfile -t GRPCS < <(yq e -r '.workers[].grpc' "$LOCAL_TOPOLOGY")
    if [ "''${#HOSTS[@]}" -eq 0 ]; then
      echo "nes-distributed-up: no workers defined in topology" >&2
      exit 2
    fi

    # Deconflict ports on host and apply base port offset
    need_deconflict=0
    declare -A _seen_host
    for h in "''${HOSTS[@]}"; do
      p="''${h##*:}"
      if [ -n "''${_seen_host[$p]:-}" ]; then need_deconflict=1; break; fi
      _seen_host[$p]=1
    done
    if [ "$need_deconflict" -eq 0 ]; then
      declare -A _seen_grpc
      for g in "''${GRPCS[@]}"; do
        p="''${g##*:}"
        if [ -n "''${_seen_grpc[$p]:-}" ]; then need_deconflict=1; break; fi
        _seen_grpc[$p]=1
      done
    fi

    if [ "$need_deconflict" -eq 1 ] || [ "$PORT_OFFSET" != "0" ]; then
      echo "nes-distributed-up: applying base offset $PORT_OFFSET and per-index increments to avoid port clashes"
      COUNT=''${#HOSTS[@]}
      for IDX in $(seq 0 $((COUNT - 1))); do
        OLD_HOST=$(yq e -r ".workers[$IDX].host" "$LOCAL_TOPOLOGY")
        OLD_GRPC=$(yq e -r ".workers[$IDX].grpc" "$LOCAL_TOPOLOGY")
        HOST_PORT="''${OLD_HOST##*:}"
        GRPC_PORT="''${OLD_GRPC##*:}"
        DELTA=$((PORT_OFFSET + IDX))
        NEW_HOST_PORT=$((HOST_PORT + DELTA))
        NEW_GRPC_PORT=$((GRPC_PORT + DELTA))
        NEW_HOST="$BIND_ADDR:$NEW_HOST_PORT"
        NEW_GRPC="$BIND_ADDR:$NEW_GRPC_PORT"

        yq e -i "
          .workers[''${IDX}].host = \"''${NEW_HOST}\" |
          .workers[''${IDX}].grpc = \"''${NEW_GRPC}\" |
          (.workers[] | select(has(\"downstream\")) | .downstream |= map(if . == \"''${OLD_HOST}\" then \"''${NEW_HOST}\" else . end)) |
          (.allow_source_placement |= ((. // []) | map(if . == \"''${OLD_HOST}\" then \"''${NEW_HOST}\" else . end))) |
          (.allow_sink_placement   |= ((. // []) | map(if . == \"''${OLD_HOST}\" then \"''${NEW_HOST}\" else . end)))
        " "$LOCAL_TOPOLOGY"
      done
      mapfile -t HOSTS < <(yq e -r '.workers[].host' "$LOCAL_TOPOLOGY")
      mapfile -t GRPCS < <(yq e -r '.workers[].grpc' "$LOCAL_TOPOLOGY")
    fi

    # Optional extra worker flags via env (space-separated)
    FLAGS_SRC="''${NES_DISTRIBUTED_WORKER_FLAGS-}"
    if [ -z "$FLAGS_SRC" ] && [ "''${NES_REMOTE_WORKER_FLAGS-}" != "" ]; then
      FLAGS_SRC="$NES_REMOTE_WORKER_FLAGS"
    fi
    NES_DISTRIBUTED_WORKER_FLAGS_ARRAY=()
    if [ -n "$FLAGS_SRC" ]; then
      # shellcheck disable=SC2206
      NES_DISTRIBUTED_WORKER_FLAGS_ARRAY=($FLAGS_SRC)
    fi

    # Resolve binaries and start workers
    resolve_bins
    pids=()
    names=()
    for IDX in $(seq 0 $(( ''${#HOSTS[@]} - 1 ))); do
      host="''${HOSTS[$IDX]}"
      grpc="''${GRPCS[$IDX]}"

      host_port="''${host#*:}"
      name="worker-$host_port"
      log="$WORKDIR/logs/$name.log"

      cmd=("$NES_WORKER_BIN" "--grpc=$grpc" "--connection=$host" "''${NES_DISTRIBUTED_WORKER_FLAGS_ARRAY[@]}")

      echo "nes-distributed-up: starting $name (grpc=$grpc, conn=$host)"
      ("''${cmd[@]}" >"$log" 2>&1 & echo $! >"$WORKDIR/$name.pid")
      pid=$(cat "$WORKDIR/$name.pid")
      pids+=("$pid")
      names+=("$name")
    done

    # Wait for grpc health
    echo "nes-distributed-up: waiting for workers to become healthy (timeout=''${STARTUP_TIMEOUT}s)"
    for grpc in "''${GRPCS[@]}"; do
      ok=0
      for _ in $(seq 1 "$STARTUP_TIMEOUT"); do
        host="''${grpc%%:*}"; port="''${grpc##*:}"
        if timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
          ok=1; break
        fi
        sleep 1
      done
      if [ "$ok" -ne 1 ]; then
        echo "nes-distributed-up: grpc service not healthy at $grpc; showing logs" >&2
        for n in "''${names[@]}"; do
          echo "--- $n ---" >&2
          tail -n 200 "$WORKDIR/logs/$n.log" >&2 || true
        done
        exit 1
      fi
    done

    # Persist metadata
    cat >"$WORKDIR/cluster.env" <<META
WORKDIR="$WORKDIR"
TOPOLOGY_ORIG="$TOPOLOGY"
LOCAL_TOPOLOGY="$LOCAL_TOPOLOGY"
BIND_ADDR="$BIND_ADDR"
PORT_OFFSET="$PORT_OFFSET"
STARTED_AT="$(date -u +%FT%TZ)"
META

    echo "nes-distributed-up: cluster ready"
    echo "WORKDIR=$WORKDIR"
  '';
}
