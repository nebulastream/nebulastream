{ pkgs, defaultPackage, upRunner, runRunner, downRunner }:

pkgs.writeShellApplication {
  name = "nes-distributed";
  runtimeInputs = [ pkgs.coreutils pkgs.gawk pkgs.gnugrep ];
  text = ''
    set -euo pipefail

    UP_BIN="${upRunner}/bin/nes-distributed-up"
    RUN_BIN="${runRunner}/bin/nes-distributed-run"
    DOWN_BIN="${downRunner}/bin/nes-distributed-down"

    usage() {
      cat >&2 <<USAGE
Usage: nes-distributed --topology <file> [--workdir DIR] [--bind-addr ADDR] \
               [--no-rewrite] [--startup-timeout SECONDS] [--port-offset N] -- [systest args]
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
        --)
          shift; break;;
        -h|--help)
          usage; exit 0;;
        *)
          echo "Unknown option: $1" >&2; usage; exit 2;;
      esac
    done

    # Preserve remaining args for systest
    SYSTEST_ARGS=("$@")

    if [ -z "$TOPOLOGY" ]; then
      echo "nes-distributed: --topology is required" >&2
      usage; exit 2
    fi

    # Determine workdir here if not provided
    if [ -z "$WORKDIR" ]; then
      choose_base() {
        if [ -n "''${NES_DISTRIBUTED_WORKDIR_BASE-}" ]; then
          printf '%s\n' "$NES_DISTRIBUTED_WORKDIR_BASE"; return
        fi
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
        # Fallback: place under cmake-build-debug
        printf '%s\n' "$PWD/cmake-build-debug/nes-systests/working-dir"
      }
      BASE_DIR=$(choose_base)
      mkdir -p "$BASE_DIR"
      WORKDIR=$(mktemp -d -p "$BASE_DIR" nes.XXXXXX)
    fi

    echo "nes-distributed: starting cluster (up)"
    echo "nes-distributed: cluster workdir: $WORKDIR"
    _UP_ARGS=(
      --topology "$TOPOLOGY"
      --startup-timeout "$STARTUP_TIMEOUT"
      --port-offset "$PORT_OFFSET"
      --workdir "$WORKDIR"
    )
    if [ -n "$BIND_ADDR" ]; then _UP_ARGS+=( --bind-addr "$BIND_ADDR" ); fi
    if [ "$REWRITE" -eq 0 ]; then _UP_ARGS+=( --no-rewrite ); fi
    "$UP_BIN" "''${_UP_ARGS[@]}"

    cleanup() {
      status=$?
      echo "nes-distributed: tearing cluster down (down)"
      "$DOWN_BIN" --workdir "$WORKDIR" || true
      exit "$status"
    }
    trap cleanup INT TERM EXIT

    # Run tests
    echo "nes-distributed: running systest (run)"
    "$RUN_BIN" --workdir "$WORKDIR" -- "''${SYSTEST_ARGS[@]}"
  '';
}
