{ pkgs, defaultPackage }:

pkgs.writeShellApplication {
  name = "nes-distributed-run";
  runtimeInputs = [ pkgs.coreutils pkgs.findutils pkgs.gnugrep pkgs.yq-go ];
  text = ''
    set -euo pipefail

    SYSTEST_BIN=""

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
        cand="$PWD/$dir/nes-systests/systest/systest"
        if [ -x "$cand" ]; then
          SYSTEST_BIN="$cand"
          return
        fi
      done

      # 2) If caller provided an out path, use it
      if [ -n "''${NES_OUT-}" ] && [ -x "$NES_OUT/bin/systest" ]; then
        SYSTEST_BIN="$NES_OUT/bin/systest"
        return
      fi

      # 3) If caller provided an explicit path, use it
      if [ -n "''${SYSTEST-}" ] && [ -x "$SYSTEST" ]; then
        SYSTEST_BIN="$SYSTEST"
        return
      fi

      # 4) Last resort: build via Nix package
      NES_OUT=$(nix build .#default --print-out-paths)
      SYSTEST_BIN="$NES_OUT/bin/systest"
    }

    usage() {
      cat >&2 <<USAGE
Usage: nes-distributed-run --workdir DIR -- [systest args]

Examples:
  nix run .#nes-distributed-run -- --workdir /tmp/nes-distributed.ABC123 -- -e large
USAGE
    }

    WORKDIR=""

    while [ "$#" -gt 0 ]; do
      case "$1" in
        --workdir)
          WORKDIR="$2"; shift 2;;
        --)
          shift; break;;
        -h|--help)
          usage; exit 0;;
        *)
          echo "Unknown option: $1" >&2; usage; exit 2;;
      esac
    done


    if [ -z "$WORKDIR" ]; then
      echo "nes-distributed-run: --workdir is required" >&2
      usage; exit 2
    fi
    if [ ! -d "$WORKDIR" ]; then
      echo "nes-distributed-run: workdir not found: $WORKDIR" >&2
      exit 2
    fi

    LOCAL_TOPOLOGY="$WORKDIR/topology.local.yaml"
    if [ ! -f "$LOCAL_TOPOLOGY" ]; then
      echo "nes-distributed-run: missing $LOCAL_TOPOLOGY" >&2
      exit 2
    fi

    resolve_bins
    echo "nes-distributed-run: running systest against $LOCAL_TOPOLOGY"
    exec "$SYSTEST_BIN" --workingDir "$WORKDIR" --clusterConfig "$LOCAL_TOPOLOGY" --remote "$@"
  '';
}
