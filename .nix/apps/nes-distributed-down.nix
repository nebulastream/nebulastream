{ pkgs, defaultPackage }:

pkgs.writeShellApplication {
  name = "nes-distributed-down";
  runtimeInputs = [
    pkgs.coreutils
    pkgs.findutils
    pkgs.gnugrep
  ];
  text = ''
    set -euo pipefail

    usage() {
      cat >&2 <<USAGE
Usage: nes-distributed-down --workdir DIR [--force-after SECONDS]

Stops workers started by nes-distributed-up and leaves logs/artifacts intact.
USAGE
    }

    WORKDIR=""
    FORCE_AFTER=1

    while [ "$#" -gt 0 ]; do
      case "$1" in
        --workdir)
          WORKDIR="$2"; shift 2;;
        --force-after)
          FORCE_AFTER="$2"; shift 2;;
        -h|--help)
          usage; exit 0;;
        *)
          echo "Unknown option: $1" >&2; usage; exit 2;;
      esac
    done

    if [ -z "$WORKDIR" ]; then
      echo "nes-distributed-down: --workdir is required" >&2
      usage; exit 2
    fi
    if [ ! -d "$WORKDIR" ]; then
      echo "nes-distributed-down: workdir not found: $WORKDIR" >&2
      exit 2
    fi

    shopt -s nullglob
    PID_FILES=("$WORKDIR"/worker-*.pid)
    if [ ''${#PID_FILES[@]} -eq 0 ]; then
      echo "nes-distributed-down: no pid files in $WORKDIR; nothing to do"
      exit 0
    fi

    echo "nes-distributed-down: stopping ''${#PID_FILES[@]} workers"
    for f in "''${PID_FILES[@]}"; do
      pid=$(cat "$f" 2>/dev/null || true)
      if [ -z "''${pid:-}" ]; then
        echo "warning: empty pid in $f" >&2; continue
      fi
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done

    sleep "$FORCE_AFTER"

    for f in "''${PID_FILES[@]}"; do
      pid=$(cat "$f" 2>/dev/null || true)
      if [ -n "''${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    done

    echo "nes-distributed-down: done"
  '';
}
