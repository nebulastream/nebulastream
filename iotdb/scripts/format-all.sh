#!/bin/bash

CWDIR="$(pwd)"
BASEDIR="$(dirname "$0")"

cd "${BASEDIR}" || exit

echo "Start auto-format all header and source files tracked by git."
git ls-tree -r HEAD --name-only --full-tree | grep -e '\.hpp' -e '\.cpp' | xargs -I{} /bin/bash -c 'clang-format -i ../../{}'

echo "Auto-format is done!"
cd "${CWDIR}" || exit