#!/bin/bash

CWDIR="$(pwd)"
BASEDIR="$(dirname "$0")"

cd "${BASEDIR}" || exit

echo "Start auto-format all modified header files in staging-area."
git diff --name-only --cached "../*.hpp" | xargs -I{} /bin/bash -c 'clang-format -i ../../{}'
git diff --name-only --cached "../*.hpp" | xargs -I{} /bin/bash -c 'git add ../../{}'

echo "Start auto-format all modified source files in staging-area."
git diff --name-only --cached "../*.cpp" | xargs -I{} /bin/bash -c 'clang-format -i ../../{}'
git diff --name-only --cached "../*.cpp" | xargs -I{} /bin/bash -c 'git add ../../{}'

echo "Auto-format is done!"
cd "${CWDIR}" || exit