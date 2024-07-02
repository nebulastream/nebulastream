#!/usr/bin/env bash

set -eo pipefail

# cd to dir the script is located in (i.e. <nes_root>/scripts)
SOURCE="${BASH_SOURCE[0]}"
if [ -h "$SOURCE" ]
then
    # c.f. https://stackoverflow.com/a/246128/65678 for handling symlinks
    echo "this script cannot be called via a symlink (yet)"
    exit 1
fi
cd "$(dirname "$SOURCE")"

# cd to <nes_root>, so that script can be invoked from anywhere
cd ..


if ! [ -x "$(command -v clang-format-18)" ]
then
	echo could not find clang-format-18 in PATH.
	echo please install.
	exit 1
fi

if [ "$#" -gt 0 ] && [ "$1" != "-i" ]
then
    echo "Wrong args. use with no args or a single '-i'."
    exit 1
fi


if [ "${1-}" = "-i" ]
then
    ARGS=(-i)
else
    ARGS=(--dry-run -Werror)
fi


# list all cpp, hpp files known to git
#   run `nproc` clang-format processes in parallel on 10 files each
git ls-files -- '*.cpp' '*.hpp' \
  | xargs --max-args=10 --max-procs="$(nproc)" clang-format-18 "${ARGS[@]}"
