#!/usr/bin/env bash

set -eo pipefail

cd "$(git rev-parse --show-toplevel)"


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
    # clang-format
    git ls-files -- '*.cpp' '*.hpp' \
      | xargs --max-args=10 --max-procs="$(nproc)" clang-format-18 -i

    # newline at eof
    #
    # list files in repo
    #   remove filenames ending with .bin or .png
    #   if len(last char) > 0 (i.e.: not '\n'): append newline
    git ls-files \
      | grep --invert-match -e "\.bin$" -e "\.png$" \
      | xargs --max-procs="$(nproc)" -I {} sh -c '[ -n "$(tail -c 1 {})" ] && echo "" >> {}'

else
    # clang-format
    git ls-files -- '*.cpp' '*.hpp' \
      | xargs --max-args=10 --max-procs="$(nproc)" clang-format-18 --dry-run -Werror

    # newline at eof
    #
    # list files in repo
    #   remove filenames ending with .bin or .png
    #   take last char of the files, count lines and chars,
    #   fail if not equal (i.e. not every char is a newline)
    git ls-files \
      | grep --invert-match -e "\.bin$" -e "\.png$" \
      | xargs --max-args=10 --max-procs="$(nproc)" tail -qc 1  | wc -cl \
      | awk '$1 != $2 { print $2-$1, "missing newline(s) at EOF. Please run \"scripts/format.sh -i\" to fix."; exit 1 }'
fi
