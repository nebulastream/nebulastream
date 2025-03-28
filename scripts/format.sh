#!/usr/bin/env bash

set -eo pipefail

cd "$(git rev-parse --show-toplevel)"

# We have to set the pager to cat, because otherwise the output is paged and the script hangs for git grep commands.
# It looks like git grep pipes the output to less, which is not closed properly.
export GIT_PAGER=cat


if [ -x "$(command -v clang-format-19)" ]
then
    CLANG_FORMAT="clang-format-19"
elif [ -x "$(command -v clang-format)" ] && clang-format --version | grep "version 19" > /dev/null
then
    CLANG_FORMAT="clang-format"
else
    echo could not find clang-format 19 in PATH.
    echo please install.
    exit 1
fi

if [ "$#" -gt 0 ] && [ "$1" != "-i" ]
then
    cat << EOF
Usage:

  $0     to check formatting
  $0 -i  to fix formatting (if possible)
EOF
    exit 1
fi


FAIL=0

if [ "${1-}" = "-i" ]
then
    # clang-format
    git ls-files -- '*.cpp' '*.hpp' \
      | xargs --max-args=10 --max-procs="$(nproc)" "$CLANG_FORMAT" -i

    # newline at eof
    #
    # list files in repo
    #   remove filenames ending with .bin or .png
    #   last char as decimal ascii is 10 (i.e. is newline) OR append newline
    git ls-files \
      | grep --invert-match -e "\.bin$" -e "\.png$" -e "\.zip$" \
      | xargs --max-procs="$(nproc)" -I {} sh -c '[ "$(tail -c 1 {} | od -A n -t d1)" = "   10" ] || echo "" >> {}'

else
    # clang-format
    git ls-files -- '*.cpp' '*.hpp' \
      | xargs --max-args=10 --max-procs="$(nproc)" "$CLANG_FORMAT" --dry-run -Werror \
      || FAIL=1

    # newline at eof
    #
    # list files in repo
    #   remove filenames ending with .bin or .png
    #   take last char of the files, count lines and chars,
    #   fail if not equal (i.e. not every char is a newline)
    git ls-files \
      | grep --invert-match -e "\.bin$" -e "\.png$" -e "\.zip$" \
      | xargs --max-args=10 --max-procs="$(nproc)" tail -qc 1  | wc -cl \
      | awk '$1 != $2 { print $2-$1, "missing newline(s) at EOF. Please run \"scripts/format.sh -i\" to fix."; exit 1 }' \
      || FAIL=1
fi

# comment style
#
# Only /// allowed, as voted in https://github.com/nebulastream/nebulastream-public/discussions/18
# The regex matches an even number of slashes (i.e. //, ////, ...)
# The regex does not match "://" (for e.g. https://foo)
if git grep -n -E -e "([^/:]|^)(//)+[^/]" -- '*.cpp' '*.hpp'
then
    echo
    echo Found forbidden comments. Please use /// for doc comments, remove all else.
    FAIL=1
fi

# no comment after closing bracket for namespace
#
# No comment after closing brackets, as voted in https://github.com/nebulastream/nebulastream-public/discussions/379
# This is done to ensure that no one uses a comment after a closing bracket for a namespace.
if git grep -n -E -e "}[ \t]*//.*namespace.*" -- "nes-*"
then
    echo
    echo Found comment after closing bracket for namespace. Please remove.
    FAIL=1
fi

python3 scripts/check_preamble.py || FAIL=1

python3 scripts/check_todos.py || FAIL=1

[ "$FAIL" = "0" ] && echo "format.sh: no problems found"

exit "$FAIL"
