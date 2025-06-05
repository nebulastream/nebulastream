#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

cd "$(git rev-parse --show-toplevel)"

# We have to set the pager to cat, because otherwise the output is paged and the script hangs for git grep commands.
# It looks like git grep pipes the output to less, which is not closed properly.
export GIT_PAGER=cat

COLOR_YELLOW_BOLD="\e[1;33m"
COLOR_RED_BOLD="\e[1;31m"
COLOR_BG_RED_FONT_WHITE="\e[41m\e[1;37m"
COLOR_RESET="\e[0m"

log_warn() {
    echo -e "${COLOR_YELLOW_BOLD}Warning${COLOR_RESET}: $*"
}
log_error() {
    FAIL=1
    echo -e "${COLOR_RED_BOLD}Error${COLOR_RESET}: $*"
}
log_fatal() {
    echo -e "${COLOR_BG_RED_FONT_WHITE}Fatal${COLOR_RESET}: $*"
    exit 1
}


if [ -x "$(command -v clang-format-19)" ]
then
    CLANG_FORMAT="clang-format-19"
elif [ -x "$(command -v clang-format)" ] && clang-format --version | grep "version 19" > /dev/null
then
    CLANG_FORMAT="clang-format"
else
    log_fatal could not find clang-format 19 in PATH, please install.
fi

# calculate distance to base branch
#
# Ideally, we would want to use e.g. git diff --merge-base origin/main,
# but when running in CI, this can lead to 'fatal: no merge base found'
#
# So instead, we obstain the distance to the base branch in CI from github
# and locally we calculate it.
#
# c.f. bc88d4e05760f60f12524eff1a205cf5c85d14d2
# c.f. https://github.com/nebulastream/nebulastream/pull/677
if [ -v CI ]
then
    [ -v DISTANCE_MERGE_BASE ] || log_fatal Running in CI but DISTANCE_MERGE_BASE not set
else
    DISTANCE_MERGE_BASE=$(git rev-list --count origin/main..HEAD)
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
    #   remove filenames indicating non-text content
    #   last char as decimal ascii is 10 (i.e. is newline) OR append newline
    git ls-files \
      | grep --invert-match -e "\.png$" -e "\.zip$" \
      | xargs --max-procs="$(nproc)" -I {} sh -c '[ "$(tail -c 1 {} | od -A n -t d1)" = "   10" ] || echo "" >> {}'

else
    # clang-format
    git ls-files -- '*.cpp' '*.hpp' \
      | xargs --max-args=10 --max-procs="$(nproc)" "$CLANG_FORMAT" --dry-run -Werror \
      || FAIL=1

    # newline at eof
    #
    # list files in repo
    #   remove filenames indicating non-text content
    #   take last char of the files, count lines and chars,
    #   fail if not equal (i.e. not every char is a newline)
    git ls-files \
      | grep --invert-match -e "\.png$" -e "\.zip$" \
      | xargs --max-args=10 --max-procs="$(nproc)" tail -qc 1  | wc -cl \
      | awk '$1 != $2 { exit 1 }' \
      || log_error 'There are missing newline(s) at EOF. Please run "scripts/format.sh -i" to fix'
fi

# comment style
#
# Only /// allowed, as voted in https://github.com/nebulastream/nebulastream-public/discussions/18
# The regex matches an even number of slashes (i.e. //, ////, ...)
# The regex does not match "://" (for e.g. https://foo)
for file in $(git diff --name-only "HEAD~$DISTANCE_MERGE_BASE" -- "*.hpp" "*.cpp")
do
    if git grep -n -E -e "([^/:]|^)(//)+[^/]" -- "$file" > /dev/null
    then
        log_error "Illegal comment in $(git grep -n -E -e "([^/:]|^)(//)+[^/]" -- "$file")"
    fi
done

# first include in .cpp file is the corresponding .hpp file
#
for file in $(git diff --name-only "HEAD~$DISTANCE_MERGE_BASE" -- "*.cpp")
do
    # remove path and .cpp suffix, i.e. /foo/bar.cpp -> bar
    basename=$(basename "$file" .cpp)
    # check if corresponding header file exists
    if ! git ls-files | grep "$basename.hpp" > /dev/null
    then
        log_warn "file has no corresponding header file: $file"
        continue
    fi
    # error if the first include does not contain the basename
    if ! grep "#include" < "$file" | head -n 1 | grep "$basename.hpp" > /dev/null
    then
        # line 15 shall be the first include after license preamble and one blank line
        log_error "First include is not the corresponding .hpp file in $file:15"
    fi
done

# warning: no includes with double quotes
#
# CLion uses double quotes when adding includes (automatically).
# This check warns the author of a PR about includes with double quotes to avoid burdening the reviewers
for file in $(git diff --name-only "HEAD~$DISTANCE_MERGE_BASE" -- "*.hpp" "*.cpp")
do
    # if an added line contains contains a quote include
    if git diff "HEAD~$DISTANCE_MERGE_BASE" -- "$file" | grep "^+" | grep '#include ".*"' > /dev/null
    then
        log_warn "New include with double quotes in $(git grep -n '#include ".*"' -- "$file")"
    fi
done


python3 scripts/check_preamble.py || FAIL=1

DISTANCE_MERGE_BASE=$DISTANCE_MERGE_BASE python3 scripts/check_todos.py || FAIL=1

[ "$FAIL" = "0" ] && echo "format.sh: no problems found"

exit "$FAIL"
