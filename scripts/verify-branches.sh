#!/usr/bin/env bash
set -uo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
DOCKER_IMAGE="${DOCKER_IMAGE:-nebulastream/nes-development:local}"
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || git rev-parse HEAD)

docker_run() {
    docker run --rm -v "$(pwd):$(pwd)" -v ccache:/ccache -e CCACHE_DIR=/ccache --workdir "$(pwd)" "$DOCKER_IMAGE" "$@"
}

## Format: branch | build_target | type | runner | expected_pass | expected_total
## type=gtest: runner is the binary path relative to BUILD_DIR
## type=systest: runner is the .test file path relative to repo root
CHECKS=(
    # ── casewhen-0: logical stubs, physical still stub ──
    "tutorial-casewhen-0           | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 13 | 16"
    "tutorial-casewhen-0           | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test |  6 | 18"

    # ── casewhen-0-reference: logical validation filled in, physical still stub ──
    "tutorial-casewhen-0-reference | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-casewhen-0-reference | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test |  6 | 18"

    # ── casewhen-1: physical execution is the student task ──
    "tutorial-casewhen-1           | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-casewhen-1           | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test |  6 | 18"

    # ── casewhen-1-reference: physical execution filled in ──
    "tutorial-casewhen-1-reference | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-casewhen-1-reference | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-casewhen-1-reference | systest                            | systest | nes-systests/function/CaseWhen.test                                                |  2 |  2"

    # ── delta-0: all delta stubs ──
    "tutorial-delta-0              | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-delta-0              | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-delta-0              | delta-logical-operator-test        | gtest   | nes-logical-operators/tests/delta-logical-operator-test                            |  3 | 12"
    "tutorial-delta-0              | DeltaPhysicalOperatorTest         | gtest   | nes-physical-operators/tests/DeltaPhysicalOperatorTest        |  0 | 12"
    "tutorial-delta-0              | delta-parser-test                 | gtest   | nes-sql-parser/tests/delta-parser-test                        | 10 | 10"
    "tutorial-delta-0              | systest                           | systest | nes-systests/function/CaseWhen.test                           |  2 |  2"
    "tutorial-delta-0              | systest                           | systest | nes-systests/operator/delta/Delta.test                        |  0 |  5"

    # ── delta-0-reference: schema inference filled in ──
    "tutorial-delta-0-reference    | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-delta-0-reference    | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-delta-0-reference    | delta-logical-operator-test        | gtest   | nes-logical-operators/tests/delta-logical-operator-test                            | 12 | 12"
    "tutorial-delta-0-reference    | DeltaPhysicalOperatorTest         | gtest   | nes-physical-operators/tests/DeltaPhysicalOperatorTest        |  0 | 12"
    "tutorial-delta-0-reference    | delta-parser-test                 | gtest   | nes-sql-parser/tests/delta-parser-test                        | 10 | 10"
    "tutorial-delta-0-reference    | systest                           | systest | nes-systests/operator/delta/Delta.test                        |  0 |  5"

    # ── delta-1: same as delta-0-reference ──
    "tutorial-delta-1              | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-delta-1              | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-delta-1              | delta-logical-operator-test        | gtest   | nes-logical-operators/tests/delta-logical-operator-test                            | 12 | 12"
    "tutorial-delta-1              | DeltaPhysicalOperatorTest         | gtest   | nes-physical-operators/tests/DeltaPhysicalOperatorTest        |  0 | 12"
    "tutorial-delta-1              | delta-parser-test                 | gtest   | nes-sql-parser/tests/delta-parser-test                        | 10 | 10"
    "tutorial-delta-1              | systest                           | systest | nes-systests/operator/delta/Delta.test                        |  0 |  5"

    # ── delta-1-reference: naive per-buffer physical operator filled in ──
    "tutorial-delta-1-reference    | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-delta-1-reference    | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-delta-1-reference    | delta-logical-operator-test        | gtest   | nes-logical-operators/tests/delta-logical-operator-test                            | 12 | 12"
    "tutorial-delta-1-reference    | DeltaPhysicalOperatorTest         | gtest   | nes-physical-operators/tests/DeltaPhysicalOperatorTest        | 12 | 12"
    "tutorial-delta-1-reference    | delta-parser-test                 | gtest   | nes-sql-parser/tests/delta-parser-test                        | 10 | 10"
    "tutorial-delta-1-reference    | systest                           | systest | nes-systests/operator/delta/Delta.test                        |  5 |  5"

    # ── delta-2: handler stubs ──
    "tutorial-delta-2              | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-delta-2              | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-delta-2              | delta-logical-operator-test        | gtest   | nes-logical-operators/tests/delta-logical-operator-test                            | 12 | 12"
    "tutorial-delta-2              | DeltaPhysicalOperatorTest         | gtest   | nes-physical-operators/tests/DeltaPhysicalOperatorTest        | 12 | 16"
    "tutorial-delta-2              | delta-parser-test                 | gtest   | nes-sql-parser/tests/delta-parser-test                        | 10 | 10"
    "tutorial-delta-2              | systest                           | systest | nes-systests/operator/delta/Delta.test                        |  5 |  5"

    # ── delta-2-reference: cross-buffer handshake filled in ──
    "tutorial-delta-2-reference    | conditional-logical-function-test  | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-logical-function-test  | 16 | 16"
    "tutorial-delta-2-reference    | conditional-physical-function-test | gtest   | nes-plugins/Functions/ConditionalFunction/tests/conditional-physical-function-test | 18 | 18"
    "tutorial-delta-2-reference    | delta-logical-operator-test        | gtest   | nes-logical-operators/tests/delta-logical-operator-test                            | 12 | 12"
    "tutorial-delta-2-reference    | DeltaPhysicalOperatorTest         | gtest   | nes-physical-operators/tests/DeltaPhysicalOperatorTest        | 16 | 16"
    "tutorial-delta-2-reference    | delta-parser-test                 | gtest   | nes-sql-parser/tests/delta-parser-test                        | 10 | 10"
    "tutorial-delta-2-reference    | systest                           | systest | nes-systests/function/CaseWhen.test                           |  2 |  2"
    "tutorial-delta-2-reference    | systest                           | systest | nes-systests/operator/delta/Delta.test                        |  5 |  5"
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

ok=0
bad=0
declare -a summary_lines

cleanup() {
    echo ""
    echo -e "${BOLD}Restoring ${ORIGINAL_BRANCH}${RESET}"
    git checkout "$ORIGINAL_BRANCH" --quiet 2>/dev/null || true
}
trap cleanup EXIT

current_branch=""
needs_systest_build=""

for entry in "${CHECKS[@]}"; do
    IFS='|' read -r branch build_target type runner expect_pass expect_total <<< "$entry"
    ## trim whitespace
    branch=$(echo "$branch" | xargs)
    build_target=$(echo "$build_target" | xargs)
    type=$(echo "$type" | xargs)
    runner=$(echo "$runner" | xargs)
    expect_pass=$(echo "$expect_pass" | xargs)
    expect_total=$(echo "$expect_total" | xargs)

    if [ "$branch" != "$current_branch" ]; then
        current_branch="$branch"
        needs_systest_build="yes"
        echo ""
        echo -e "${BOLD}── ${branch} ──${RESET}"
        if ! git checkout "$branch" --quiet 2>/dev/null; then
            echo -e "  ${RED}could not checkout${RESET}"
            summary_lines+=("$(printf "%-36s %-50s ${RED}CHECKOUT FAILED${RESET}" "$branch" "")")
            ((bad++)) || true
            current_branch=""
            continue
        fi
    fi

    if [ -z "$current_branch" ]; then
        continue
    fi

    ## Derive display name from runner
    display_name="$runner"
    if [ "$type" = "systest" ]; then
        display_name="systest:$(basename "$runner" .test)"
    fi

    echo -ne "  ${display_name}  "

    ## Build
    if [ "$type" = "systest" ] && [ "$needs_systest_build" = "yes" ]; then
        build_output=$(docker_run cmake --build "$BUILD_DIR" --target systest -j 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${RED}BUILD FAILED${RESET}"
            summary_lines+=("$(printf "%-36s %-50s ${RED}BUILD FAILED${RESET}" "$branch" "$display_name")")
            ((bad++)) || true
            continue
        fi
        needs_systest_build="no"
    elif [ "$type" = "gtest" ]; then
        build_output=$(docker_run cmake --build "$BUILD_DIR" --target "$build_target" -j 2>&1)
        if [ $? -ne 0 ]; then
            echo -e "${RED}BUILD FAILED${RESET}"
            summary_lines+=("$(printf "%-36s %-50s ${RED}BUILD FAILED${RESET}" "$branch" "$display_name")")
            ((bad++)) || true
            continue
        fi
    fi

    ## Run
    if [ "$type" = "gtest" ]; then
        test_output=$(docker_run "${BUILD_DIR}/${runner}" 2>&1) || true
        passed=$(echo "$test_output" | sed -n 's/.*\[  PASSED  \] \([0-9][0-9]*\) test.*/\1/p') || passed=0
        failed=$(echo "$test_output" | sed -n 's/.*\[  FAILED  \] \([0-9][0-9]*\) test.*/\1/p') || failed=0
        [ -z "$passed" ] && passed=0
        [ -z "$failed" ] && failed=0
    elif [ "$type" = "systest" ]; then
        test_output=$(docker_run "${BUILD_DIR}/nes-systests/systest/systest" -t "$(pwd)/${runner}" --sequential 2>&1) || true
        passed=$(echo "$test_output" | grep -c "PASSED") || passed=0
        failed=$(echo "$test_output" | grep -c "FAILED") || failed=0
    fi
    total=$((passed + failed))

    if [ "$passed" -eq "$expect_pass" ] && [ "$total" -eq "$expect_total" ]; then
        if [ "$failed" -eq 0 ]; then
            echo -e "${GREEN}${passed}/${total} passed${RESET}"
        else
            echo -e "${GREEN}${passed}/${total} passed (${failed} expected stub failures)${RESET}"
        fi
        summary_lines+=("$(printf "%-36s %-50s ${GREEN}OK  %d/%d${RESET}" "$branch" "$display_name" "$passed" "$total")")
        ((ok++)) || true
    else
        echo -e "${RED}UNEXPECTED: ${passed}/${total} (expected ${expect_pass}/${expect_total})${RESET}"
        summary_lines+=("$(printf "%-36s %-50s ${RED}BAD %d/%d (want %d/%d)${RESET}" "$branch" "$display_name" "$passed" "$total" "$expect_pass" "$expect_total")")
        ((bad++)) || true
    fi
done

echo ""
echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  SUMMARY${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════════════════════════════════════════${RESET}"

for line in "${summary_lines[@]}"; do
    echo -e "$line"
done

echo -e "${BOLD}────────────────────────────────────────────────────────────────────────────────────────────${RESET}"
echo -e "  ${GREEN}OK: ${ok}${RESET}    ${RED}BAD: ${bad}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════════════════════════════════════════${RESET}"

if [ "$bad" -gt 0 ]; then
    exit 1
fi
