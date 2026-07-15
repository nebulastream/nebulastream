# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Registers the `tidy-diff` and `tidy-diff-fix` targets, mirroring the
# `format` / `check-format` targets (see cmake/macros.cmake). They run
# clang-tidy over the local git diff via clang-tidy-diff(.py):
#   tidy-diff     -- check mode
#   tidy-diff-fix -- check mode + `-fix` (applies suggested edits)
#
# The actual logic lives in scripts/tidy-diff.sh so the local developer command
# and CI exercise the same code path. This module only discovers the tools and
# wires up the targets.
function(project_enable_tidy_diff)
    # Gating: clang-tidy-diff needs a compile_commands.json. If compile command
    # export is off the targets cannot work, so do not register them. This is a
    # silent no-op (the option is ON by default; an explicit OFF is intentional).
    if (NOT CMAKE_EXPORT_COMPILE_COMMANDS)
        message(STATUS "CMAKE_EXPORT_COMPILE_COMMANDS is OFF. Disabling tidy-diff targets.")
        return()
    endif ()

    # Locate the clang-tidy binary (versioned name first, matching the format target).
    find_program(CLANG_TIDY_EXECUTABLE NAMES clang-tidy-${LLVM_TOOLCHAIN_VERSION} clang-tidy)
    if (NOT CLANG_TIDY_EXECUTABLE)
        message(WARNING "clang-tidy not found, but can be installed with 'sudo apt install clang-tidy-${LLVM_TOOLCHAIN_VERSION}'. Disabling tidy-diff targets.")
        return()
    endif ()

    # Verify a compatible clang-tidy major version (must match the toolchain).
    execute_process(
            COMMAND ${CLANG_TIDY_EXECUTABLE} --version
            OUTPUT_VARIABLE CLANG_TIDY_VERSION
    )
    string(REGEX MATCH "version ([0-9]+)\\.([0-9]+)\\.([0-9]+)" CLANG_TIDY_MAJOR_MINOR_PATCH "${CLANG_TIDY_VERSION}")
    if (NOT CMAKE_MATCH_1 STREQUAL ${LLVM_TOOLCHAIN_VERSION})
        message(WARNING "Incompatible clang-tidy version requires ${LLVM_TOOLCHAIN_VERSION}, got \"${CMAKE_MATCH_1}\". Disabling tidy-diff targets.")
        return()
    endif ()

    # Locate clang-tidy-diff.py. It ships with LLVM; the file name carries the
    # version (e.g. clang-tidy-diff-19.py) and it is usually only on PATH, not in
    # a predictable prefix, so we probe the versioned and unversioned names the
    # same way the format target probes clang-format.
    find_program(CLANG_TIDY_DIFF_EXECUTABLE
            NAMES clang-tidy-diff-${LLVM_TOOLCHAIN_VERSION}.py clang-tidy-diff.py)
    if (NOT CLANG_TIDY_DIFF_EXECUTABLE)
        message(WARNING "clang-tidy-diff(.py) not found (ships with LLVM ${LLVM_TOOLCHAIN_VERSION}). Disabling tidy-diff targets.")
        return()
    endif ()

    set(TIDY_DIFF_DRIVER ${CMAKE_SOURCE_DIR}/scripts/tidy-diff.sh)
    set(TIDY_DIFF_REPORT ${CMAKE_BINARY_DIR}/clang-tidy-diff-report.txt)

    message(STATUS "Enabling tidy-diff targets using ${CLANG_TIDY_DIFF_EXECUTABLE} (binary: ${CLANG_TIDY_EXECUTABLE})")

    # tidy-diff: check mode. The driver discovers the diff base from the
    # NES_TIDY_DIFF_BASE env var (default 'git diff HEAD'), prints a summary,
    # then runs clang-tidy-diff, teeing colored output to stdout and a de-colored
    # copy to the report file. USES_TERMINAL preserves the colored stdout.
    add_custom_target(tidy-diff
            COMMAND ${TIDY_DIFF_DRIVER}
                ${CLANG_TIDY_DIFF_EXECUTABLE}
                ${CLANG_TIDY_EXECUTABLE}
                ${CMAKE_BINARY_DIR}
                ${TIDY_DIFF_REPORT}
                check
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            USES_TERMINAL
            COMMENT "Running clang-tidy on the local diff (check)")

    # tidy-diff-fix: same, but applies `-fix`.
    add_custom_target(tidy-diff-fix
            COMMAND ${TIDY_DIFF_DRIVER}
                ${CLANG_TIDY_DIFF_EXECUTABLE}
                ${CLANG_TIDY_EXECUTABLE}
                ${CMAKE_BINARY_DIR}
                ${TIDY_DIFF_REPORT}
                fix
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            USES_TERMINAL
            COMMENT "Running clang-tidy on the local diff (fix)")

    # tidy-diff-to-main / -fix: same targets, but pin the base to origin/main so
    # the check covers the whole branch instead of just uncommitted changes.
    # We pin via `cmake -E env`, which sets NES_TIDY_DIFF_BASE in the child
    # process regardless of whether the outer (Docker) toolchain forwards the
    # variable -- so these work as one-click targets from CLion with no env setup.
    add_custom_target(tidy-diff-to-main
            COMMAND ${CMAKE_COMMAND} -E env NES_TIDY_DIFF_BASE=origin/main
                ${TIDY_DIFF_DRIVER}
                ${CLANG_TIDY_DIFF_EXECUTABLE}
                ${CLANG_TIDY_EXECUTABLE}
                ${CMAKE_BINARY_DIR}
                ${TIDY_DIFF_REPORT}
                check
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            USES_TERMINAL
            COMMENT "Running clang-tidy on the diff vs origin/main (check)")

    add_custom_target(tidy-diff-to-main-fix
            COMMAND ${CMAKE_COMMAND} -E env NES_TIDY_DIFF_BASE=origin/main
                ${TIDY_DIFF_DRIVER}
                ${CLANG_TIDY_DIFF_EXECUTABLE}
                ${CLANG_TIDY_EXECUTABLE}
                ${CMAKE_BINARY_DIR}
                ${TIDY_DIFF_REPORT}
                fix
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            USES_TERMINAL
            COMMENT "Running clang-tidy on the diff vs origin/main (fix)")

    # TODO #1609: migrate the handwritten bash in
    # .github/workflows/clang_tidy_diff.yml to invoke `cmake --build --target
    # tidy-diff` so the CI check and the local command stay in sync. Deferred to
    # keep this PR focused and avoid risking the CI pipeline; the CI workflow also
    # relies on -export-fixes for PR annotations, which needs separate wiring.
endfunction(project_enable_tidy_diff)
