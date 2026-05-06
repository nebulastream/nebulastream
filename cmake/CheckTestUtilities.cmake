# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Validate docker is available when ENABLE_DOCKER_TESTS is ON
if (ENABLE_DOCKER_TESTS)
    execute_process(
        COMMAND docker version
        RESULT_VARIABLE DOCKER_CHECK_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
    )

    if (NOT DOCKER_CHECK_RESULT EQUAL 0)
        message(WARNING
            "ENABLE_DOCKER_TESTS is ON but docker is not working.\n"
            "  For dev container: Mount docker socket with -v /var/run/docker.sock:/var/run/docker.sock\n"
            "  For host system: Ensure docker is installed and running\n"
            "  Set -DENABLE_DOCKER_TESTS=OFF to suppress this warning\n"
            "Docker tests will be automatically disabled."
        )
        set(ENABLE_DOCKER_TESTS OFF PARENT_SCOPE)
    else()
        message(STATUS "Docker tests enabled: using docker")
    endif()
endif()

# Check if bats is available for shell-based e2e tests
find_program(BATS bats)
if (BATS STREQUAL "BATS-NOTFOUND")
    set(ENABLE_BATS_TESTS OFF CACHE BOOL "Runs testcases that require bats" FORCE)
    message(WARNING "Bats not found. Disabling Bats based e2e tests. You can install Bats via apt install bats")
else ()
    set(ENABLE_BATS_TESTS ON CACHE BOOL "Runs testcases that require bats" FORCE)
    message(STATUS "Bats tests enabled: using ${BATS}")
endif ()

# Check if IREE tools are available for inference-backed physical operator tests
find_program(IREE_IMPORT_ONNX iree-import-onnx)
find_program(IREE_COMPILE iree-compile)
if (IREE_IMPORT_ONNX STREQUAL "IREE_IMPORT_ONNX-NOTFOUND" OR IREE_COMPILE STREQUAL "IREE_COMPILE-NOTFOUND")
    set(ENABLE_IREE_TESTS OFF CACHE BOOL "Build tests that require iree-import-onnx and iree-compile" FORCE)
    message(WARNING
        "IREE tools not found. Disabling IREE inference tests.\n"
        "  iree-import-onnx: ${IREE_IMPORT_ONNX}\n"
        "  iree-compile: ${IREE_COMPILE}\n"
        "  To enable, install the IREE toolchain and ensure iree-import-onnx and iree-compile are in PATH."
    )
else ()
    set(ENABLE_IREE_TESTS ON CACHE BOOL "Build tests that require iree-import-onnx and iree-compile" FORCE)
    message(STATUS "IREE inference tests enabled")
endif ()
