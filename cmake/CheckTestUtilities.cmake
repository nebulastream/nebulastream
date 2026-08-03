# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Check Docker once for both tests and image packaging.
find_program(DOCKER_EXECUTABLE docker)

# Docker commands in the development container use the host daemon, which cannot
# necessarily access paths from the container's filesystem. Write a unique token
# into the path, identify the development image through the container hostname,
# then start a sibling with the requested mount and verify that it reads the token.
# A native host uses the local filesystem directly and needs no probe.
function(verify_host_path_is_accessible_from_docker PATH MOUNT_SOURCE RESULT_VARIABLE)
    set(${RESULT_VARIABLE} TRUE PARENT_SCOPE)
    if (NOT EXISTS "/.dockerenv")
        return()
    endif ()

    string(RANDOM LENGTH 32 ALPHABET 0123456789abcdef _probe_token)
    set(_probe_file "${PATH}/docker-bind-probe-${_probe_token}")
    file(WRITE "${_probe_file}" "${_probe_token}")
    execute_process(
        COMMAND bash -c [=[
            probe_file="$1"
            probe_target="$2"
            mount_source="$3"
            image=$(docker inspect --format='{{.Config.Image}}' "$(hostname)") || exit 42
            docker run --rm --pull=never --entrypoint /bin/sh \
                -v "$mount_source:$probe_target:ro" "$image" \
                -c "cat '$probe_file'"
        ]=] _ "${_probe_file}" "${PATH}" "${MOUNT_SOURCE}"
        RESULT_VARIABLE _probe_result
        OUTPUT_VARIABLE _probe_output
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    file(REMOVE "${_probe_file}")
    if (_probe_result EQUAL 42)
        message(WARNING
            "Cannot identify the development container through its hostname. "
            "Do not override the development container's default hostname.")
        set(${RESULT_VARIABLE} FALSE PARENT_SCOPE)
    elseif (NOT _probe_result EQUAL 0 OR NOT _probe_output STREQUAL _probe_token)
        set(${RESULT_VARIABLE} FALSE PARENT_SCOPE)
    endif ()
endfunction()

if (DOCKER_EXECUTABLE)
    execute_process(
        COMMAND ${DOCKER_EXECUTABLE} version
        RESULT_VARIABLE DOCKER_CHECK_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
    )
else ()
    set(DOCKER_CHECK_RESULT 1)
endif ()

if (DOCKER_CHECK_RESULT EQUAL 0)
    set(NES_DOCKER_AVAILABLE ON)
else ()
    set(NES_DOCKER_AVAILABLE OFF)
endif ()

if (NES_DOCKER_AVAILABLE)
    if (NOT DEFINED VCPKG_HASH OR VCPKG_HASH STREQUAL "")
        execute_process(
            COMMAND ${CMAKE_SOURCE_DIR}/docker/dependency/hash_dependencies.sh
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE VCPKG_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE
            COMMAND_ERROR_IS_FATAL ANY
        )
    endif ()
    set(NES_RUNTIME_BASE_IMAGE "nebulastream/nes-runtime-base:${VCPKG_HASH}")
endif ()

if (ENABLE_DOCKER_TESTS)
    if (NOT NES_DOCKER_AVAILABLE)
        message(WARNING
            "ENABLE_DOCKER_TESTS is ON but docker is not working.\n"
            "  For dev container: Mount docker socket with -v /var/run/docker.sock:/var/run/docker.sock\n"
            "  For host system: Ensure docker is installed and running\n"
            "  Set -DENABLE_DOCKER_TESTS=OFF to suppress this warning\n"
            "Docker tests will be automatically disabled."
        )
        set(ENABLE_DOCKER_TESTS OFF CACHE BOOL "Runs testcases that require docker" FORCE)
    elseif (EXISTS "/.dockerenv")
        # Docker commands use the host daemon through its socket. Prove that
        # the build directory is mounted at the same absolute path on the host
        # before registering tests that use it as a Compose bind source.
        verify_host_path_is_accessible_from_docker(
            "${CMAKE_BINARY_DIR}" "${CMAKE_BINARY_DIR}" _docker_can_access_build_directory)
        if (NOT _docker_can_access_build_directory)
            message(WARNING
                "The host Docker daemon cannot read the CMake build directory at its container path:\n"
                "  ${CMAKE_BINARY_DIR}\n"
                "  Mount the repository at the same absolute path on the host and in the development container.\n"
                "  CLion mounts it at /tmp/nebulastream by default. For a checkout at /path/to/nebulastream,\n"
                "  add -v /path/to:/path/to to the Docker toolchain's container run options.\n"
                "Docker tests will be automatically disabled."
            )
            set(ENABLE_DOCKER_TESTS OFF CACHE BOOL "Runs testcases that require docker" FORCE)
        else ()
            message(STATUS "Docker tests enabled")
        endif ()
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
    # Smoke test to verify bats can load the helper libraries we depend on.
    set(_bats_libs_check_script "${CMAKE_BINARY_DIR}/check_bats_libs.bats")
    file(WRITE "${_bats_libs_check_script}" [[
@test "bats helper libraries loadable" {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-file
}
]])
    execute_process(
        COMMAND ${BATS} ${_bats_libs_check_script}
        RESULT_VARIABLE _bats_libs_check
        OUTPUT_VARIABLE _bats_libs_output
        ERROR_VARIABLE  _bats_libs_output
    )
    if (NOT _bats_libs_check EQUAL 0)
        set(ENABLE_BATS_TESTS OFF CACHE BOOL "Runs testcases that require bats" FORCE)
        message(WARNING
            "Bats found at ${BATS} but helper libraries are not loadable "
            "(BATS_LIB_PATH=$ENV{BATS_LIB_PATH}). Install via: "
            "apt install bats-support bats-assert bats-file, or adjust the "
            "BATS_LIB_PATH environment variable to point at their install dir.\n"
            "${_bats_libs_output}"
        )
    else ()
        set(ENABLE_BATS_TESTS ON CACHE BOOL "Runs testcases that require bats" FORCE)
        message(STATUS "Bats tests enabled: ${BATS}")
    endif ()
endif ()

# Check if IREE tools are available for inference-backed physical operator tests.
# Auto-detect by default, but honor a user-provided -DENABLE_IREE_TESTS=ON/OFF.
find_program(IREE_IMPORT_ONNX iree-import-onnx)
find_program(IREE_COMPILE iree-compile)
if (IREE_IMPORT_ONNX STREQUAL "IREE_IMPORT_ONNX-NOTFOUND" OR IREE_COMPILE STREQUAL "IREE_COMPILE-NOTFOUND")
    if (DEFINED ENABLE_IREE_TESTS AND ENABLE_IREE_TESTS)
        message(FATAL_ERROR
            "ENABLE_IREE_TESTS=ON but IREE tools were not found.\n"
            "  iree-import-onnx: ${IREE_IMPORT_ONNX}\n"
            "  iree-compile: ${IREE_COMPILE}\n"
            "  Install the IREE toolchain and ensure iree-import-onnx and iree-compile are in PATH, or pass -DENABLE_IREE_TESTS=OFF."
        )
    endif ()
    set(ENABLE_IREE_TESTS OFF CACHE BOOL "Build tests that require iree-import-onnx and iree-compile" FORCE)
    message(WARNING
        "IREE tools not found. Disabling IREE inference tests.\n"
        "  iree-import-onnx: ${IREE_IMPORT_ONNX}\n"
        "  iree-compile: ${IREE_COMPILE}\n"
        "  To enable, install the IREE toolchain and ensure iree-import-onnx and iree-compile are in PATH."
    )
else ()
    if (NOT DEFINED ENABLE_IREE_TESTS)
        set(ENABLE_IREE_TESTS ON CACHE BOOL "Build tests that require iree-import-onnx and iree-compile")
    endif ()
    if (ENABLE_IREE_TESTS)
        message(STATUS "IREE inference tests enabled")
    else ()
        message(STATUS "IREE inference tests disabled (user override)")
    endif ()
endif ()
