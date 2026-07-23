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

# Check if the OpenVINO model converter is available for inference-backed tests.
# Auto-detect by default, but honor a user-provided -DENABLE_INFERENCE_TESTS=ON/OFF.
find_program(OPENVINO_CONVERTER ovc)
if (OPENVINO_CONVERTER STREQUAL "OPENVINO_CONVERTER-NOTFOUND")
    if (DEFINED ENABLE_INFERENCE_TESTS AND ENABLE_INFERENCE_TESTS)
        message(FATAL_ERROR
            "ENABLE_INFERENCE_TESTS=ON but the OpenVINO converter was not found.\n"
            "  ovc: ${OPENVINO_CONVERTER}\n"
            "  Install the OpenVINO python package and ensure ovc is in PATH, or pass -DENABLE_INFERENCE_TESTS=OFF."
        )
    endif ()
    set(ENABLE_INFERENCE_TESTS OFF CACHE BOOL "Build tests that require the ovc model converter" FORCE)
    message(WARNING
        "OpenVINO converter not found. Disabling inference tests.\n"
        "  ovc: ${OPENVINO_CONVERTER}\n"
        "  To enable, install the OpenVINO python package and ensure ovc is in PATH."
    )
else ()
    if (NOT DEFINED ENABLE_INFERENCE_TESTS)
        set(ENABLE_INFERENCE_TESTS ON CACHE BOOL "Build tests that require the ovc model converter")
    endif ()
    if (ENABLE_INFERENCE_TESTS)
        message(STATUS "Inference tests enabled")
    else ()
        message(STATUS "Inference tests disabled (user override)")
    endif ()
endif ()
