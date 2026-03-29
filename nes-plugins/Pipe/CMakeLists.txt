# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# --- PipeService shared library (used by both sink and source) ---
find_package(folly REQUIRED)
add_library(pipe-service PipeService.cpp)
target_include_directories(pipe-service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_link_libraries(pipe-service PUBLIC nes-memory folly::folly PRIVATE nes-executable)

# --- Pipe Source plugin ---
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
add_plugin_as_library(Pipe Source nes-sources-registry pipe_source_plugin_library PipeSource.cpp)
add_plugin_as_library(Pipe SourceValidation nes-sources-registry pipe_source_validation_plugin_library PipeSource.cpp)
target_include_directories(pipe_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_include_directories(pipe_source_validation_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_link_libraries(pipe_source_plugin_library PRIVATE pipe-service)
target_link_libraries(pipe_source_validation_plugin_library PRIVATE pipe-service)

# --- Pipe Sink plugin ---
add_plugin_as_library(Pipe Sink nes-sinks-registry pipe_sink_plugin PipeSink.cpp)
add_plugin_as_library(Pipe SinkValidation nes-sinks-registry pipe_sink_validation_plugin PipeSink.cpp)
target_include_directories(pipe_sink_plugin PRIVATE .)
target_include_directories(pipe_sink_validation_plugin PRIVATE .)
target_link_libraries(pipe_sink_plugin PRIVATE pipe-service)
target_link_libraries(pipe_sink_validation_plugin PRIVATE pipe-service)

# --- Tests ---
add_tests_if_enabled(tests)

if (ENABLE_DOCKER_TESTS)
    if (ENABLE_BATS_TESTS)
        add_test(
                NAME pipe-plugin-e2e-test
                COMMAND
                env NES_CLI=$<TARGET_FILE:nes-cli>
                env NES_CLI_TESTDATA=${CMAKE_CURRENT_SOURCE_DIR}/tests/bats
                env NEBULASTREAM=$<TARGET_FILE:nes-single-node-worker>
                env NES_TEST_TMP_DIR=${CMAKE_BINARY_DIR}/test-tmp
                env NES_RUNTIME_BASE_IMAGE=${NES_RUNTIME_BASE_IMAGE}
                ${BATS} -x --jobs 8 --verbose-run ${CMAKE_CURRENT_SOURCE_DIR}/tests/bats/pipe.bats
        )
        set_tests_properties(pipe-plugin-e2e-test PROPERTIES LABELS "DockerCompose" FIXTURES_REQUIRED RuntimeBaseImage)
    endif ()
endif ()
