# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)
find_package(folly REQUIRED)
add_library(pipe-service STATIC PipeService.cpp)
target_include_directories(pipe-service PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(pipe-service PUBLIC nes-memory folly::folly PRIVATE nes-executable)

add_library(pipe_source_plugin_library STATIC PipeSource.cpp)
target_link_libraries(pipe_source_plugin_library PRIVATE nes-sources PUBLIC pipe-service)
target_include_directories(pipe_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-sources pipe_source_plugin_library)
add_registry_entry(Source Pipe)
add_registry_entry(SourceValidation Pipe)

add_library(pipe_sink_plugin STATIC PipeSink.cpp)
target_link_libraries(pipe_sink_plugin PRIVATE nes-sinks PUBLIC pipe-service)
target_include_directories(pipe_sink_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
link_plugin_library(nes-sinks pipe_sink_plugin)
add_registry_entry(Sink Pipe)
add_registry_entry(SinkValidation Pipe)

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
                ${BATS} -x --jobs 8 --verbose-run --timing ${CMAKE_CURRENT_SOURCE_DIR}/tests/bats/pipe.bats
        )
        set_tests_properties(pipe-plugin-e2e-test PROPERTIES LABELS "DockerCompose" FIXTURES_REQUIRED RuntimeBaseImage)
    endif ()
endif ()
