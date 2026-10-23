# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Connection tests: a harness that steps one connector through the engine's source path,
# and a pytest runner that sequences it against external services.
# Each connector registers its tests with add_conn_test in cmake/macros.cmake.
if (NES_ENABLES_TESTS)
    # Deferred to the end of the configure step, after every connector has called add_conn_test.
    cmake_language(DEFER DIRECTORY ${CMAKE_SOURCE_DIR} CALL write_conn_test_connectors)

    # Source plugins link into nes-sources, so a new connector needs no edit here.
    add_library(conntest-binder STATIC
            driver/Binder.cpp
            driver/Reply.cpp
    )
    target_include_directories(conntest-binder PUBLIC driver)
    find_package(reflectcpp CONFIG REQUIRED)
    target_link_libraries(conntest-binder
            PUBLIC nes-sql-parser nes-sources nes-common
            PRIVATE reflectcpp::reflectcpp
    )

    # The connector runs in its own process, so the runner can sequence it step by step and kill and respawn it.
    add_executable(conntest-harness
            driver/ConntestStarter.cpp
            driver/ControlChannel.cpp
            driver/SessionCommand.cpp
            driver/SourceSession.cpp
    )

    find_package(argparse CONFIG REQUIRED)
    find_package(boost_asio CONFIG REQUIRED)
    target_link_libraries(conntest-harness PRIVATE
            conntest-binder
            nes-memory
            argparse::argparse
            Boost::asio
    )
    # The runner expects the binary at <build-dir>/nes-conn-test/conntest-harness.
    set_target_properties(conntest-harness PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

    # add_conn_test registers one ctest instance per connector.
    add_executable(conntest-config-test tests/ConfigBindTest.cpp)
    target_link_libraries(conntest-config-test PRIVATE conntest-binder nes-test-util)

    # The runner's own unit tests need no external service.
    # They run in the development image's venv (UV_PROJECT_ENVIRONMENT), or else in the one that uv creates.
    if (ENABLE_CONN_TESTS)
        add_test(NAME conntest-runner-tests
                COMMAND ${UV_EXECUTABLE} run --frozen pytest tests --build-dir=${CMAKE_BINARY_DIR}
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/runner
        )
        set_tests_properties(conntest-runner-tests PROPERTIES LABELS "ConnTest")
    endif ()
endif ()
