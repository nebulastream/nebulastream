# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(ODBC Sink nes-sinks-registry odbc_sink_plugin_library ODBCSink.cpp)
add_plugin_as_library(ODBC SinkValidation nes-sinks-registry odbc_sink_validation_plugin_library ODBCSink.cpp)

# The SinkCapture plugin lives in ODBCSinkCapture.cpp so ODBCSink.{hpp,cpp}
# stays a pure runtime writer with no test-scaffold awareness. The capture
# reader rewrites the sink's db_host / db_port to the dispatcher-allocated
# endpoint and polls the target table back into the systest result file — the
# sink-side mirror of the ODBC source's ODBCTestLoader.
add_plugin_as_library(ODBC SinkCapture nes-sinks-registry odbc_sink_capture_plugin_library ODBCSinkCapture.cpp)

# unixODBC is the ODBC driver manager. It is a system dependency (apt:
# unixodbc-dev, nix: unixODBC) rather than a vcpkg port — see
# Sources/ODBCSource/CMakeLists.txt for the full rationale. CMake's builtin
# FindODBC module provides the ODBC::ODBC imported target.
find_package(ODBC REQUIRED)

foreach (_lib odbc_sink_plugin_library odbc_sink_validation_plugin_library odbc_sink_capture_plugin_library)
    target_include_directories(${_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(${_lib} PRIVATE ODBC::ODBC)
endforeach ()

# External-dependency systests: every `*.test` file under `systests/` that
# declares `# requires: odbc` is registered as a ctest entry by the macro.
# The co-located `systest-profile/` brings up a PostgreSQL container; the
# in-binary dispatcher (ExternalSystestDispatch.cpp) drives docker compose and
# the test runs in-process. add_external_systest_profile is a no-op when
# ENABLE_DOCKER_TESTS is OFF, so this needs no if() guard.
add_external_systest_profile(
        PROFILE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/systest-profile
)
