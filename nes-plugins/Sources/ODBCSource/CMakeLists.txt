# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(ODBC Source nes-sources-registry odbc_source_plugin_library ODBCSource.cpp ODBCConnection.cpp)
add_plugin_as_library(ODBC SourceValidation nes-sources-registry odbc_source_validation_plugin_library ODBCSource.cpp ODBCConnection.cpp)
# InlineData / FileData plugins live in ODBCTestLoader.cpp so ODBCSource.{hpp,cpp}
# stay a pure runtime reader with no test-scaffold awareness. The loader seeds the
# PostgreSQL table from the `.test` file's ATTACH rows; see ODBCTestLoader.cpp.
add_plugin_as_library(ODBC InlineData nes-sources-registry odbc_inline_data_plugin_library ODBCTestLoader.cpp)
add_plugin_as_library(ODBC FileData nes-sources-registry odbc_file_data_plugin_library ODBCTestLoader.cpp)

# unixODBC is the ODBC driver manager. It is a system dependency (apt: unixodbc-dev,
# nix: unixODBC) rather than a vcpkg port: the runtime driver it loads (psqlodbc /
# "PostgreSQL Unicode") is itself a system component registered in /etc/odbcinst.ini,
# so the whole ODBC stack lives consistently at the system level. CMake's builtin
# FindODBC module provides the ODBC::ODBC imported target.
find_package(ODBC REQUIRED)

foreach (_lib odbc_source_plugin_library odbc_source_validation_plugin_library
        odbc_inline_data_plugin_library odbc_file_data_plugin_library)
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
