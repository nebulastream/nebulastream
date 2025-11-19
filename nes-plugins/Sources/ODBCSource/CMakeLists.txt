# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(ODBC Source nes-sources-registry ODBC_source_plugin_library ODBCSource.cpp)
add_plugin_as_library(ODBC SourceValidation nes-sources-registry ODBC_source_validation_plugin_library ODBCSource.cpp)

set(unofficial-unixodbc_DIR /vcpkg/gt)
find_package(unixodbc CONFIG REQUIRED)

target_link_libraries(ODBC_source_plugin_library PRIVATE UNIX::odbc)
target_link_libraries(ODBC_source_validation_plugin_library PRIVATE UNIX::odbc)

#find_package(ODBC REQUIRED)
#target_link_libraries(ODBC_source_plugin_library PRIVATE ODBC::ODBC)
#target_link_libraries(ODBC_source_validation_plugin_library PRIVATE ODBC::ODBC)

add_executable(msconnect connectTest.cpp
        ODBCConnection.hpp)
target_link_libraries(msconnect PUBLIC UNIX::odbc)
