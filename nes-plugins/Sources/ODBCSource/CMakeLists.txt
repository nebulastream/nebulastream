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

# One implementation library serving both registries this plugin participates in.
add_library(odbc_source_plugin_library STATIC ODBCSource.cpp ODBCTypeMappings.cpp)
target_link_libraries(odbc_source_plugin_library PRIVATE nes-sources)
target_include_directories(odbc_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

# unixODBC is the ODBC driver manager. The driver it loads at runtime (msodbcsql18 /
# "ODBC Driver 18 for SQL Server") is a system component registered in
# /etc/odbcinst.ini. CMake's builtin FindODBC module provides the ODBC::ODBC
# imported target.
find_package(ODBC REQUIRED)
target_link_libraries(odbc_source_plugin_library PRIVATE ODBC::ODBC)

link_plugin_library(nes-sources odbc_source_plugin_library)
add_registry_entry(Source ODBC)
add_registry_entry(SourceValidation ODBC)
