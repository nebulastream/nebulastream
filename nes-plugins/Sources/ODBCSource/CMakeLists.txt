# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(ODBC Source odbc_source_plugin_library ODBCSource.cpp ODBCTypeMappings.cpp)
add_plugin_as_library(ODBC SourceValidation odbc_source_validation_plugin_library ODBCSource.cpp ODBCTypeMappings.cpp)

# unixODBC is the ODBC driver manager. The driver it loads at runtime (msodbcsql18 /
# "ODBC Driver 18 for SQL Server") is a system component registered in
# /etc/odbcinst.ini. CMake's builtin FindODBC module provides the ODBC::ODBC
# imported target.
find_package(ODBC REQUIRED)

foreach (_lib odbc_source_plugin_library odbc_source_validation_plugin_library)
    target_include_directories(${_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(${_lib} PRIVATE ODBC::ODBC)
endforeach ()
