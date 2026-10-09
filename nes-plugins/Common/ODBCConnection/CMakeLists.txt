# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Not a plugin: a plain support library shared by every component that speaks ODBC (the ODBC
# source and the LOOKBACK function today). It registers nothing; consumers link it and get the
# connection-string dialects, diagnostics, and RAII handle wrappers.
add_library(odbc_connection STATIC ODBCConnection.cpp)
target_include_directories(odbc_connection PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

# PUBLIC: the header exposes SQLHANDLE/SQLSMALLINT in its signatures and NES_ODBC_CHECK expands to
# NES_WARNING plus a NES exception, so consumers need both include trees.
find_package(ODBC REQUIRED)
target_link_libraries(odbc_connection PUBLIC ODBC::ODBC nes-common)
