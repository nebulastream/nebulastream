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

include(FindODBC)
if (NOT ODBC_FOUND)
    message(FATAL_ERROR "Could not find ODBC. Install `unixodbc-dev` or disable the plugin.")
endif()

target_link_libraries(ODBC_source_plugin_library PRIVATE ODBC::ODBC)
target_link_libraries(ODBC_source_validation_plugin_library PRIVATE ODBC::ODBC)
