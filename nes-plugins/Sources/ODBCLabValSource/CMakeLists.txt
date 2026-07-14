# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin_as_library(ODBCLabVal Source nes-sources-registry odbc_labval_source_plugin_library ODBCLabValSource.cpp ODBCConnection.cpp)
add_plugin_as_library(ODBCLabVal SourceValidation nes-sources-registry odbc_labval_source_validation_plugin_library ODBCLabValSource.cpp ODBCConnection.cpp)

find_package(unixodbc CONFIG REQUIRED)

foreach (_lib odbc_labval_source_plugin_library odbc_labval_source_validation_plugin_library)
    target_include_directories(${_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/)
    target_link_libraries(${_lib} PRIVATE UNIX::odbc)
endforeach ()
