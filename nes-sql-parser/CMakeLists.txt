# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
get_source(nes-sql-parser SQL_PARSER_SOURCE_FILES)

find_program(antlr4_executable antlr4 REQUIRED)
add_custom_command(
        OUTPUT
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLBaseListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLLexer.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLParser.cpp
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/AntlrSQL.g4
        COMMAND ${antlr4_executable} -Dlanguage=Cpp ${CMAKE_CURRENT_SOURCE_DIR}/AntlrSQL.g4 -o ${CMAKE_CURRENT_BINARY_DIR}
)

# The VCPKG port is called 'antlr4', finding the package requires to look for 'antlr4-runtime' and
# adding antlr library as a dependency to another library, requires to link against 'antlr-static'.
find_package(antlr4-runtime CONFIG REQUIRED)
add_library(nes-sql-parser
        ${SQL_PARSER_SOURCE_FILES}
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLBaseListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLLexer.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQLParser.cpp
)

target_include_directories(nes-sql-parser PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        ${CMAKE_CURRENT_BINARY_DIR} ${ANTLR4_INCLUDE_DIR}
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)
target_link_libraries(nes-sql-parser PUBLIC antlr4_static nes-common nes-functions nes-sinks nes-sources nes-client nes-operators PRIVATE nes-client)

add_tests_if_enabled(tests)

