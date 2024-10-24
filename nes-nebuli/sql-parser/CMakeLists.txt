# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_custom_command(
        OUTPUT
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLBaseListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLLexer.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLParser.cpp
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/NebulaSQL.g4
        COMMAND antlr4 -Dlanguage=Cpp ${CMAKE_CURRENT_SOURCE_DIR}/NebulaSQL.g4 -o ${CMAKE_CURRENT_BINARY_DIR}
)

find_package(antlr4-runtime CONFIG REQUIRED)
add_library(sql-parser
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLBaseListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLLexer.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLListener.cpp
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaSQLParser.cpp
)

target_include_directories(sql-parser PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${ANTLR4_INCLUDE_DIR})
target_link_libraries(sql-parser PUBLIC antlr4_static nes-common)
