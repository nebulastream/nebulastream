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

find_package(antlr4-generator CONFIG REQUIRED)

set(ANTLR_JAR_FILE "antlr-${ANTLR_VERSION}-complete.jar")

if (DEFINED ENV{NES_PREBUILT_VCPKG_ROOT})
  # if in prebuilt docker, expect jar _with correct version_ in /opt
  if (NOT EXISTS "/opt/${ANTLR_JAR_FILE}")
    message(FATAL_ERROR "could not find expected ANTLR jar file /opt/${ANTLR_JAR_FILE}. "
                        "Maybe there is a version mismatch?")
  endif ()
  set(ANTLR4_JAR_LOCATION "/opt/${ANTLR_JAR_FILE}")
else ()
  include(FetchContent)
  # The value of ANTLR_JAR_FILE (containing the ANTLR version) is set by antlr4-generator.
  # On mismatch: update antlr version in dockerfile, then update hashes here and in docker.
  FetchContent_Declare(
    antlr4_jar
    URL      https://www.antlr.org/download/${ANTLR_JAR_FILE}
    URL_HASH SHA256=eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76
    DOWNLOAD_NO_EXTRACT TRUE
  )
  FetchContent_MakeAvailable(antlr4_jar)
  set(ANTLR4_JAR_LOCATION "${antlr4_jar_SOURCE_DIR}/${ANTLR_JAR_FILE}")
endif ()

# generate BOTH parser and lexer, and additionaly listener
antlr4_generate(NES_SQL_PARSER ${CMAKE_CURRENT_SOURCE_DIR}/AntlrSQL.g4 BOTH -listener)

# The VCPKG port is called 'antlr4', finding the package requires to look for 'antlr4-runtime' and
# adding antlr library as a dependency to another library, requires to link against 'antlr-static'.
find_package(antlr4-runtime CONFIG REQUIRED)
add_library(nes-sql-parser ${SQL_PARSER_SOURCE_FILES} ${ANTLR4_SRC_FILES_NES_SQL_PARSER})

target_include_directories(nes-sql-parser PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        ${ANTLR4_INCLUDE_DIR_NES_SQL_PARSER} ${ANTLR4_INCLUDE_DIR}
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)
target_link_libraries(nes-sql-parser PUBLIC antlr4_static nes-common nes-sinks nes-sources nes-logical-operators)
