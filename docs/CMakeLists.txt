# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


find_package(Doxygen)
if (NOT ${DOXYGEN_FOUND})
        message(WARNING "Doxygen not found.")
        return ()
endif ()

if (${DOXYGEN_DOT_FOUND})
        set(DOXYGEN_HAVE_DOT YES)
else ()
        set(DOXYGEN_HAVE_DOT NO)
endif ()

execute_process(COMMAND ${DOXYGEN_EXECUTABLE} --version
                OUTPUT_VARIABLE DOXYGEN_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Doxygen version: ${DOXYGEN_VERSION}")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
               "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
               ESCAPE_QUOTES)

add_custom_target(nes-doc ${DOXYGEN_EXECUTABLE} "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
                  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile")


