# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(USE_SANITIZER "none" CACHE STRING "Enables sanitizer. asan, tsan, ubsan, none (default)")

if (DEFINED ENV{VCPKG_SANITIZER})
    SET(SANITIZER_OPTION $ENV{VCPKG_SANITIZER})
else ()
    set_property(CACHE USE_SANITIZER PROPERTY STRINGS "none" "asan" "tsan" "ubsan")
    string(TOLOWER "${USE_SANITIZER}" SANITIZER_OPTION)
endif ()

if (SANITIZER_OPTION STREQUAL "tsan")
    MESSAGE(STATUS "Enabling Thread Sanitizer")
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
elseif (SANITIZER_OPTION STREQUAL "ubsan")
    MESSAGE(STATUS "Enabling UB Sanitizer")
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)

    # TODO #799: Memory Alignment in NebulaStream
    add_compile_options(-fno-sanitize=alignment)
    add_link_options(-fno-sanitize=alignment)

elseif (SANITIZER_OPTION STREQUAL "asan")
    MESSAGE(STATUS "Enabling Address Sanitizer")
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
else ()
    MESSAGE(STATUS "Enabling No Sanitizer")
endif ()

option(USE_LIBFUZZER "" OFF)

if (USE_LIBFUZZER)
    add_compile_options(-fsanitize=fuzzer-no-link)
    add_link_options(-fsanitize=fuzzer-no-link)
endif ()
