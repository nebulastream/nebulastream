# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks if the chosen c++ stldib matches required minimum version.
# If libc++ is chosen, also enables hardening mode.

include(CheckCXXSourceCompiles)

if (USE_CPP_STDLIB STREQUAL "libcxx")
    # check if libc++ available and at least version 19
    set(CMAKE_REQUIRED_FLAGS "-std=c++23 -nostdinc++ -isystem ${USE_CPP_STDLIB_LIBCXX_PATH}/include/c++/v1")
    set(CMAKE_REQUIRED_LINK_OPTIONS "-L${USE_CPP_STDLIB_LIBCXX_PATH}/lib -lc++ -rpath ${USE_CPP_STDLIB_LIBCXX_PATH}/lib")
    check_cxx_source_compiles("
        #include <cstddef>
        #if defined(_LIBCPP_VERSION) && _LIBCPP_VERSION >= 190000
            int main() { return 0; }
        #else
            #error \"libc++ version is below 19\"
        #endif
    " LIBCXX_VERSION_CHECK)

    if (NOT LIBCXX_VERSION_CHECK)
        message(FATAL_EROR "libc++ not found or version below 19")
    endif ()
    add_compile_options(-nostdinc++ -isystem ${USE_CPP_STDLIB_LIBCXX_PATH}/include/c++/v1)
    add_compile_options(-fexperimental-library)
    # Enable Libc++ hardening mode
    add_compile_definitions($<$<CONFIG:DEBUG>:_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_DEBUG>)
    add_compile_definitions($<$<CONFIG:RelWithDebInfo>:_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST>)

    add_link_options(-L${USE_CPP_STDLIB_LIBCXX_PATH}/lib -lc++ -rpath ${USE_CPP_STDLIB_LIBCXX_PATH}/lib)
elseif (USE_CPP_STDLIB STREQUAL "libstdcxx")
    # Check if Libstdc++ version is 14 or above
    set(CMAKE_REQUIRED_FLAGS "-std=c++23")
    check_cxx_source_compiles("
        #include <cstddef>
        #if defined(_GLIBCXX_RELEASE) && _GLIBCXX_RELEASE >= 14
            int main() { return 0; }
        #else
            #error \"libstdc++ version is below 14\"
        #endif
    " LIBSTDCXX_VERSION_CHECK)

    if (NOT LIBSTDCXX_VERSION_CHECK)
        message(FATAL_ERROR "Requires Libstdc++ >= 14. On ubuntu systems this can be installed via 'apt install g++-14'")
    endif ()

    message(STATUS "Libstdc++ >= 14")
else ()
    message(FATAL_ERROR "Unexpected value for USE_CPP_STDLIB: ${USE_CPP_STDLIB}")
endif ()
