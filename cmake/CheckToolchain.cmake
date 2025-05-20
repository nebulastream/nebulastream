# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Picks a standard c++ library. By default we opt into libc++ for its hardening mode. However if libc++ is not available
# we fallback to libstdc++. The user can manually opt out of libc++ by disabling the USE_LIBCXX_IF_AVAILABLE option.
# Currently NebulaStream requires Libc++-19 or Libstdc++-14 or above.

include(CheckCXXSourceCompiles)

option(USE_LIBCXX_IF_AVAILABLE "Use Libc++ if supported by the system" ON)
SET(USING_LIBCXX OFF)
SET(USING_LIBSTDCXX OFF)

if (USE_LIBCXX_IF_AVAILABLE)
    # Determine if libc++ is available by invoking the compiler with -std=libc++ and examine _LIBCPP_VERSION
    set(CMAKE_REQUIRED_FLAGS "-std=c++23 -stdlib=libc++")
    check_cxx_source_compiles("
        #include <cstddef>
        #if defined(_LIBCPP_VERSION) && _LIBCPP_VERSION >= 190000
            int main() { return 0; }
        #else
            #error \"libc++ version is not 19 or above\"
        #endif
    " LIBCXX_VERSION_CHECK)

    if (LIBCXX_VERSION_CHECK)
        message(STATUS "Using Libc++")
        set(USING_LIBCXX ON)
    else ()
        message(STATUS "Not using Libc++")
        set(USING_LIBCXX OFF)
    endif ()
endif ()

if (NOT ${USING_LIBCXX})
    # Check if Libstdc++ version is 14 or above
    set(CMAKE_REQUIRED_FLAGS "-std=c++23")
    check_cxx_source_compiles("
    #include <cstddef>
    #if defined(_GLIBCXX_RELEASE) && _GLIBCXX_RELEASE >= 14
        int main() { return 0; }
    #else
        #error \"libstdc++ version is not above 14\"
    #endif
" LIBSTDCXX_VERSION_CHECK)

    if (LIBSTDCXX_VERSION_CHECK)
        set(USING_LIBSTDCXX ON)
        message(STATUS "Libstdc++ >= 14")
    else ()
        message(FATAL_ERROR "Requires Libstdc++ >= 14. On ubuntu systems this can be installed via g++-14")
    endif ()
endif ()

if (${USING_LIBCXX})
    add_compile_options(-stdlib=libc++)
    # Currently C++20 threading features are hidden behind the feature flag
    add_compile_options(-fexperimental-library)
    # Enable Libc++ hardening mode
    add_compile_definitions($<$<CONFIG:DEBUG>:_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_DEBUG>)
    add_compile_definitions($<$<CONFIG:RelWithDebInfo>:_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST>)
    add_link_options(-lc++)
endif ()
