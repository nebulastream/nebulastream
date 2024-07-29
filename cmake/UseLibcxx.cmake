# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

option(USE_LIBCXX_IF_AVAILABLE "Use Libc++ if supported by the system" ON)
# Determine if libc++ is available by invoking the compiler with -std=libc++ and examine _LIBCPP_VERSION
execute_process(
        COMMAND ${CMAKE_COMMAND} -E echo "#include<ciso646> \n int main(){return 0;}"
        COMMAND ${CMAKE_CXX_COMPILER} -E -stdlib=libc++ -x c++ -dM -
        COMMAND grep "#define _LIBCPP_VERSION"
        OUTPUT_STRIP_TRAILING_WHITESPACE
        OUTPUT_VARIABLE LIBCXX_CHECK_OUTPUT
        ERROR_VARIABLE LIBCXX_CHECK_ERROR
        RESULT_VARIABLE LIBCXX_CHECK_RESULT
)

if (LIBCXX_CHECK_RESULT EQUAL 0 AND ${USE_LIBCXX_IF_AVAILABLE})
    message(STATUS "Using Libc++")
    add_compile_options(-stdlib=libc++)
    # Currently C++20 threading features are hidden behind the feature flag
    add_compile_options(-fexperimental-library)
    add_link_options(-lc++)
endif ()