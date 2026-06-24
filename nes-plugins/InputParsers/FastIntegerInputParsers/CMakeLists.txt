# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# fast_float ships an integer `from_chars` overload alongside the float one. The
# FastFloat plugin already FetchContent_Declare's "fastfloat"; CMake dedups by
# the declare-name, so declaring it again here is safe and order-independent.
FetchContent_Declare(
        fastfloat
        GIT_REPOSITORY https://github.com/fastfloat/fast_float
        GIT_TAG        v8.2.4
)
FetchContent_MakeAvailable(fastfloat)

add_plugin_as_library(FastINT8 InputParser nes-input-formatters-registry fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastINT16  InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastINT32  InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastINT64  InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastUINT8  InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastUINT16 InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastUINT32 InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
add_plugin(FastUINT64 InputParser fast_integer_input_parser_plugin FastIntegerInputParser.cpp)
nautilus_inline(fast_integer_input_parser_plugin)

target_link_libraries(fast_integer_input_parser_plugin PRIVATE FastFloat::fast_float)
target_include_directories(fast_integer_input_parser_plugin PUBLIC include PRIVATE .)
