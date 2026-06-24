# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# Hand-rolled simdjson-style integer parsers (no fast_float / from_chars / external deps): the SWAR digit
# scanner is ordinary C++ in a proxy function that nautilus inlines into the JIT'd scan loop.
add_plugin_as_library(SimdINT8 InputParser nes-input-formatters-registry simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdINT16  InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdINT32  InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdINT64  InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdUINT8  InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdUINT16 InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdUINT32 InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
add_plugin(SimdUINT64 InputParser simd_integer_input_parser_plugin SimdIntegerInputParser.cpp)
nautilus_inline(simd_integer_input_parser_plugin)

target_include_directories(simd_integer_input_parser_plugin PUBLIC include PRIVATE .)
