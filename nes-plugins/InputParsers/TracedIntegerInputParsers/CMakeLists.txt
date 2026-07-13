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

# Pure traced-nautilus integer parsers (no fast_float / external deps): the SWAR digit logic is emitted
# as nautilus ops in parseToVarVal, so it fuses into the JIT'd scan loop on every platform.
add_plugin_as_library(TracedINT8 InputParser nes-input-formatters-registry traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedINT16  InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedINT32  InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedINT64  InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedUINT8  InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedUINT16 InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedUINT32 InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
add_plugin(TracedUINT64 InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
# BOOL rides in the integer plugin: the first-byte decision is pure integer compares, and a separate
# plugin library for one tiny parser isn't worth the registration boilerplate.
add_plugin(TracedBOOL   InputParser traced_integer_input_parser_plugin TracedIntegerInputParser.cpp)
nautilus_inline(traced_integer_input_parser_plugin)

target_include_directories(traced_integer_input_parser_plugin PUBLIC include PRIVATE .)
