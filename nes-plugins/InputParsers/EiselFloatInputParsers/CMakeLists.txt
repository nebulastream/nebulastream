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

# Hand-rolled fast_float (Eisel-Lemire) float parsers (no fast_float / from_chars / external deps): the ASCII
# scan and the 128-bit power-of-five rounding are ordinary C++ in a proxy function that nautilus inlines into
# the JIT'd scan loop. The 128-bit power-of-five table is vendored under include/PowersOfFive.hpp.
add_plugin_as_library(EiselFloatF32 InputParser nes-input-formatters-registry eisel_float_input_parser_plugin EiselFloatInputParser.cpp)
add_plugin(EiselFloatF64 InputParser eisel_float_input_parser_plugin EiselFloatInputParser.cpp)
nautilus_inline(eisel_float_input_parser_plugin)

target_include_directories(eisel_float_input_parser_plugin PUBLIC include PRIVATE .)
