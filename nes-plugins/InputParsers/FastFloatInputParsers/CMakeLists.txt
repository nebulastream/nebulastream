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

FetchContent_Declare(
        fastfloat
        GIT_REPOSITORY https://github.com/fastfloat/fast_float
        GIT_TAG        v8.2.4
)
FetchContent_MakeAvailable(fastfloat)

add_plugin_as_library(FastFloatF32 InputParser nes-input-formatters-registry fastfloat_input_parser_plugin FastFloatInputParser.cpp)
add_plugin(FastFloatF64 InputParser fastfloat_input_parser_plugin FastFloatInputParser.cpp)
nautilus_inline(fastfloat_input_parser_plugin)

target_link_libraries(fastfloat_input_parser_plugin PRIVATE FastFloat::fast_float)
target_include_directories(fastfloat_input_parser_plugin PUBLIC include PRIVATE .)
