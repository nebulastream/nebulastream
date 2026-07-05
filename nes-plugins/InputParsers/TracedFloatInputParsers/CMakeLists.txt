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

# Traced float parsers: the common decimal fast path (SWAR-free scalar accumulate + Clinger scale) is
# emitted as nautilus trace ops so it fuses into the JIT'd scan loop (unlike Eisel/Fast, whose bodies stay
# a native call). Hard cases (exponent notation, > 15/7 digits) fall back to a vendored fast_float proxy.
# Reuse the SAME FetchContent name as the FastFloat plugin so MakeAvailable dedupes (a different name
# would re-add the fast_float subproject and collide on the FastFloat::fast_float target).
FetchContent_Declare(
        fastfloat
        GIT_REPOSITORY https://github.com/fastfloat/fast_float
        GIT_TAG        v8.2.4
)
FetchContent_MakeAvailable(fastfloat)

add_plugin_as_library(TracedFloatF32 InputParser nes-input-formatters-registry traced_float_input_parser_plugin TracedFloatInputParser.cpp)
add_plugin(TracedFloatF64 InputParser traced_float_input_parser_plugin TracedFloatInputParser.cpp)
nautilus_inline(traced_float_input_parser_plugin)

target_link_libraries(traced_float_input_parser_plugin PRIVATE FastFloat::fast_float)
target_include_directories(traced_float_input_parser_plugin PUBLIC include PRIVATE .)
