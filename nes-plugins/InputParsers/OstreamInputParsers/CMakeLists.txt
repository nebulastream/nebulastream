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

# NOTE: intentionally NOT nautilus_inline'd -- iostream locale statics must stay out of the JIT module
# (__emutls). Opaque invoke; its per-value cost dwarfs the invoke boundary anyway.
add_plugin_as_library(OstreamF32 InputParser nes-input-formatters-registry ostream_input_parser_plugin OstreamInputParser.cpp)
add_plugin(OstreamF64 InputParser ostream_input_parser_plugin OstreamInputParser.cpp)

target_include_directories(ostream_input_parser_plugin PUBLIC include PRIVATE .)
