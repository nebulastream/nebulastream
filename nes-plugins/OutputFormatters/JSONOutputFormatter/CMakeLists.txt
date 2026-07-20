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
add_plugin_as_library(JSON OutputFormatter nes-output-formatters-registry json_output_formatter_plugin JSONOutputFormatter.cpp)
add_plugin_as_library(JSON OutputFormatterValidation nes-output-formatters-registry json_output_formatter_validation_plugin JSONOutputFormatter.cpp)

target_include_directories(json_output_formatter_plugin
        PUBLIC include
        PRIVATE .
)

target_include_directories(json_output_formatter_validation_plugin
        PUBLIC include
        PRIVATE .
)

# Compile the runtime formatter with the nautilus inlining pass so its own NAUTILUS_INLINE helper
# writeRawBytes (the lazy value passthrough write) gets its bitcode registered and can be spliced
# into the JIT'd output pipeline. Without this, add_plugin_as_library builds the plugin TU WITHOUT
# -fpass-plugin, so writeRawBytes is never registered and inline_invoke_calls cannot reach it -- it
# stays an opaque per-row proxy call. The shared core helpers (writeBytesToBuffer, getMemArea, ...)
# already splice because nes-output-formatters is pass-compiled; this closes the plugin-local gap.
# Only the runtime plugin runs the JIT; the validation plugin never compiles a pipeline.
nautilus_inline(json_output_formatter_plugin)
