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
add_plugin_as_library(HL7 OutputFormatter nes-output-formatters-registry hl7_output_formatter_plugin HL7OutputFormatter.cpp)
add_plugin_as_library(HL7 OutputFormatterValidation nes-output-formatters-registry hl7_output_formatter_validation_plugin HL7OutputFormatter.cpp)

target_include_directories(hl7_output_formatter_plugin
        PUBLIC include
        PRIVATE .
)

target_include_directories(hl7_output_formatter_validation_plugin
        PUBLIC include
        PRIVATE .
)

# Compile the runtime formatter with the nautilus inlining pass so its NAUTILUS_INLINE helper
# writeRawBytes (the raw passthrough write) gets its bitcode registered and can be spliced into
# the JIT'd output pipeline (see the JSONOutputFormatter CMakeLists for the full rationale).
# Only the runtime plugin runs the JIT; the validation plugin never compiles a pipeline.
nautilus_inline(hl7_output_formatter_plugin)
