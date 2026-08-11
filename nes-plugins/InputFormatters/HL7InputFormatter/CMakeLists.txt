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
add_plugin_as_library(HL7 InputFormatIndexer nes-input-formatters-registry hl7_input_format_indexer_plugin_library HL7InputFormatIndexer.cpp)
add_plugin(HL7 InputFormatterValidation nes-input-formatters HL7InputFormatIndexer.cpp)
add_plugin_as_library(SIMDHL7 InputFormatIndexer nes-input-formatters-registry simd_hl7_input_format_indexer_plugin_library SIMDHL7InputFormatIndexer.cpp Hl7SimdKernel.cpp)
add_plugin(SIMDHL7 InputFormatterValidation nes-input-formatters SIMDHL7InputFormatIndexer.cpp Hl7SimdKernel.cpp)
# Sequential variant (ablation baseline rung): same SIMD flatten + band FIF, IsSequential=true. Registered into
# the SAME plugin library so Hl7SimdKernel.cpp is compiled exactly once per library.
add_plugin(SequentialHL7 InputFormatIndexer simd_hl7_input_format_indexer_plugin_library SequentialHL7InputFormatIndexer.cpp)
add_plugin(SequentialHL7 InputFormatterValidation nes-input-formatters SequentialHL7InputFormatIndexer.cpp)
target_include_directories(hl7_input_format_indexer_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
target_include_directories(simd_hl7_input_format_indexer_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
