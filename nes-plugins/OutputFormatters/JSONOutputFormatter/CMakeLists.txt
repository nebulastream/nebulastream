# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# One implementation library serving all registries this plugin participates in.
add_library(json_output_formatter_plugin STATIC JSONOutputFormatter.cpp)
target_link_libraries(json_output_formatter_plugin PRIVATE nes-output-formatters)
target_include_directories(json_output_formatter_plugin PUBLIC include ${CMAKE_CURRENT_SOURCE_DIR})

find_package(simdjson CONFIG REQUIRED)
target_link_libraries(json_output_formatter_plugin PRIVATE simdjson::simdjson)

link_plugin_library(nes-output-formatters json_output_formatter_plugin)
add_registry_entry(OutputFormatter JSON)
add_registry_entry(OutputFormatterValidation JSON)
