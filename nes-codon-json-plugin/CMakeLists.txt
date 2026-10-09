# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include("${PROJECT_SOURCE_DIR}/nes-codon-plugin/cmake/NESCodonPlugin.cmake")
find_package(simdjson CONFIG REQUIRED)

add_codon_extension_plugin(nes-codon-json-plugin
    PLUGIN_NAME json
    CODON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/codon"
    ADDITIONAL_SOURCES src/SimdJsonBackend.cpp
    NATIVE_SYMBOLS nes_json_parse
    NATIVE_SYMBOL_HEADERS JsonParser.hpp
    LINK_LIBRARIES simdjson::simdjson)
target_include_directories(nes-codon-json-plugin PRIVATE src)
