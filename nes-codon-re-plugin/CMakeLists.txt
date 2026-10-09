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
find_package(re2 CONFIG REQUIRED)
find_package(PCRE2 CONFIG REQUIRED COMPONENTS 8BIT)

# Shadows Codon's stdlib re module (codon/re.codon) and provides the natives it calls: RE2, with PCRE2 for the patterns
# RE2 rejects.
add_codon_extension_plugin(nes-codon-re-plugin
    PLUGIN_NAME re
    CODON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/codon"
    ADDITIONAL_SOURCES src/RegexBackend.cpp
    NATIVE_SYMBOLS
        seq_re_match
        seq_re_match_one
        seq_re_escape
        seq_re_compile
        seq_re_purge
        seq_re_pattern_groups
        seq_re_group_name_to_index
        seq_re_group_index_to_name
        seq_re_check_rewrite_string
        seq_re_pattern_error
    NATIVE_SYMBOL_HEADERS RegexBackend.hpp
    LINK_LIBRARIES re2::re2 PCRE2::8BIT)
target_include_directories(nes-codon-re-plugin PRIVATE src)
