# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include("${PROJECT_SOURCE_DIR}/nes-python-udf/codon-sdk/cmake/NESCodonPlugin.cmake")
find_package(utf8proc CONFIG REQUIRED)

# Shadows Codon's stdlib str methods (internal/str.codon) with methods that work on code points, as CPython's do, instead
# of bytes. The natives give the Unicode properties and case mappings: utf8proc and tables generated from CPython
# (generate_tables.py).
add_codon_extension_plugin(nes-codon-unicode-plugin
    PLUGIN_NAME unicode
    CODON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/codon"
    ADDITIONAL_SOURCES src/Unicode.cpp
    NATIVE_SYMBOLS
        nes_unicode_flags
        nes_unicode_map
        nes_unicode_decimal
    NATIVE_SYMBOL_HEADERS Unicode.hpp
    LINK_LIBRARIES utf8proc::utf8proc)
target_include_directories(nes-codon-unicode-plugin PRIVATE src)
