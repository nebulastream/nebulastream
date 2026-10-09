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

# Stand-ins for Python builtins that Codon's stdlib lacks. NES star-imports the nes_compat module into every Codon UDF
# module while this plugin is loaded (see PythonUdfFilesystem.cpp).
add_codon_extension_plugin(nes-codon-compat-plugin
    PLUGIN_NAME compat
    CODON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/codon")
