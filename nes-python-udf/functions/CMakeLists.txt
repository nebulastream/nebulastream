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

# The Python UDF backends of the UdfLowering registry, and the Codon-compiled physical function behind the Codon backend.
add_library(nes-python-udf STATIC
        CodonUdfLowering.cpp
        PythonPhysicalFunction.cpp
        PythonUdfRuntime.cpp)
target_include_directories(nes-python-udf PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(nes-python-udf PRIVATE nes-physical-operators nes-udf-runtime nes-codon-plugin nes-python-udf-compiler codon::compiler)

link_plugin_library(nes-physical-operators nes-python-udf)
add_registry_entry(UdfLowering Codon)
