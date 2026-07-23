# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(OpenSSL REQUIRED)
find_package(OpenVINO CONFIG REQUIRED COMPONENTS Runtime)

# OpenVINO's plugins are loaded through a link group so the static plugin registry
# resolves against the runtime library regardless of link order.
function(target_link_openvino_runtime TARGET_NAME)
    target_link_libraries(${TARGET_NAME} PRIVATE
            openvino::runtime
            "$<LINK_GROUP:RESCAN,$<TARGET_FILE:openvino::runtime>,$<TARGET_FILE:openvino::openvino_auto_plugin>,$<TARGET_FILE:openvino::openvino_intel_cpu_plugin>,$<TARGET_FILE:openvino::frontend::ir>,$<TARGET_FILE:openvino::snippets>,$<TARGET_FILE:openvino::reference>,$<TARGET_FILE:openvino::shape_inference>,$<TARGET_FILE:openvino::openvino_common_translators>,$<TARGET_FILE:openvino::dnnl>,$<TARGET_FILE:openvino::mlas>,$<TARGET_FILE:openvino::util>,$<TARGET_FILE:openvino::itt>>")
endfunction()

add_library(nes-inference
        src/Inference.cpp
        src/Model.cpp
        src/ModelCatalog.cpp
        src/BackendTool.cpp
        src/OpenVINO/OpenVinoImporter.cpp)
target_include_directories(nes-inference PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/src/OpenVINO)
target_link_libraries(nes-inference PUBLIC nes-data-types PRIVATE nes-common OpenSSL::Crypto)
target_link_openvino_runtime(nes-inference)

add_subdirectory(runtime)

add_tests_if_enabled(tests)
