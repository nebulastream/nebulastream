# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(NES_INFERENCE_SOURCES
        src/Inference.cpp
        src/Model.cpp
        src/ModelCatalog.cpp
        src/BackendTool.cpp)

set(NES_INFERENCE_PRIVATE_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(NES_INFERENCE_PRIVATE_LIBS nes-common OpenSSL::Crypto)
set(NES_INFERENCE_COMPILE_DEFINITIONS)

find_package(OpenSSL REQUIRED)

if (NES_ENABLE_IREE_INFERENCE_BACKEND)
    list(APPEND NES_INFERENCE_SOURCES
            src/IREE/IreeImporter.cpp
            src/IREE/IreeCompiler.cpp
            src/IREE/MlirAnalyzer.cpp)
    list(APPEND NES_INFERENCE_PRIVATE_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/src/IREE)
    list(APPEND NES_INFERENCE_COMPILE_DEFINITIONS NES_ENABLE_INFERENCE_BACKEND_IREE)
endif ()

if (NES_ENABLE_OPENVINO_INFERENCE_BACKEND)
    find_package(OpenVINO CONFIG REQUIRED COMPONENTS Runtime)
    function(target_link_openvino_runtime TARGET_NAME)
        target_link_libraries(${TARGET_NAME} PRIVATE
                openvino::runtime
                "$<LINK_GROUP:RESCAN,$<TARGET_FILE:openvino::runtime>,$<TARGET_FILE:openvino::openvino_auto_plugin>,$<TARGET_FILE:openvino::openvino_intel_cpu_plugin>,$<TARGET_FILE:openvino::frontend::ir>,$<TARGET_FILE:openvino::snippets>,$<TARGET_FILE:openvino::reference>,$<TARGET_FILE:openvino::shape_inference>,$<TARGET_FILE:openvino::openvino_common_translators>,$<TARGET_FILE:openvino::dnnl>,$<TARGET_FILE:openvino::mlas>,$<TARGET_FILE:openvino::util>,$<TARGET_FILE:openvino::itt>>")
    endfunction()

    list(APPEND NES_INFERENCE_SOURCES src/OpenVINO/OpenVinoImporter.cpp)
    list(APPEND NES_INFERENCE_PRIVATE_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/src/OpenVINO)
    list(APPEND NES_INFERENCE_COMPILE_DEFINITIONS NES_ENABLE_INFERENCE_BACKEND_OPENVINO)
endif ()

add_library(nes-inference ${NES_INFERENCE_SOURCES})
target_include_directories(nes-inference PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${NES_INFERENCE_PRIVATE_INCLUDES})
target_compile_definitions(nes-inference PUBLIC ${NES_INFERENCE_COMPILE_DEFINITIONS})
target_link_libraries(nes-inference PUBLIC nes-data-types PRIVATE ${NES_INFERENCE_PRIVATE_LIBS})
if (NES_ENABLE_OPENVINO_INFERENCE_BACKEND)
    target_link_openvino_runtime(nes-inference)
endif ()

add_subdirectory(runtime)

add_tests_if_enabled(tests)
