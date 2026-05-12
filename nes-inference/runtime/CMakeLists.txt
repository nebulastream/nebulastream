# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(NES_INFERENCE_RUNTIME_SOURCES src/InferenceRuntime.cpp)
set(NES_INFERENCE_RUNTIME_PRIVATE_LIBS nes-common)

if (NES_ENABLE_IREE_INFERENCE_BACKEND)
    find_package(IREERuntime CONFIG REQUIRED)
    list(APPEND NES_INFERENCE_RUNTIME_SOURCES src/IreeRuntimeBackend.cpp)
    list(APPEND NES_INFERENCE_RUNTIME_PRIVATE_LIBS iree_runtime_runtime)
endif ()

if (NES_ENABLE_OPENVINO_INFERENCE_BACKEND)
    find_package(OpenVINO CONFIG REQUIRED COMPONENTS Runtime)
    list(APPEND NES_INFERENCE_RUNTIME_SOURCES src/OpenVinoRuntimeBackend.cpp)
endif ()

add_library(nes-inference-runtime ${NES_INFERENCE_RUNTIME_SOURCES})
target_include_directories(nes-inference-runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(nes-inference-runtime
        PUBLIC nes-inference
        PRIVATE ${NES_INFERENCE_RUNTIME_PRIVATE_LIBS})
if (NES_ENABLE_OPENVINO_INFERENCE_BACKEND)
    target_link_openvino_runtime(nes-inference-runtime)
endif ()
