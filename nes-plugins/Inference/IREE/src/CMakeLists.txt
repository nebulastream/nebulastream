# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/FetchIREE.cmake)

add_plugin_as_library(IREE Operator nes-execution-registry iree_inference_plugin_library
        IREEInferenceOperator.cpp
        IREEInferenceOperatorHandler.cpp
        IREEAdapter.cpp
        IREECompilerWrapper.cpp
        IREERuntimeWrapper.cpp
)

set(_IREE_COMPILER_API "${IREE_SOURCE_DIR}/compiler/bindings/c/iree/compiler")

target_include_directories(iree_inference_plugin_library PRIVATE ../include)
target_include_directories(iree_inference_plugin_library SYSTEM PRIVATE ${_IREE_COMPILER_API})

target_link_libraries(iree_inference_plugin_library PRIVATE
        iree::runtime
        iree::base
        iree::vm
        iree::hal
        iree::hal::drivers
        iree::modules::hal
        iree_compiler_API_Internal_CompilerDriver
        iree_compiler_plugins_input_Torch_torch-mlir_CAPI
        nes-execution
        nes-runtime
)
