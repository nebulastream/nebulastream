# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(boost_system REQUIRED CONFIG)
find_package(boost_filesystem REQUIRED CONFIG)
find_package(boost_process REQUIRED CONFIG)
find_package(boost_graph REQUIRED CONFIG)

add_library(nes-inference ModelLoader.cpp Model.cpp)
target_include_directories(nes-inference PUBLIC include PRIVATE .)
target_link_libraries(nes-inference PUBLIC nes-data-types nes-grpc nes-logical-operators PRIVATE Boost::system Boost::filesystem Boost::process Boost::graph nes-common)

function(add_nes_test_nebuli)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-inference Boost::system Boost::filesystem nes-common nes-data-types nes-grpc nes-logical-operators )
    target_include_directories(${TARGET_NAME} PRIVATE .)
endfunction()

add_tests_if_enabled(tests)
target_compile_definitions(nes-inference PUBLIC NEBULI_INFERENCE_SUPPORT)

add_plugin_as_library(InferenceModelName LogicalOperator nes-logical-operators nes-inference-model-name-operator LogicalInferModelNameOperator.cpp)
add_plugin_as_library(InferenceModel LogicalOperator nes-logical-operators nes-inference-model-operator LogicalInferModelOperator.cpp)

target_link_libraries(nes-inference-model-name-operator PRIVATE nes-inference)
target_link_libraries(nes-inference-model-operator PRIVATE nes-inference)
