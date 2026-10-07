# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_nes_test_semantic)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-semantic)
endfunction()

add_nes_test_semantic(SemanticModelCatalogTest SemanticModelCatalogTest.cpp)
add_nes_test_semantic(SemanticMapCodecTest SemanticMapCodecTest.cpp)
add_nes_test_semantic(SemanticFilterCodecTest SemanticFilterCodecTest.cpp)
add_nes_test_semantic(SemanticBackendFactoryTest SemanticBackendFactoryTest.cpp)
add_nes_test_semantic(HttpSemanticBackendTest HttpSemanticBackendTest.cpp)
add_nes_test_semantic(SemanticMapExecutorTest SemanticMapExecutorTest.cpp)
# nes-async carries the executor plugin as an interface link, so this one target pulls in
# both the registry and the implementation.
target_link_libraries(SemanticMapExecutorTest nes-async)
add_nes_test_semantic(SemanticFilterExecutorTest SemanticFilterExecutorTest.cpp)
target_link_libraries(SemanticFilterExecutorTest nes-async)
