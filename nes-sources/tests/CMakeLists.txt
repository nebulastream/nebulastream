# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-sources-test-utils Util/TestSource.cpp)
target_include_directories(nes-sources-test-utils PUBLIC ../private Util)
target_link_libraries(nes-sources-test-utils PUBLIC nes-sources nes-memory-test-utils nes-input-formatters)

function(add_nes_source_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-memory-test-utils nes-sources-test-utils)
endfunction()

add_nes_source_test(source-thread-test SourceThreadTest.cpp)
add_nes_source_test(source-catalog-test SourceCatalogTest.cpp)
add_nes_source_test(tokio-source-test TokioSourceTest.cpp)
target_link_libraries(tokio-source-test nes-io-bindings)
# Two Rust crates (nes_rust_bindings, nes_source_lib) both statically link the CXX runtime,
# resulting in duplicate cxxbridge symbols. Allow the linker to resolve this by keeping the
# first definition (both are identical -- same CXX version's runtime glue).
target_link_options(tokio-source-test PRIVATE "LINKER:--allow-multiple-definition")

add_nes_source_test(tokio-sink-test TokioSinkTest.cpp)
target_link_libraries(tokio-sink-test nes-io-bindings)
target_link_options(tokio-sink-test PRIVATE "LINKER:--allow-multiple-definition")
