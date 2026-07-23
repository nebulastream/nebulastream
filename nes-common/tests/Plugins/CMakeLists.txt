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

# Host executable for the dynamic plugin loading test. Deliberately NOT using add_nes_test: the
# host must link ONLY the shared core (plus gtest), never the static component libraries, so it
# shares registry singletons with the plugin. libnes is linked PUBLIC so the registry glue
# sub-library (which links this executable for usage requirements) can see the core's headers;
# linking an executable also requires ENABLE_EXPORTS.
add_executable(plugin-catalog-test PluginCatalogTest.cpp)
set_target_properties(plugin-catalog-test PROPERTIES ENABLE_EXPORTS ON)
target_link_libraries(plugin-catalog-test PUBLIC libnes GTest::gtest_main)

# Test-only registry owned by the host executable, announced through the built-in collection.
# The entry returns its own registry key.
create_runtime_registry(TestPlugin plugin-catalog-test
        ENTRY_TEMPLATE "[] { return std::string{\"\${KEY}\"}; }")

# Fully generated test plugin: a MODULE with no handwritten sources — its only TU is the
# generated nes_plugin_describe contributing one entry to the host's TestPluginRegistry.
add_library(nes-test-plugin MODULE)
target_link_libraries(nes-test-plugin PRIVATE libnes)
target_include_directories(nes-test-plugin PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/registry/include)
add_dynamic_registry_entry(nes-test-plugin TestPlugin TestEntry)
finalize_dynamic_plugin(nes-test-plugin)

target_compile_definitions(plugin-catalog-test PRIVATE NES_TEST_PLUGIN_PATH="$<TARGET_FILE:nes-test-plugin>")
add_dependencies(plugin-catalog-test nes-test-plugin)
add_test(NAME plugin-catalog-test COMMAND plugin-catalog-test)
