# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copy Test Data --------------------------------------------------------------
file(COPY TestData DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/TestData/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/TestData")

function(add_nebuli_test)
    add_nes_unit_test(${ARGN})
    target_link_libraries(${TARGET_NAME}
            PRIVATE nes-client nebuli-lib
    )
endfunction()

# Optimizer Unit Tests
add_nes_unit_test(optimizer-unit-tests "UnitTests/Optimizer/Phases/TypeInferencePhaseTest.cpp"
        "UnitTests/Optimizer/Phases/OriginIdInferencePhaseTest.cpp"
        "UnitTests/Optimizer/Phases/MemoryLayoutSelectionPhaseTest.cpp"
        "UnitTests/Optimizer/Phases/QueryRewritePhaseTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/FilterMergeRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/PredicateReorderingRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/FilterSplitUpRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/BinaryOperatorSortRuleTest.cpp")

add_compiler_unit_test(compiler-test "UnitTests/Compiler/JITCompilerTest.cpp"
        "UnitTests/Compiler/SharedLibraryTest.cpp"
        "UnitTests/Compiler/CompilationCacheTest.cpp"
        "UnitTests/Compiler/CompilerFlagsTest.cpp")

add_subdirectory(UnitTests)