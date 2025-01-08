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


# Optimizer Unit Tests
add_nes_unit_test(optimizer-unit-tests "UnitTests/Optimizer/Phases/TypeInferencePhaseTest.cpp"
        "UnitTests/Optimizer/Phases/OriginIdInferencePhaseTest.cpp"
        "UnitTests/Optimizer/Phases/MemoryLayoutSelectionPhaseTest.cpp"
        "UnitTests/Optimizer/Phases/QueryPlacementAmendmentTest.cpp"
        "UnitTests/Optimizer/Phases/QueryRewritePhaseTest.cpp"
        "UnitTests/Optimizer/Phases/SignatureInferencePhaseTest.cpp"
        "UnitTests/Optimizer/Phases/StatisticIdInferenceTest.cpp"
        "UnitTests/Optimizer/QuerySignatures/QuerySignatureUtilTests.cpp"
        "UnitTests/Optimizer/QuerySignatures/QueryContainmentIdentificationTest.cpp"
        "UnitTests/Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/SyntaxBasedPartialQueryMergerRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedBottomUpQueryContainmentRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp"
        "UnitTests/Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/FilterMergeRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/PredicateReorderingRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/FilterSplitUpRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/MapUDFsToOpenCLOperatorsRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/AttributeSortRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp"
        "UnitTests/Optimizer/QueryRewrite/BinaryOperatorSortRuleTest.cpp")

### NEMO Placement Tests ###
add_nes_unit_test(distributed-matrix-join-test "UnitTests/Optimizer/QueryPlacement/DistributedMatrixJoinPlacementTest.cpp")
add_nes_unit_test(nemo-join-placement-test "UnitTests/Optimizer/QueryPlacement/NemoJoinPlacementTest.cpp")

### Global Query Plan Tests ###
add_nes_unit_test(global-query-plan-tests "UnitTests/Plans/Global/Query/GlobalQueryPlanTest.cpp")

### Thread Naming Test ###
add_nes_unit_test(global-execution-plan-tests "UnitTests/Plans/Global/Execution/GlobalExecutionPlanTest.cpp")

### ChangeLog Test ###
add_nes_unit_test(change-log-test "UnitTests/Plans/ChangeLog/ChangeLogTest.cpp")

### ChangeLogEntry Test ###
add_nes_unit_test(change-log-entry-test "UnitTests/Plans/ChangeLog/ChangeLogEntryTest.cpp")

if (NES_BUILD_PLUGIN_TENSORFLOW)
    add_nes_unit_test(ml-heuristic-placement-test "UnitTests/Optimizer/QueryPlacement/MlHeuristicPlacementTest.cpp")
endif ()
