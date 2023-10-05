# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Type inference test
add_nes_unit_test(type-inference-tests "UnitTests/Phases/TypeInferencePhaseTest.cpp")

# FilterMergeTest
add_nes_unit_test(filter-merge-rule-test "UnitTests/QueryRewrite/FilterMergeRuleTest.cpp")

# FilterPushDownTest
add_nes_unit_test(filter-push-down-rule-test "UnitTests/QueryRewrite/FilterPushDownRuleTest.cpp")

# PredicateReorderingTest
add_nes_unit_test(predicate-reordering-rule-test "UnitTests/QueryRewrite/PredicateReorderingRuleTest.cpp")

# FilterSplitUpTest
add_nes_unit_test(filter-split-up-rule-test "UnitTests/QueryRewrite/FilterSplitUpRuleTest.cpp")

# DistributeWindowRuleTest
add_nes_unit_test(distribute-window-rule-test "UnitTests/QueryRewrite/DistributeWindowRuleTest.cpp")

# DistributeWindowRuleTest
add_nes_unit_test(originid-inference-phase-test "UnitTests/Phases/OriginIdInferencePhaseTest.cpp")

### Choose MemLayout Phase Tests ###
add_nes_unit_test(memory-layout-selection-phase-tests "UnitTests/Phases/MemoryLayoutSelectionPhaseTest.cpp")

### Query Merger Phase Tests ###
add_nes_unit_test(global-query-plan-update-phase-tests "UnitTests/Phases/GlobalQueryPlanUpdatePhaseTest.cpp")

### Logical Source Expansion Rule Tests ###
add_nes_unit_test(logical-source-expansion-rule-test "UnitTests/QueryRewrite/LogicalSourceExpansionRuleTest.cpp")

### Map UDFs Open CL Operators Rule Tests ###
add_nes_unit_test(map-udf-to-opencl-rule-test "UnitTests/QueryRewrite/MapUDFsToOpenCLOperatorsRuleTest.cpp")

### Syntax Based Equal Query Merger Rule Test ###
add_nes_unit_test(syntax-based-equal-query-merger-rule-test "UnitTests/QueryMerger/SyntaxBasedCompleteQueryMergerRuleTest.cpp")

### Syntax Based Partial Query Merger Rule Test ###
add_nes_unit_test(syntax-based-partial-query-merger-rule-test "UnitTests/QueryMerger/SyntaxBasedPartialQueryMergerRuleTest.cpp")

### Query Placement Tests ###
add_nes_unit_test(query-placement-test "UnitTests/QueryPlacement/QueryPlacementTest.cpp")

add_nes_unit_test(ilp-placement-test "UnitTests/QueryPlacement/ILPPlacementTest.cpp")

if (NES_USE_TF)
    add_nes_unit_test(ml-heuristic-placement-test "UnitTests/QueryPlacement/MlHeuristicPlacementTest.cpp")
endif ()

add_nes_unit_test(nemo-placement-test "UnitTests/QueryPlacement/NemoPlacementTest.cpp")

### Syntactic Query Validation Test ###
add_nes_unit_test(syntactic-query-validation-test "UnitTests/QueryValidation/SyntacticQueryValidationTest.cpp")

### Semantic Query Validation Test ###
add_nes_unit_test(semantic-query-validation-test "UnitTests/QueryValidation/SemanticQueryValidationTest.cpp")

### Operator To FOL Util Test ###
add_nes_unit_test(query-signature-util-tests "UnitTests/QuerySignatures/QuerySignatureUtilTests.cpp")

### Z3 Signature Inference Phase Test ###
add_nes_unit_test(z3-signature-inference-phase-test "UnitTests/Phases/SignatureInferencePhaseTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-bottom-up-query-containment-rule-test "UnitTests/QueryMerger/Z3SignatureBasedBottomUpQueryContainmentRuleTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-tree-based-query-containment-rule-test "UnitTests/QueryMerger/Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(query-containment-identification-test "UnitTests/QuerySignatures/QueryContainmentIdentificationTest.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-equal-query-merger-rule-test "UnitTests/QueryMerger/Z3SignatureBasedCompleteQueryMergerRuleTest.cpp")

### Z3 Signature Based Partial Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-partial-query-merger-rule-test "UnitTests/QueryMerger/Z3SignatureBasedPartialQueryMergerRuleTest.cpp")

### Z3 Signature Based Partial Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-partial-query-merger-bottom-up-rule-test "UnitTests/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp")

### String Signature Based Complete Query Merger Rule Test ###
add_nes_unit_test(hash-signature-based-equal-query-merger-rule-test "UnitTests/QueryMerger/HashSignatureBasedCompleteQueryMergerRuleTest.cpp")

### Attribute Sort Rule Test ###
add_nes_unit_test(attribute-sort-rule-test "UnitTests/QueryRewrite/AttributeSortRuleTest.cpp")

### Rename Source To Project Operator Rule Test ###
add_nes_unit_test(rename-source-to-project-operator-rule-test "UnitTests/QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp")

### Project Before Union Operator Rule Test ###
add_nes_unit_test(project-before-union-operator-rule-test "UnitTests/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp")

### Binary Operator Sort Rule Tests ###
add_nes_unit_test(binary-operator-sort-rule-tests "UnitTests/QueryRewrite/BinaryOperatorSortRuleTest.cpp")


