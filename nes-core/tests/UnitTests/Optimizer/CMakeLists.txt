
add_subdirectory(Phases)
add_subdirectory(QueryMerger)

# FilterMergeTest
add_nes_unit_test(filter-merge-rule-test "QueryRewrite/FilterMergeRuleTest.cpp")

# FilterPushDownTest
add_nes_unit_test(filter-push-down-rule-test "QueryRewrite/FilterPushDownRuleTest.cpp")

# PredicateReorderingTest
add_nes_unit_test(predicate-reordering-rule-test "QueryRewrite/PredicateReorderingRuleTest.cpp")

# FilterSplitUpTest
add_nes_unit_test(filter-split-up-rule-test "QueryRewrite/FilterSplitUpRuleTest.cpp")

# DistributeWindowRuleTest
add_nes_unit_test(distribute-window-rule-test "QueryRewrite/DistributeWindowRuleTest.cpp")

### Binary Operator Sort Rule Tests ###
add_nes_unit_test(binary-operator-sort-rule-tests "QueryRewrite/BinaryOperatorSortRuleTest.cpp")

### Logical Source Expansion Rule Tests ###
add_nes_unit_test(logical-source-expansion-rule-test "QueryRewrite/LogicalSourceExpansionRuleTest.cpp")

### Map UDFs Open CL Operators Rule Tests ###
add_nes_unit_test(map-udf-to-opencl-rule-test "QueryRewrite/MapUDFsToOpenCLOperatorsRuleTest.cpp")

### Query Placement Tests ###
add_nes_unit_test(query-placement-test "QueryPlacement/QueryPlacementTest.cpp")

add_nes_unit_test(ilp-placement-test "QueryPlacement/ILPPlacementTest.cpp")

if (NES_USE_TF)
    add_nes_unit_test(ml-heuristic-placement-test "QueryPlacement/MlHeuristicPlacementTest.cpp")
endif ()

add_nes_unit_test(nemo-placement-test "QueryPlacement/NemoPlacementTest.cpp")
#add_executable(nemo-placement-test "QueryPlacement/NemoPlacementTest.cpp")
#target_link_libraries(nemo-placement-test nes-core-test-util)
#add_test(NAME nemo-placement-test COMMAND nemo-placement-test)


### Syntactic Query Validation Test ###
add_nes_unit_test(syntactic-query-validation-test "QueryValidation/SyntacticQueryValidationTest.cpp")

### Semantic Query Validation Test ###
add_nes_unit_test(semantic-query-validation-test "QueryValidation/SemanticQueryValidationTest.cpp")
#target_link_libraries(semantic-query-validation-test nes-core-test-util)
#add_test(NAME semantic-query-validation-test COMMAND semantic-query-validation-test)

### Operator To FOL Util Test ###
add_nes_unit_test(query-signature-util-tests "QuerySignatures/QuerySignatureUtilTests.cpp")

### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(query-containment-identification-test "QuerySignatures/QueryContainmentIdentificationTest.cpp")

### Attribute Sort Rule Test ###
add_nes_unit_test(attribute-sort-rule-test "QueryRewrite/AttributeSortRuleTest.cpp")

### Rename Source To Project Operator Rule Test ###
add_nes_unit_test(rename-source-to-project-operator-rule-test "QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp")

### Project Before Union Operator Rule Test ###
add_nes_unit_test(project-before-union-operator-rule-test "QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp")