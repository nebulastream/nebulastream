
### Query Merger Rule Test ###
add_nes_unit_test(optimizer-query-merger-test
        HashSignatureBasedCompleteQueryMergerRuleTest.cpp
        #HashSignatureBasedPartialQueryMergerRuleTest.cpp
        SyntaxBasedCompleteQueryMergerRuleTest.cpp
        SyntaxBasedPartialQueryMergerRuleTest.cpp
        Z3SignatureBasedBottomUpQueryContainmentRuleTest.cpp
        Z3SignatureBasedCompleteQueryMergerRuleTest.cpp
        Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp
        Z3SignatureBasedPartialQueryMergerRuleTest.cpp
        Z3SignatureBasedTreeBasedQueryContainmentMergerRuleTest.cpp)
