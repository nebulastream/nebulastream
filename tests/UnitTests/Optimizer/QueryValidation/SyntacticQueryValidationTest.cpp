/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <gtest/gtest.h>

#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>


namespace NES {

class SyntacticQueryValidationTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SyntacticQueryValidationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SyntacticQueryValidationTest class.");
    }
    void TearDown() { NES_INFO("Tear down SyntacticQueryValidationTest class."); }
};

TEST_F(SyntacticQueryValidationTest, validQueryTest) {
    NES_INFO("Valid Query test");

    SyntacticQueryValidation syntacticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100); "
    ;

    ASSERT_EQ(syntacticQueryValidation.isValid(queryString), true);
}

TEST_F(SyntacticQueryValidationTest, missingSemicolonTest) {
    NES_INFO("Missing semicolon test");

    SyntacticQueryValidation syntacticQueryValidation;

    // missing ; at line end
    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100) "
    ;

    ASSERT_EQ(syntacticQueryValidation.isValid(queryString), false);   
}

TEST_F(SyntacticQueryValidationTest, typoInFilterTest) {
    NES_INFO("Typo in filter test");

    SyntacticQueryValidation syntacticQueryValidation;

    // filter is misspelled as fliter
    std::string queryString =
        "Query::from(\"default_logical\").fliter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100); "
    ;

    ASSERT_EQ(syntacticQueryValidation.isValid(queryString), false);
}

TEST_F(SyntacticQueryValidationTest, missingClosingParenthesisTest) {
    NES_INFO("Missing closing parenthesis test");

    SyntacticQueryValidation syntacticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 && Attribute(\"id\") < 100; "
    ;

    ASSERT_EQ(syntacticQueryValidation.isValid(queryString), false);
}

TEST_F(SyntacticQueryValidationTest, invalidBoolOperatorTest) {
    NES_INFO("Invalid bool operator test");

    SyntacticQueryValidation syntacticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10 & Attribute(\"id\") < 100); "
    ;

    ASSERT_EQ(syntacticQueryValidation.isValid(queryString), false);
}

}// namespace NES