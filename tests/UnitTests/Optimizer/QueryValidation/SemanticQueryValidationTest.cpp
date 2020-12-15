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

#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>


namespace NES {

class SemanticQueryValidationTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SemanticQueryValidationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SemanticQueryValidationTest class.");
    }
    void TearDown() { NES_INFO("Tear down SemanticQueryValidationTest class."); }
};

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithSingleFilter) {
    NES_INFO("Satisfiable Query with single filter");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), true);
}

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithLogicalExpression) {
    NES_INFO("Satisfiable Query with logical expression");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") > 10 && Attribute(\"value\") < 100); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), true);
}

TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLogicalExpression) {
    NES_INFO("Unatisfiable Query with logical expression");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 10 && Attribute(\"value\") > 100); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), false);
}

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithMultipleFilters) {
    NES_INFO("Satisfiable Query with multiple filters");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") > 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), true);
}

TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithMultipleFilters) {
    NES_INFO("Unsatisfiable Query with multiple filters");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") > 10).filter(Attribute(\"id\") < 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), false);
}

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Satisfiable Quer with later added filters");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") < 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    filterQuery->filter(Attribute("id") != 42);
    filterQuery->filter(Attribute("value") < 42);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), true);
}

TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Unatisfiable Quer with later added filters");

    SemanticQueryValidation semanticQueryValidation;

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 100).filter(Attribute(\"value\") < 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);

    filterQuery->filter(Attribute("id") == 42);
    filterQuery->filter(Attribute("value") < 42);

    ASSERT_EQ(semanticQueryValidation.isSatisfiable(filterQuery), false);
}

}// namespace NES