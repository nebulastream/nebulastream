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
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include "Exceptions/InvalidQueryException.hpp"


namespace NES {

class SemanticQueryValidationTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SemanticQueryValidationTest.log", NES::LOG_NONE);
        NES_INFO("Setup SemanticQueryValidationTest class.");
    }
    void TearDown() { NES_INFO("Tear down SemanticQueryValidationTest class."); }

    void PrintQString(std::string s) { std::cout << std::endl << "QUERY STRING:" << std::endl << s << std::endl; }

    void CallValidation(std::string queryString){
        PrintQString(queryString);
        StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
        SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);
        QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);
        filterQuery->sink(FileSinkDescriptor::create(""));
        semanticQueryValidation->isSatisfiable(filterQuery);
    }

    void TestForException(std::string queryString){
        EXPECT_THROW(CallValidation(queryString), InvalidQueryException);
    }
};

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithSingleFilter) {
    NES_INFO("Satisfiable Query with single filter");

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 10); "
        ;
    
    CallValidation(queryString);
}

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithLogicalExpression) {
    NES_INFO("Satisfiable Query with logical expression");

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") > 10 && Attribute(\"value\") < 100); "
        ;
    
    CallValidation(queryString);
}

TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLogicalExpression) {
    NES_INFO("Unatisfiable Query with logical expression");

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 10 && Attribute(\"value\") > 100); "
        ;
    
    TestForException(queryString);
}

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithMultipleFilters) {
    NES_INFO("Satisfiable Query with multiple filters");

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") > 10); "
        ;
    
    CallValidation(queryString);
}

TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithMultipleFilters) {
    NES_INFO("Unsatisfiable Query with multiple filters");

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") > 10).filter(Attribute(\"id\") < 10); "
        ;
    
    TestForException(queryString);
}

TEST_F(SemanticQueryValidationTest, satisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Satisfiable Query with later added filters");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
    SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") < 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);
    
    filterQuery->filter(Attribute("id") != 42);
    filterQuery->filter(Attribute("value") < 42);

    filterQuery->sink(FileSinkDescriptor::create(""));
    
    semanticQueryValidation->isSatisfiable(filterQuery);
}

TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Unatisfiable Query with later added filters");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
    SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);


    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"id\") > 100).filter(Attribute(\"value\") < 10); "
        ;
    
    QueryPtr filterQuery = UtilityFunctions::createQueryFromCodeString(queryString);
    
    filterQuery->filter(Attribute("id") == 42);
    filterQuery->filter(Attribute("value") < 42);
    
    filterQuery->sink(FileSinkDescriptor::create(""));

    EXPECT_THROW(semanticQueryValidation->isSatisfiable(filterQuery), InvalidQueryException);
}


TEST_F(SemanticQueryValidationTest, invalidLogicalStreamTest) {
    NES_INFO("Invalid logical stream");

    std::string queryString =
        "Query::from(\"nonexistent_logical\").filter(Attribute(\"id\") > 100).filter(Attribute(\"value\") < 10); "
        ;
    
    TestForException(queryString);
}

TEST_F(SemanticQueryValidationTest, invalidAttributesInLogicalStreamTest) {
    NES_INFO("Invalid attributes in logical stream");

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"nonex_1\") > 100).filter(Attribute(\"nonex_2\") < 10); "
        ;
    
    TestForException(queryString);
}


TEST_F(SemanticQueryValidationTest, DISABLED_asOperatorTest) {
    NES_INFO("asOperatorTest");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
    SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString =
        "Query::from(\"default_logical\").as(\"dl\").filter(Attribute(\"value\") > 100); "
        ;
    
    CallValidation(queryString);
}

TEST_F(SemanticQueryValidationTest, DISABLED_asOperatorNegativeTest) {
    NES_INFO("asOperatorNegativeTest");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
    SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString =
        "Query::from(\"default_logical\").as(\"value\").filter(Attribute(\"value\") > 100); "
        ;
    
    CallValidation(queryString);
}

TEST_F(SemanticQueryValidationTest, DISABLED_projectionTest) {
    NES_INFO("projectionTest");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
    SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);

    auto query = Query::from("default_logical")
                     .project(Attribute("id").rename("new_id"), Attribute("value"))
                     .filter(Attribute("new_id") < 42)
                     .map(Attribute("value") = Attribute("value") + 2)
                     .sink(FileSinkDescriptor::create(""));

    semanticQueryValidation->isSatisfiable(std::make_shared<Query>(query));

}

TEST_F(SemanticQueryValidationTest, DISABLED_projectionNegativeTest) {
    NES_INFO("projectionNegativeTest");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>();
    SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalogPtr);

    auto query = Query::from("default_logical")
                     .map(Attribute("value") = Attribute("value") + 2)
                     .project(Attribute("id").rename("new_id"), Attribute("value"))
                     .filter(Attribute("id") < 42)
                     .sink(FileSinkDescriptor::create(""));

    semanticQueryValidation->isSatisfiable(std::make_shared<Query>(query));

}


}// namespace NES