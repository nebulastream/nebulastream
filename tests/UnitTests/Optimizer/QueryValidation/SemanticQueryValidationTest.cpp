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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Services/QueryParsingService.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class SemanticQueryValidationTest : public testing::Test {
  public:
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    std::shared_ptr<QueryParsingService> queryParsingService;

    ~SemanticQueryValidationTest() override = default;

    void SetUp() override {
        NES::setupLogging("SemanticQueryValidationTest.log", NES::LOG_NONE);
        NES_INFO("Setup SemanticQueryValidationTest class.");

        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }
    void TearDown() override { NES_INFO("Tear down SemanticQueryValidationTest class."); }

    static void PrintQString(const std::string& s) { std::cout << std::endl << "QUERY STRING:" << std::endl << s << std::endl; }

    void CallValidation(const std::string& queryString) {
        PrintQString(queryString);
        StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
        auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);
        QueryPtr filterQuery = queryParsingService->createQueryFromCodeString(queryString);
        filterQuery->sink(FileSinkDescriptor::create(""));
        semanticQueryValidation->checkSatisfiability(filterQuery);
    }

    void TestForException(std::string queryString) { EXPECT_THROW(CallValidation(queryString), InvalidQueryException); }

};

// Positive test for a semantically valid query
TEST_F(SemanticQueryValidationTest, satisfiableQueryWithSingleFilter) {
    NES_INFO("Satisfiable Query with single filter");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("value") < 10); )";

    CallValidation(queryString);
}

// Positive test for a semantically valid query with an AND condition
TEST_F(SemanticQueryValidationTest, satisfiableQueryWithLogicalExpression) {
    NES_INFO("Satisfiable Query with logical expression");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("value") > 10 && Attribute("value") < 100); )";

    CallValidation(queryString);
}

// Test a query with contradicting filters in an AND condition
TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLogicalExpression) {
    NES_INFO("Unatisfiable Query with logical expression");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("value") < 10 && Attribute("value") > 100); )";

    TestForException(queryString);
}

// Positive test for a semantically valid query with multiple filter operators
TEST_F(SemanticQueryValidationTest, satisfiableQueryWithMultipleFilters) {
    NES_INFO("Satisfiable Query with multiple filters");

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 10).filter(Attribute("value") > 10); )";

    CallValidation(queryString);
}

// Test a query with contradicting filters over multiple filter operators
TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithMultipleFilters) {
    NES_INFO("Unsatisfiable Query with multiple filters");

    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"id\") > 10).filter(Attribute(\"value\") > "
                              "10).filter(Attribute(\"id\") < 10); ";

    TestForException(queryString);
}

// Positive test for a semantically valid query with programmatically added filter operators
TEST_F(SemanticQueryValidationTest, satisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Satisfiable Query with later added filters");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 10).filter(Attribute("value") < 10); )";

    QueryPtr filterQuery = queryParsingService->createQueryFromCodeString(queryString);

    filterQuery->filter(Attribute("id") != 42);
    filterQuery->filter(Attribute("value") < 42);

    filterQuery->sink(FileSinkDescriptor::create(""));

    semanticQueryValidation->checkSatisfiability(filterQuery);
}

// Test a query with contradicting filters over multiple programmatically added filter operators
TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Unatisfiable Query with later added filters");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString = R"(Query::from("default_logical").filter(Attribute("id") > 100).filter(Attribute("value") < 10); )";

    QueryPtr filterQuery = queryParsingService->createQueryFromCodeString(queryString);

    filterQuery->filter(Attribute("id") == 42);
    filterQuery->filter(Attribute("value") < 42);

    filterQuery->sink(FileSinkDescriptor::create(""));

    EXPECT_THROW(semanticQueryValidation->checkSatisfiability(filterQuery), InvalidQueryException);
}

// Test a query with an invalid logical stream name
TEST_F(SemanticQueryValidationTest, invalidLogicalStreamTest) {
    NES_INFO("Invalid logical stream");

    std::string queryString =
        R"(Query::from("nonexistent_logical").filter(Attribute("id") > 100).filter(Attribute("value") < 10); )";

    TestForException(queryString);
}

// Test a query with invalid attributes
TEST_F(SemanticQueryValidationTest, invalidAttributesInLogicalStreamTest) {
    NES_INFO("Invalid attributes in logical stream");

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("nonex_1") > 100).filter(Attribute("nonex_2") < 10); )";

    TestForException(queryString);
}

// Test a query with a valid "as" operator
// This test is disabled, because the signature inference breaks when using this operator
TEST_F(SemanticQueryValidationTest, DISABLED_validAsOperatorTest) {
    NES_INFO("Valid as operator test");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString = R"(Query::from("default_logical").as("dl").filter(Attribute("value") > 100); )";

    CallValidation(queryString);
}

// Test a query with a, invalid "as" operator
// This test is disabled, because the signature inference breaks when using this operator
TEST_F(SemanticQueryValidationTest, DISABLED_invalidAsOperatorTest) {
    NES_INFO("Invalid as operator test");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);

    std::string queryString = R"(Query::from("default_logical").as("value").filter(Attribute("value") > 100); )";

    TestForException(queryString);
}

// Test a query with a valid "project" operator
// This test is disabled, because the signature inference breaks when using this operator
TEST_F(SemanticQueryValidationTest, DISABLED_validProjectionTest) {
    NES_INFO("Valid projection test");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);

    auto query = Query::from("default_logical")
                     .project(Attribute("id").rename("new_id"), Attribute("value"))
                     .filter(Attribute("new_id") < 42)
                     .map(Attribute("value") = Attribute("value") + 2)
                     .sink(FileSinkDescriptor::create(""));

    semanticQueryValidation->checkSatisfiability(std::make_shared<Query>(query));
}

// Test a query with an invalid "project" operator
// This test is disabled, because the signature inference breaks when using this operator
TEST_F(SemanticQueryValidationTest, DISABLED_invalidProjectionTest) {
    NES_INFO("Invalid projection test");

    StreamCatalogPtr streamCatalogPtr = std::make_shared<StreamCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalogPtr);

    auto query = Query::from("default_logical")
                     .map(Attribute("value") = Attribute("value") + 2)
                     .project(Attribute("id").rename("new_id"), Attribute("value"))
                     .filter(Attribute("id") < 42)
                     .sink(FileSinkDescriptor::create(""));

    EXPECT_THROW(semanticQueryValidation->checkSatisfiability(std::make_shared<Query>(query)), InvalidQueryException);
}

}// namespace NES