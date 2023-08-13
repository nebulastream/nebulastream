/*
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

#include "Common/DataTypes/DataTypeFactory.hpp"
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class SemanticQueryValidationTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    std::shared_ptr<QueryParsingService> queryParsingService;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SemanticQueryValidationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SemanticQueryValidationTest class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    static void PrintQString(const std::string& s) { NES_DEBUG("\nQUERY STRING:\n{}", s); }

    void CallValidation(const std::string& queryString) {
        PrintQString(queryString);
        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        std::string logicalSourceName = "default_logical";
        auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
        auto physicalSource = PhysicalSource::create(logicalSourceName, "phy1");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4, properties);
        auto sourceCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNode1);
        sourceCatalog->addPhysicalSource(logicalSourceName, sourceCatalogEntry);
        auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);
        QueryPlanPtr filterQuery = queryParsingService->createQueryFromCodeString(queryString);
        filterQuery = QueryPlanBuilder::addSink(filterQuery, FileSinkDescriptor::create(""));
        semanticQueryValidation->validate(filterQuery);
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

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("id") > 10).filter(Attribute("value") < 10).filter(Attribute("id") != 42).filter(Attribute("value") < 42); )";
    CallValidation(queryString);
}

// Test a query with contradicting filters over multiple programmatically added filter operators
TEST_F(SemanticQueryValidationTest, unsatisfiableQueryWithLaterAddedFilters) {
    NES_INFO("Unatisfiable Query with later added filters");

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("id") > 100).filter(Attribute("value") < 10).filter(Attribute("id") == 42).filter(Attribute("value") < 42); )";
    TestForException(queryString);
}

// Test a query with an invalid logical source name
TEST_F(SemanticQueryValidationTest, invalidLogicalSourceTest) {
    NES_INFO("Invalid logical source test");

    std::string queryString =
        R"(Query::from("nonexistent_logical").filter(Attribute("id") > 100).filter(Attribute("value") < 10); )";
    TestForException(queryString);
}

// Test a query with invalid attributes
TEST_F(SemanticQueryValidationTest, invalidAttributesInLogicalSourceTest) {
    NES_INFO("Invalid attributes in logical stream");

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("nonex_1") > 100).filter(Attribute("nonex_2") < 10); )";
    TestForException(queryString);
}

// Test a query with a valid "as" operator
// This test is disabled, because the signature inference breaks when using this operator
TEST_F(SemanticQueryValidationTest, DISABLED_validAsOperatorTest) {
    NES_INFO("Valid as operator test");

    std::string queryString = R"(Query::from("default_logical").as("dl").filter(Attribute("value") > 100); )";
    CallValidation(queryString);
}

// Test a query with a, invalid "as" operator
TEST_F(SemanticQueryValidationTest, invalidAsOperatorTest) {
    NES_INFO("Invalid as operator test");

    std::string queryString = R"(Query::from("default_logical").as("value").filter(Attribute("value") > 100); )";
    CallValidation(queryString);
}

// Test a query with a valid "project" operator
TEST_F(SemanticQueryValidationTest, validProjectionTest) {
    NES_INFO("Valid projection test");

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "default_logical";
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, "phy1");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4, properties);
    auto sourceCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNode1);
    sourceCatalog->addPhysicalSource(logicalSourceName, sourceCatalogEntry);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);

    auto query = Query::from("default_logical")
                     .project(Attribute("id").as("new_id"), Attribute("value"))
                     .filter(Attribute("new_id") < 42)
                     .map(Attribute("value") = Attribute("value") + 2)
                     .sink(FileSinkDescriptor::create(""));

    semanticQueryValidation->validate(std::make_shared<Query>(query)->getQueryPlan());
}

// Test a query with an invalid "project" operator
TEST_F(SemanticQueryValidationTest, invalidProjectionTest) {
    NES_INFO("Invalid projection test");

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "default_logical";
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, "phy1");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4, properties);
    auto sourceCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNode1);
    sourceCatalog->addPhysicalSource(logicalSourceName, sourceCatalogEntry);

    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);

    auto query = Query::from("default_logical")
                     .map(Attribute("value") = Attribute("value") + 2)
                     .project(Attribute("id").as("new_id"), Attribute("value"))
                     .filter(Attribute("id") < 42)
                     .sink(FileSinkDescriptor::create(""));

    EXPECT_THROW(semanticQueryValidation->validate(std::make_shared<Query>(query)->getQueryPlan()), InvalidQueryException);
}

#ifdef TFDEF
/**
 * Test ML inference operator input with valid input
 */
TEST_F(SemanticQueryValidationTest, validMLInferenceOperatorTest) {
    NES_INFO("Valid ML inference operator test");

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "irisData";
    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createFloat())
                          ->addField("f2", DataTypeFactory::createUInt32())
                          ->addField("f3", DataTypeFactory::createInt8())
                          ->addField("f4", DataTypeFactory::createInt64())
                          ->addField("target", DataTypeFactory::createUInt64());
    sourceCatalog->addLogicalSource(logicalSourceName, irisSchema);
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, "phy1");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4, properties);
    auto sourceCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNode1);
    sourceCatalog->addPhysicalSource(logicalSourceName, sourceCatalogEntry);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);

    auto query = Query::from("irisData")
                     .inferModel(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite",
                                 {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .sink(FileSinkDescriptor::create(""));

    semanticQueryValidation->validate(std::make_shared<Query>(query)->getQueryPlan());
}
#endif
/**
 * Test ML inference operator input with invalid mixed input
 */
TEST_F(SemanticQueryValidationTest, invalidMixedInputMLInferenceOperatorTest) {
    NES_INFO("Invalid projection test");

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "irisData";
    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createFloat())
                          ->addField("f2", DataTypeFactory::createBoolean())
                          ->addField("f3", DataTypeFactory::createInt8())
                          ->addField("f4", DataTypeFactory::createInt64())
                          ->addField("target", DataTypeFactory::createUInt64());
    sourceCatalog->addLogicalSource(logicalSourceName, irisSchema);
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, "phy1");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4, properties);
    auto sourceCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNode1);
    sourceCatalog->addPhysicalSource(logicalSourceName, sourceCatalogEntry);

    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);

    auto query = Query::from("irisData")
                     .inferModel(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite",
                                 {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .sink(FileSinkDescriptor::create(""));

    EXPECT_THROW(semanticQueryValidation->validate(std::make_shared<Query>(query)->getQueryPlan()), InvalidQueryException);
}

#ifdef TFDEF
/**
 * Test ML inference operator input with invalid input
 */
TEST_F(SemanticQueryValidationTest, invalidInputMLInferenceOperatorTest) {
    NES_INFO("Invalid projection test");

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    std::string logicalSourceName = "irisData";
    auto irisSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("f1", DataTypeFactory::createChar())
                          ->addField("f2", DataTypeFactory::createUInt32())
                          ->addField("f3", DataTypeFactory::createInt8())
                          ->addField("f4", DataTypeFactory::createInt64())
                          ->addField("target", DataTypeFactory::createUInt64());
    sourceCatalog->addLogicalSource(logicalSourceName, irisSchema);
    auto logicalSource = sourceCatalog->getLogicalSource(logicalSourceName);
    auto physicalSource = PhysicalSource::create(logicalSourceName, "phy1");
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4, properties);
    auto sourceCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNode1);
    sourceCatalog->addPhysicalSource(logicalSourceName, sourceCatalogEntry);

    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);

    auto query = Query::from("irisData")
                     .inferModel(std::filesystem::path(TEST_DATA_DIRECTORY) / "iris_95acc.tflite",
                                 {Attribute("f1"), Attribute("f2"), Attribute("f3"), Attribute("f4")},
                                 {Attribute("iris0", BasicType::FLOAT32),
                                  Attribute("iris1", BasicType::FLOAT32),
                                  Attribute("iris2", BasicType::FLOAT32)})
                     .sink(FileSinkDescriptor::create(""));

    EXPECT_THROW(semanticQueryValidation->validate(std::make_shared<Query>(query)->getQueryPlan()), InvalidQueryException);
}
#endif
// Test a query with an invalid logical source having to physical source defined
TEST_F(SemanticQueryValidationTest, missingPhysicalSourceTest) {
    NES_INFO("Invalid projection test");

    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    auto semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog, true, udfCatalog);

    auto subQuery = Query::from("default_logical");
    auto query = Query::from("default_logical")
                     .map(Attribute("value") = Attribute("value") + 2)
                     .project(Attribute("id").as("new_id"), Attribute("value"))
                     .sink(FileSinkDescriptor::create(""));
    EXPECT_THROW(semanticQueryValidation->validate(std::make_shared<Query>(query)->getQueryPlan()), MapEntryNotFoundException);
}

}// namespace NES