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

// clang-format off
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

class LogicalSourceExpansionRuleTest : public Testing::BaseUnitTest {

  public:
    SchemaPtr schema;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LogicalSourceExpansionRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
        TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4, properties);

        auto csvSourceType = CSVSourceType::create("default_logical", "test_stream");
        PhysicalSourcePtr physicalSource = PhysicalSource::create(csvSourceType);
        LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
        auto sce1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode1->getId());
        auto sce2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode2->getId());
        sourceCatalog->addPhysicalSource("default_logical", sce1);
        sourceCatalog->addPhysicalSource("default_logical", sce2);
    }
};

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithJustSource) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    Query query = Query::from(logicalSourceName).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<WorkerId> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1u);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2u);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinksAndJustSource) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    const std::string logicalSourceName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(logicalSourceName));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<WorkerId> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinks) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    const std::string logicalSourceName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(logicalSourceName));

    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<WorkerId> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithFilterAndMap) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    Query query =
        Query::from(logicalSourceName).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<WorkerId> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithUnionOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    Query subQuery = Query::from(logicalSourceName).map(Attribute("value") = 50);

    Query query = Query::from(logicalSourceName)
                      .map(Attribute("value") = 40)
                      .unionWith(subQuery)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<WorkerId> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size() * 2);
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 1U);
    auto mergeOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(mergeOperators.size(), 1U);
    EXPECT_EQ(mergeOperators[0]->getChildren().size(), 4U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithFlatMapOperator) {
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    auto udfSchema = Schema::create()->addField("id", BasicType::INT32);
    auto javaUDFDescriptor =
        Catalogs::UDF::JavaUDFDescriptorBuilder{}
            .setClassName("stream.nebula.IntegerFlatMapFunction")
            .setMethodName("flatMap")
            .setInstance({})
            .setByteCodeList({{"stream.nebula.FlatMapFunction", {}}, {"stream.nebula.IntegerFlatMapFunction", {}}})
            .setInputSchema(udfSchema)
            .setOutputSchema(udfSchema)
            .setInputClassName("java.lang.Integer")
            .setOutputClassName("java.util.Collection")
            .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
            .build();

    auto query = Query::from(logicalSourceName).mapUDF(javaUDFDescriptor).flatMapUDF(javaUDFDescriptor).sink(printSinkDescriptor);
    auto queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    auto sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    auto rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 1U);
    auto flatMapOperators = queryPlan->getOperatorByType<FlatMapUDFLogicalOperatorNode>();
    EXPECT_EQ(flatMapOperators.size(), 1U);
    EXPECT_EQ(flatMapOperators[0]->getChildren().size(), 2U);

    //Validate that FlatMap is connected to two map logical operators
    for (const auto& childOperator : flatMapOperators[0]->getChildren()) {
        EXPECT_TRUE(childOperator->as_if<LogicalOperatorNode>()->instanceOf<MapUDFLogicalOperatorNode>());
    }
}
