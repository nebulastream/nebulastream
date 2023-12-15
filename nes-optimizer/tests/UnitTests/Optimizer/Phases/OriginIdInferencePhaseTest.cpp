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
// clang-format on
#include "Operators/LogicalOperators/Operators/LogicalUnionOperator.hpp"
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <Operators/LogicalOperators/Windows/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

using namespace NES;
using namespace Configurations;

class OriginIdInferencePhaseTest : public Testing::BaseUnitTest {

  public:
    Optimizer::OriginIdInferencePhasePtr originIdInferenceRule;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::TopologySpecificQueryRewritePhasePtr topologySpecificQueryRewritePhase;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("OriginIdInferencePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup OriginIdInferencePhaseTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup OriginIdInferencePhaseTest test case.");
        originIdInferenceRule = Optimizer::OriginIdInferencePhase::create();
        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        setupTopologyNodeAndSourceCatalog(sourceCatalog);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, Catalogs::UDF::UDFCatalog::create());
        auto optimizerConfiguration = OptimizerConfiguration();
        optimizerConfiguration.performDistributedWindowOptimization = false;
        topologySpecificQueryRewritePhase =
            Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(), sourceCatalog, optimizerConfiguration);
    }

    void setupTopologyNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup FilterPushDownTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);

        auto schemaA = Schema::create()->addField("id", BasicType::INT32)->addField("value", BasicType::UINT32);
        sourceCatalog->addLogicalSource("A", schemaA);
        LogicalSourcePtr logicalSourceA = sourceCatalog->getLogicalSource("A");

        PhysicalSourcePtr physicalSourceA1 = PhysicalSource::create(DefaultSourceType::create("A", "A1"));
        Catalogs::Source::SourceCatalogEntryPtr sceA1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceA1, logicalSourceA, physicalNode);
        sourceCatalog->addPhysicalSource("A", sceA1);

        PhysicalSourcePtr physicalSourceA2 = PhysicalSource::create(DefaultSourceType::create("A", "A2"));
        Catalogs::Source::SourceCatalogEntryPtr sceA2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceA2, logicalSourceA, physicalNode);
        sourceCatalog->addPhysicalSource("A", sceA2);

        auto schemaB = Schema::create()->addField("id", BasicType::INT32)->addField("value", BasicType::UINT32);
        sourceCatalog->addLogicalSource("B", schemaB);
        LogicalSourcePtr logicalSourceB = sourceCatalog->getLogicalSource("B");

        PhysicalSourcePtr physicalSourceB1 = PhysicalSource::create(DefaultSourceType::create("B", "B1"));
        Catalogs::Source::SourceCatalogEntryPtr sceB1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceB1, logicalSourceB, physicalNode);
        sourceCatalog->addPhysicalSource("B", sceB1);
    }
};

/**
 * @brief Tests inference on a query plan with a single source.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForSinglePhysicalSource) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("B"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = typeInferencePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    ASSERT_EQ(sourceOperators.size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOperators.size(), 1);
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds().size(), 1);

    // input origin id of the sink should be the same as the one from the sink.
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[0], sourceOperators[0]->getOriginId());
}

/**
 * @brief Tests inference on a query plan with a single source.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForMultiplePhysicalSources) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = typeInferencePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    ASSERT_EQ(sourceOperators.size(), 2);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOperators.size(), 1);
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds().size(), 2);

    // input origin id of the sink should be the same as the one from the sink.
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[0], sourceOperators[0]->getOriginId());
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[1], sourceOperators[1]->getOriginId());
}

/**
 * @brief Tests inference on a query plan with multiple sources.
 * Thus the root operator should contain the origin ids from all sources.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForMultipleSources) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source1);
    auto source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source2);
    auto source3 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source3);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    NES_DEBUG(" plan before={}", queryPlan->toString());

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds().size(), 3);

    // input origin ids of the sink should contain all origin ids from the sources.
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[0], sourceOperators[0]->getOriginId());
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[1], sourceOperators[1]->getOriginId());
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[2], sourceOperators[2]->getOriginId());
}

/**
 * @brief Tests inference on a query plan with multiple sources and intermediate unary operators.
 * Thus the all intermediate operators should contain the origin ids from all sources.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForMultipleSourcesAndIntermediateUnaryOperators) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source1);
    auto source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source2);
    auto source3 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source3);
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("id") == Attribute("id"));
    queryPlan->appendOperatorAsNewRoot(filter);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("x") = Attribute("id"));
    queryPlan->appendOperatorAsNewRoot(map);
    auto project = LogicalOperatorFactory::createProjectionOperator(
        {Attribute("x").getExpressionNode(), Attribute("id").getExpressionNode()});
    queryPlan->appendOperatorAsNewRoot(project);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    NES_DEBUG(" plan before={}", queryPlan->toString());

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds().size(), 3);

    // input origin ids of the sink should contain all origin ids from the sources.
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[0], sourceOperators[0]->getOriginId());
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[1], sourceOperators[1]->getOriginId());
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[2], sourceOperators[2]->getOriginId());
}

/**
 * @brief Tests inference on a query plan with multiple sources and a central window operator.
 * Thus the root operator should contain the origin id from the window operator and the window operator from all sources.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForMultipleSourcesAndWindow) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"))
                       ->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source1);
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"))
                       ->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source2);
    auto source3 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"))
                       ->as<LogicalSourceOperator>();
    queryPlan->addRootOperator(source3);
    auto dummyWindowDefinition = LogicalWindowDescriptor::create({},
                                                                 WindowTypePtr(),
                                                                 DistributionCharacteristicPtr(),
                                                                 0);
    auto window = LogicalOperatorFactory::createCentralWindowSpecializedOperator(dummyWindowDefinition)->as<WindowOperator>();
    queryPlan->appendOperatorAsNewRoot(window);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    NES_DEBUG(" plan before={}", queryPlan->toString());

    auto updatedPlan = originIdInferenceRule->execute(queryPlan);

    // the source should always expose its own origin id as an output
    ASSERT_EQ(source1->getOutputOriginIds().size(), 1);
    ASSERT_EQ(source1->getOutputOriginIds()[0], source1->getOriginId());

    // the window should always have one input origin id.
    ASSERT_EQ(window->getInputOriginIds().size(), 3);

    // input origin ids of the window should contain all origin ids from the sources.
    ASSERT_EQ(window->getInputOriginIds()[0], source1->getOriginId());
    ASSERT_EQ(window->getInputOriginIds()[1], source2->getOriginId());
    ASSERT_EQ(window->getInputOriginIds()[2], source3->getOriginId());

    // the sink should always have one input origin id.
    ASSERT_EQ(sink->getInputOriginIds().size(), 1);

    // input origin ids of the sink should be the same as the window operator origin id
    ASSERT_EQ(sink->getInputOriginIds()[0], window->getOriginId());
}

/**
 * @brief: This test infer origin id for a union operator fetching data from two distinct sources.
 * Therefore, the output origin ids for union operator should return 3 distinct ids. 2 for each physical source of A and 1 for physical source B.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForUnionOperators) {

    auto query = Query::from("A").unionWith(Query::from("B")).sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = typeInferencePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    auto sourceOps = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    auto unionOps = updatedQueryPlan->getOperatorByType<LogicalUnionOperator>();
    ASSERT_EQ(unionOps[0]->getOutputOriginIds().size(), 3);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[0], sourceOps[0]->getOutputOriginIds()[0]);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[1], sourceOps[1]->getOutputOriginIds()[0]);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[2], sourceOps[2]->getOutputOriginIds()[0]);

    auto sinkOps = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOps[0]->getOutputOriginIds()[0], unionOps[0]->getOutputOriginIds()[0]);
}

/**
 * @brief: This test infer origin id for a union operator fetching data from same sources.
 * Therefore, the output origin ids for union operator should return 4 distinct ids. 2 for each physical source of A on the right
 * side and 2 for physical source A on the left side.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForSelfUnionOperators) {

    auto query = Query::from("A").unionWith(Query::from("A")).sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = typeInferencePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    auto sourceOps = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    auto unionOps = updatedQueryPlan->getOperatorByType<LogicalUnionOperator>();
    const std::vector<OriginId>& unionOutputOriginIds = unionOps[0]->getOutputOriginIds();
    ASSERT_EQ(unionOutputOriginIds.size(), 4);

    auto found = std::find(unionOutputOriginIds.begin(), unionOutputOriginIds.end(), sourceOps[0]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != unionOutputOriginIds.end());
    found = std::find(unionOutputOriginIds.begin(), unionOutputOriginIds.end(), sourceOps[1]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != unionOutputOriginIds.end());
    found = std::find(unionOutputOriginIds.begin(), unionOutputOriginIds.end(), sourceOps[2]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != unionOutputOriginIds.end());
    found = std::find(unionOutputOriginIds.begin(), unionOutputOriginIds.end(), sourceOps[3]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != unionOutputOriginIds.end());

    auto sinkOps = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOps[0]->getOutputOriginIds()[0], unionOutputOriginIds[0]);
}

/**
 * @brief: This test infer origin id for a join operator joining data from same source.
 * Therefore, the output origin ids for join operator should return 4 distinct ids. 2 for each physical source of A on the left side
 * and 2 for each physical source of A with alias C on the right side.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForSelfJoinOperator) {

    auto query = Query::from("A")
                     .joinWith(Query::from("A").as("C"))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("id")), Seconds(3)))
                     .sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = typeInferencePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    auto sourceOps = updatedQueryPlan->getOperatorByType<LogicalSourceOperator>();
    auto joinOps = updatedQueryPlan->getOperatorByType<LogicalJoinOperator>();
    auto joinInputOriginIds = joinOps[0]->getLeftInputOriginIds();
    auto rightInputOriginIds = joinOps[0]->getRightInputOriginIds();
    joinInputOriginIds.insert(joinInputOriginIds.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());

    ASSERT_EQ(joinInputOriginIds.size(), 4);

    auto found = std::find(joinInputOriginIds.begin(), joinInputOriginIds.end(), sourceOps[0]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != joinInputOriginIds.end());
    found = std::find(joinInputOriginIds.begin(), joinInputOriginIds.end(), sourceOps[1]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != joinInputOriginIds.end());
    found = std::find(joinInputOriginIds.begin(), joinInputOriginIds.end(), sourceOps[2]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != joinInputOriginIds.end());
    found = std::find(joinInputOriginIds.begin(), joinInputOriginIds.end(), sourceOps[3]->getOutputOriginIds()[0]);
    ASSERT_TRUE(found != joinInputOriginIds.end());

    const std::vector<OriginId>& joinOutputOriginIds = joinOps[0]->getOutputOriginIds();
    ASSERT_EQ(joinOutputOriginIds.size(), 1);

    auto sinkOps = updatedQueryPlan->getOperatorByType<LogicalSinkOperator>();
    ASSERT_EQ(sinkOps[0]->getOutputOriginIds()[0], joinOutputOriginIds[0]);
}

/**
 * @brief: This test infer origin id for a query involving join, aggregation, and union operators.
 * Therefore, the output origin ids for union operator should return 3 distinct ids. 2 for each physical source of A on the left side
 * and 2 for each physical source of A with alias C on the right side.
 */
TEST_F(OriginIdInferencePhaseTest, testRuleForJoinAggregationAndUnionOperators) {

    auto query = Query::from("B")
                     .unionWith(Query::from("A"))
                     .map(Attribute("x") = Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("value")), Seconds(3)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("x")))
                     .joinWith(Query::from("A")
                                   .map(Attribute("x") = Attribute("id"))
                                   .window(TumblingWindow::of(EventTime(Attribute("value")), Seconds(3)))
                                   .byKey(Attribute("id"))
                                   .apply(Avg(Attribute("x"))))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("x")), Seconds(3)))
                     .sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = typeInferencePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->execute(updatedQueryPlan);

    // Assert on origin ids for union operator
    auto unionOps = updatedQueryPlan->getOperatorByType<LogicalUnionOperator>();
    ASSERT_EQ(unionOps[0]->getOutputOriginIds().size(), 3);

    // Assert on origin ids for union operator
    auto windowOps = updatedQueryPlan->getOperatorByType<WindowOperator>();
    ASSERT_EQ(windowOps.size(), 2);

    // Window aggregations
    auto aggregations = windowOps[0]->getWindowDefinition()->getWindowAggregation();
    ASSERT_EQ(aggregations.size(), 1);
    if (aggregations[0]->getType() == NES::Windowing::WindowAggregationDescriptor::Type::Sum) {
        ASSERT_EQ(windowOps[0]->getInputOriginIds().size(), 3);
    } else if (aggregations[0]->getType() == NES::Windowing::WindowAggregationDescriptor::Type::Avg) {
        ASSERT_EQ(windowOps[0]->getInputOriginIds().size(), 2);
    } else {
        FAIL();
    }
    ASSERT_EQ(windowOps[0]->getOutputOriginIds().size(), 1);

    // Window aggregations
    aggregations = windowOps[1]->getWindowDefinition()->getWindowAggregation();
    ASSERT_EQ(aggregations.size(), 1);
    if (aggregations[0]->getType() == NES::Windowing::WindowAggregationDescriptor::Type::Sum) {
        ASSERT_EQ(windowOps[1]->getInputOriginIds().size(), 3);
    } else if (aggregations[0]->getType() == NES::Windowing::WindowAggregationDescriptor::Type::Avg) {
        ASSERT_EQ(windowOps[1]->getInputOriginIds().size(), 2);
    } else {
        FAIL();
    }
    ASSERT_EQ(windowOps[1]->getOutputOriginIds().size(), 1);

    // Assert on origin ids for join operator
    auto joinOps = updatedQueryPlan->getOperatorByType<LogicalJoinOperator>();
    ASSERT_EQ(joinOps[0]->getLeftInputOriginIds().size(), 1);
    ASSERT_EQ(joinOps[0]->getRightInputOriginIds().size(), 1);
    ASSERT_EQ(joinOps[0]->getOutputOriginIds().size(), 1);
}
