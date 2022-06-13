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
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/OriginIdInferenceRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

class OriginIdInferenceRuleTest : public testing::Test {

  public:
    Optimizer::OriginIdInferenceRulePtr originIdInferenceRule;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::TopologySpecificQueryRewritePhasePtr topologySpecificQueryRewritePhase;

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("OriginIdInferenceRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup OriginIdInferenceRuleTest test case.");
        originIdInferenceRule = Optimizer::OriginIdInferenceRule::create();
        SourceCatalogPtr sourceCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
        setupTopologyNodeAndSourceCatalog(sourceCatalog);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);
        topologySpecificQueryRewritePhase =
            Optimizer::TopologySpecificQueryRewritePhase::create(sourceCatalog, OptimizerConfiguration());
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup OriginIdInferenceRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down OriginIdInferenceRuleTest test class."); }

    void setupTopologyNodeAndSourceCatalog(const SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup FilterPushDownTest test case.");
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

        auto schemaA = Schema::create()->addField("id", INT32)->addField("value", UINT32);
        sourceCatalog->addLogicalSource("A", schemaA);
        LogicalSourcePtr logicalSourceA = sourceCatalog->getLogicalSource("A");

        PhysicalSourcePtr physicalSourceA1 = PhysicalSource::create("A", "A1", DefaultSourceType::create());
        SourceCatalogEntryPtr sceA1 = std::make_shared<SourceCatalogEntry>(physicalSourceA1, logicalSourceA, physicalNode);
        sourceCatalog->addPhysicalSource("A", sceA1);

        PhysicalSourcePtr physicalSourceA2 = PhysicalSource::create("A", "A2", DefaultSourceType::create());
        SourceCatalogEntryPtr sceA2 = std::make_shared<SourceCatalogEntry>(physicalSourceA2, logicalSourceA, physicalNode);
        sourceCatalog->addPhysicalSource("A", sceA2);

        auto schemaB = Schema::create()->addField("id", INT32)->addField("value", UINT32);
        sourceCatalog->addLogicalSource("B", schemaB);
        LogicalSourcePtr logicalSourceB = sourceCatalog->getLogicalSourceOrThrowException("B");

        PhysicalSourcePtr physicalSourceB1 = PhysicalSource::create("B", "B1", DefaultSourceType::create());
        SourceCatalogEntryPtr sceB1 = std::make_shared<SourceCatalogEntry>(physicalSourceB1, logicalSourceB, physicalNode);
        sourceCatalog->addPhysicalSource("B", sceB1);
    }
};

/**
 * @brief Tests inference on a query plan with a single source.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForSinglePhysicalSource) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("B"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->apply(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<SourceLogicalOperatorNode>();
    ASSERT_EQ(sourceOperators.size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
    ASSERT_EQ(sinkOperators.size(), 1);
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds().size(), 1);

    // input origin id of the sink should be the same as the one from the sink.
    ASSERT_EQ(sinkOperators[0]->getInputOriginIds()[0], sourceOperators[0]->getOriginId());
}

/**
 * @brief Tests inference on a query plan with a single source.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForMultiplePhysicalSources) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->apply(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<SourceLogicalOperatorNode>();
    ASSERT_EQ(sourceOperators.size(), 2);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
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
TEST_F(OriginIdInferenceRuleTest, testRuleForMultipleSources) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source1);
    auto source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source2);
    auto source3 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source3);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = originIdInferenceRule->apply(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<SourceLogicalOperatorNode>();
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
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
TEST_F(OriginIdInferenceRuleTest, testRuleForMultipleSourcesAndIntermediateUnaryOperators) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source1);
    auto source2 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source2);
    auto source3 =
        LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("A"))->as<SourceLogicalOperatorNode>();
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

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = originIdInferenceRule->apply(updatedQueryPlan);

    // the source should always expose its own origin id as an output
    auto sourceOperators = updatedQueryPlan->getOperatorByType<SourceLogicalOperatorNode>();
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds().size(), 1);
    ASSERT_EQ(sourceOperators[0]->getOutputOriginIds()[0], sourceOperators[0]->getOriginId());

    // the sink should always have one input origin id.
    auto sinkOperators = updatedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
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
TEST_F(OriginIdInferenceRuleTest, testRuleForMultipleSourcesAndWindow) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source1);
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source2);
    auto source3 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source3);
    auto dummyWindowDefinition = LogicalWindowDefinition::create({},
                                                                 WindowTypePtr(),
                                                                 DistributionCharacteristicPtr(),
                                                                 WindowTriggerPolicyPtr(),
                                                                 WindowActionDescriptorPtr(),
                                                                 0);
    auto window = LogicalOperatorFactory::createCentralWindowSpecializedOperator(dummyWindowDefinition)->as<WindowOperatorNode>();
    queryPlan->appendOperatorAsNewRoot(window);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedPlan = originIdInferenceRule->apply(queryPlan);

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

TEST_F(OriginIdInferenceRuleTest, testRuleForUnionOperators) {

    auto query = Query::from("A").unionWith(Query::from("B")).sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->apply(updatedQueryPlan);

    auto sourceOps = updatedQueryPlan->getOperatorByType<SourceLogicalOperatorNode>();
    auto unionOps = updatedQueryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    ASSERT_EQ(unionOps[0]->getOutputOriginIds().size(), 3);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[0], sourceOps[0]->getOutputOriginIds()[0]);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[1], sourceOps[1]->getOutputOriginIds()[0]);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[2], sourceOps[2]->getOutputOriginIds()[0]);

    auto sinkOps = updatedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
    ASSERT_EQ(sinkOps[0]->getOutputOriginIds()[0], unionOps[0]->getOutputOriginIds()[0]);
}

TEST_F(OriginIdInferenceRuleTest, testRuleForSelfUnionOperators) {

    auto query = Query::from("A").unionWith(Query::from("A")).sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedQueryPlan = typeInferencePhase->execute(queryPlan);
    updatedQueryPlan = topologySpecificQueryRewritePhase->execute(updatedQueryPlan);
    updatedQueryPlan = originIdInferenceRule->apply(updatedQueryPlan);

    auto sourceOps = updatedQueryPlan->getOperatorByType<SourceLogicalOperatorNode>();
    auto unionOps = updatedQueryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    ASSERT_EQ(unionOps[0]->getOutputOriginIds().size(), 3);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[0], sourceOps[0]->getOutputOriginIds()[0]);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[1], sourceOps[1]->getOutputOriginIds()[0]);
    ASSERT_EQ(unionOps[0]->getOutputOriginIds()[2], sourceOps[2]->getOutputOriginIds()[0]);

    auto sinkOps = updatedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
    ASSERT_EQ(sinkOps[0]->getOutputOriginIds()[0], unionOps[0]->getOutputOriginIds()[0]);
}

/**
 * @brief Tests inference on a query plan with multiple sources and a central window operator.
 * Thus the root operator should contain the origin id from the window operator and the window operator from all sources.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForJoinAggregationAndUnionOperators) {

    auto query = Query::from("A")
                     .unionWith(Query::from("B"))
                     .map(Attribute("x") = Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("id")), Seconds(3)))
                     .byKey(Attribute("value"))
                     .apply(Sum(Attribute("x")))
                     .joinWith(Query::from("A")
                                   .map(Attribute("x") = Attribute("id"))
                                   .window(TumblingWindow::of(EventTime(Attribute("id")), Seconds(3)))
                                   .byKey(Attribute("value"))
                                   .apply(Sum(Attribute("x"))))
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("x")), Seconds(3)))
                     .sink(NullOutputSinkDescriptor::create());

    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto updatedPlan = originIdInferenceRule->apply(queryPlan);

    auto unionOps = updatedPlan->getOperatorByType<UnionLogicalOperatorNode>();

    ASSERT_EQ(unionOps[0]->getOutputOriginIds().size(), 2);

    auto joinOps = updatedPlan->getOperatorByType<JoinLogicalOperatorNode>();

    ASSERT_EQ(joinOps[0]->getOutputOriginIds().size(), 2);

    // the source should always expose its own origin id as an output
    //    ASSERT_EQ(source1->getOutputOriginIds().size(), 1);
    //    ASSERT_EQ(source1->getOutputOriginIds()[0], source1->getOriginId());
    //
    //    // the window should always have one input origin id.
    //    ASSERT_EQ(window->getInputOriginIds().size(), 3);
    //
    //    // input origin ids of the window should contain all origin ids from the sources.
    //    ASSERT_EQ(window->getInputOriginIds()[0], source1->getOriginId());
    //    ASSERT_EQ(window->getInputOriginIds()[1], source2->getOriginId());
    //    ASSERT_EQ(window->getInputOriginIds()[2], source3->getOriginId());
    //
    //    // the sink should always have one input origin id.
    //    ASSERT_EQ(sink->getInputOriginIds().size(), 1);
    //
    //    // input origin ids of the sink should be the same as the window operator origin id
    //    ASSERT_EQ(sink->getInputOriginIds()[0], window->getOriginId());
}
