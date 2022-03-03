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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Optimizer/QueryRewrite/OriginIdInferenceRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

class OriginIdInferenceRuleTest : public testing::Test {

  public:
    Optimizer::OriginIdInferenceRulePtr originIdIferenceRule;

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("OriginIdInferenceRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup OriginIdInferenceRuleTest test case.");
        originIdIferenceRule = Optimizer::OriginIdInferenceRule::create();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup OriginIdInferenceRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down OriginIdInferenceRuleTest test class."); }
};

/**
 * @brief Tests inference on a query plan with a single source.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForSingleSource) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                      ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedPlan = originIdIferenceRule->apply(queryPlan);

    // the source should always expose its own origin id as an output
    ASSERT_EQ(source->getOutputOriginIds().size(), 1);
    ASSERT_EQ(source->getOutputOriginIds()[0], source->getOriginId());

    // the sink should always have one input origin id.
    ASSERT_EQ(sink->getInputOriginIds().size(), 1);

    // input origin id of the sink should be the same as the one from the sink.
    ASSERT_EQ(sink->getInputOriginIds()[0], source->getOriginId());
}

/**
 * @brief Tests inference on a query plan with multiple sources.
 * Thus the root operator should contain the origin ids from all sources.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForMultipleSources) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source1);
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source2);
    auto source3 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source3);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedPlan = originIdIferenceRule->apply(queryPlan);

    // the source should always expose its own origin id as an output
    ASSERT_EQ(source1->getOutputOriginIds().size(), 1);
    ASSERT_EQ(source1->getOutputOriginIds()[0], source1->getOriginId());

    // the sink should always have one input origin id.
    ASSERT_EQ(sink->getInputOriginIds().size(), 3);

    // input origin ids of the sink should contain all origin ids from the sources.
    ASSERT_EQ(sink->getInputOriginIds()[0], source1->getOriginId());
    ASSERT_EQ(sink->getInputOriginIds()[1], source2->getOriginId());
    ASSERT_EQ(sink->getInputOriginIds()[2], source3->getOriginId());
}

/**
 * @brief Tests inference on a query plan with multiple sources and intermediate unary operators.
 * Thus the all intermediate operators should contain the origin ids from all sources.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForMultipleSourcesAndIntermediateUnaryOperators) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source1);
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source2);
    auto source3 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source3);
    auto filter = LogicalOperatorFactory::createFilterOperator(Attribute("x") == Attribute("y"));
    queryPlan->appendOperatorAsNewRoot(filter);
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("x") = Attribute("y"));
    queryPlan->appendOperatorAsNewRoot(map);
    auto project = LogicalOperatorFactory::createProjectionOperator(
        {Attribute("x1").getExpressionNode(), Attribute("x2").getExpressionNode()});
    queryPlan->appendOperatorAsNewRoot(project);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedPlan = originIdIferenceRule->apply(queryPlan);

    // the source should always expose its own origin id as an output
    ASSERT_EQ(source1->getOutputOriginIds().size(), 1);
    ASSERT_EQ(source1->getOutputOriginIds()[0], source1->getOriginId());

    // the sink should always have one input origin id.
    ASSERT_EQ(sink->getInputOriginIds().size(), 3);

    // input origin ids of the sink should contain all origin ids from the sources.
    ASSERT_EQ(sink->getInputOriginIds()[0], source1->getOriginId());
    ASSERT_EQ(sink->getInputOriginIds()[1], source2->getOriginId());
    ASSERT_EQ(sink->getInputOriginIds()[2], source3->getOriginId());
}

/**
 * @brief Tests inference on a query plan with multiple sources and a central window operator.
 * Thus the root operator should contain the origin id from the window operator and the window operator from all sources.
 */
TEST_F(OriginIdInferenceRuleTest, testRuleForMultipleSourcesAndWindow) {
    const QueryPlanPtr queryPlan = QueryPlan::create();
    auto source1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source1);
    auto source2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source2);
    auto source3 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"))
                       ->as<SourceLogicalOperatorNode>();
    queryPlan->addRootOperator(source3);
    auto dummyWindowDefinition = LogicalWindowDefinition::create({},
                                    WindowTypePtr(),
                                    DistributionCharacteristicPtr(),
                                    0,
                                    WindowTriggerPolicyPtr(),
                                    WindowActionDescriptorPtr(),
                                    0);
    auto window = LogicalOperatorFactory::createCentralWindowSpecializedOperator(dummyWindowDefinition)->as<WindowOperatorNode>();
    queryPlan->appendOperatorAsNewRoot(window);
    auto sink = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    queryPlan->appendOperatorAsNewRoot(sink);

    std::cout << " plan before=" << queryPlan->toString() << std::endl;

    auto updatedPlan = originIdIferenceRule->apply(queryPlan);

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