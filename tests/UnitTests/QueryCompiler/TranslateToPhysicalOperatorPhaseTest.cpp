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
#include <API/QueryAPI.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <gtest/gtest.h>

#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <iostream>

using namespace std;

namespace NES {

class TranslateToPhysicalOperatorPhaseTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("TranslateToPhysicalOperatorPhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup TranslateToPhysicalOperatorPhaseTest test class.");
    }

    void SetUp() override {

        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));
        unionOp1 = LogicalOperatorFactory::createUnionOperator();
        sourceOp1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
        sourceOp2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
        sourceOp3 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
        sourceOp4 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("default_logical"));
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4 = LogicalOperatorFactory::createFilterOperator(pred4);
        filterOp5 = LogicalOperatorFactory::createFilterOperator(pred5);
        filterOp6 = LogicalOperatorFactory::createFilterOperator(pred6);
        filterOp7 = LogicalOperatorFactory::createFilterOperator(pred7);
        projectPp = LogicalOperatorFactory::createProjectionOperator({});
        iterationCEPOp = LogicalOperatorFactory::createCEPIterationOperator(2, 6);
        {
            Windowing::WindowTriggerPolicyPtr triggerPolicy = Windowing::OnTimeTriggerPolicyDescription::create(1000);
            auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
            auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();
            Join::LogicalJoinDefinitionPtr joinDef = Join::LogicalJoinDefinition::create(
                FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
                FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
                Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), API::Milliseconds(10)),
                distrType,
                triggerPolicy,
                triggerAction,
                1,
                1);

            joinOp1 = LogicalOperatorFactory::createJoinOperator(joinDef)->as<JoinLogicalOperatorNode>();
        }
        sinkOp1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        auto windowType = TumblingWindow::of(EventTime(Attribute("test")), Seconds(10));
        auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
        auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
        auto windowDefinition = LogicalWindowDefinition::create(Sum(Attribute("test")),
                                                                windowType,
                                                                Windowing::DistributionCharacteristic::createCompleteWindowType(),
                                                                1,
                                                                triggerPolicy,
                                                                triggerAction,
                                                                0);

        watermarkAssigner1 = LogicalOperatorFactory::createWatermarkAssignerOperator(
            Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        centralWindowOperator = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDefinition);
        sliceCreationOperator = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDefinition);
        windowComputation = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDefinition);
        sliceMerging = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDefinition);
        mapOp = LogicalOperatorFactory::createMapOperator(Attribute("id") = 10);
    }

    void TearDown() override { NES_DEBUG("Tear down TranslateToPhysicalOperatorPhase Test."); }

  protected:
    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorNodePtr sourceOp1, sourceOp2, sourceOp3, sourceOp4, unionOp1;
    LogicalOperatorNodePtr watermarkAssigner1, centralWindowOperator, sliceCreationOperator, windowComputation, sliceMerging;
    LogicalOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    LogicalOperatorNodePtr sinkOp1, sinkOp2;
    LogicalOperatorNodePtr mapOp;
    LogicalOperatorNodePtr projectPp;
    LogicalOperatorNodePtr iterationCEPOp;
    JoinLogicalOperatorNodePtr joinOp1;
};

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Filter -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Filter -- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateFilterQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalFilterOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Braodcast -- Source 1
 *            /
 *           /
 * --- Sink 2
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Demultiplex Operator -- Physical Source 1
 *                      /
 *                     /
 * --- Physical Sink 2
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateDemultiplexBroadcastQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    auto broadcastOperator = LogicalOperatorFactory::createBroadcastOperator();
    queryPlan->appendOperatorAsNewRoot(broadcastOperator);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sinkOp2->addChild(broadcastOperator);
    queryPlan->addRootOperator(sinkOp2);

    NES_DEBUG(queryPlan->toString());

    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalDemultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- BroadcastOperator -- Source 1
 *            /
 *           /
 * --- Sink 2
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Demultiplex Operator --- Physical Filter -- Physical Source 1
 *                      /
 *                     /
 * --- Physical Sink 2
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateDemultiplexFilterQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sinkOp2->addChild(filterOp1);
    queryPlan->addRootOperator(sinkOp2);

    NES_DEBUG(queryPlan->toString());

    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalDemultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalFilterOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Filter --- Union --- Source 1
 *                                 \
 *                                  -- Source 2
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Filter --- Physical Multiple Operator --- Physical Source 1
 *                                                                       \
 *                                                                        --- Physical Source 2
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateFilterMultiplexQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(unionOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    unionOp1->addChild(sourceOp2);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalFilterOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Filter   --- Source 1
 *                        \
 *                         --- Source 2
 *
 * Result Query plan:
 *
 * --- Physical Sink 1  --- Physical Filter --- Physical Multiplex Operator  --- Physical Source 1
 *                                                                           \
 *                                                                            --- Physical Source 2
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateFilterImplicitMultiplexQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(sourceOp2);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalFilterOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Join   --- Source 1
 *                        \
 *                         --- Source 2
 *
 * Result Query plan:
 *
 * --- Physical Sink 1  --- Physical Join Sink --- Physical Join Build --- Physical Source 1
 *                                             \
 *                                             --- Physical Join Build --- Physical Source 2
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateSimpleJoinQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);

    auto leftSchema = Schema::create()->addField("left$f1", DataTypeFactory::createInt64());
    auto rightSchema = Schema::create()->addField("right$f2", DataTypeFactory::createInt64());
    joinOp1->setLeftInputSchema(leftSchema);
    joinOp1->setRightInputSchema(rightSchema);
    sourceOp1->setOutputSchema(leftSchema);
    queryPlan->appendOperatorAsNewRoot(joinOp1);

    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    joinOp1->addChild(sourceOp2);
    sourceOp2->setOutputSchema(rightSchema);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinBuildOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinBuildOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Join   --- Source 1
 *                        \
 *                         --- Source 2
 *                         \
 *                          --- Source 3
 *
 * Result Query plan:
 *
 * --- Physical Sink 1  --- Physical Join Sink --- Physical Join Build --- Physical Source 1
 *                                             \
 *                                             --- Physical Join Build --- Physical Multiplex Operator  --- Physical Source 1
 *                                                                                                      \
 *                                                                                                       --- Physical Source 2
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateJoinQueryWithMultiplex) {
    auto queryPlan = QueryPlan::create(sourceOp1);

    auto leftSchema = Schema::create()->addField("left$f1", DataTypeFactory::createInt64());
    auto rightSchema = Schema::create()->addField("right$f2", DataTypeFactory::createInt64());
    joinOp1->setLeftInputSchema(leftSchema);
    joinOp1->setRightInputSchema(rightSchema);
    sourceOp1->setOutputSchema(leftSchema);
    queryPlan->appendOperatorAsNewRoot(joinOp1);

    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    joinOp1->addChild(sourceOp2);
    joinOp1->addChild(sourceOp3);
    sourceOp2->setOutputSchema(rightSchema);
    sourceOp3->setOutputSchema(rightSchema);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinBuildOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinBuildOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Join   --- Source 1
 *                        \
 *                         --- Source 2
 *                         \
 *                          --- Source 3
 *                          \
 *                           --- Source 4
 *
 * Result Query plan:
 *
 * --- Physical Sink 1  --- Physical Join Sink --- Physical Join Build --- Physical Multiplex Operator  --- Physical Source 1
 *                                           \                                                           \
 *                                            \                                                          --- Physical Source 2
 *                                             \
 *                                             --- Physical Join Build --- Physical Multiplex Operator  --- Physical Source 3
 *                                                                                                      \
 *                                                                                                       --- Physical Source 4
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateJoinQueryWithMultiplex4Edges) {
    auto queryPlan = QueryPlan::create(sourceOp1);

    auto leftSchema = Schema::create()->addField("left$f1", DataTypeFactory::createInt64());
    auto rightSchema = Schema::create()->addField("right$f2", DataTypeFactory::createInt64());
    joinOp1->setLeftInputSchema(leftSchema);
    joinOp1->setRightInputSchema(rightSchema);

    queryPlan->appendOperatorAsNewRoot(joinOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sourceOp1->setOutputSchema(leftSchema);
    sourceOp2->setOutputSchema(leftSchema);
    joinOp1->addChild(sourceOp2);
    joinOp1->addChild(sourceOp3);
    joinOp1->addChild(sourceOp4);
    sourceOp3->setOutputSchema(rightSchema);
    sourceOp4->setOutputSchema(rightSchema);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinBuildOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalJoinBuildOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- WatermarkAssigner --- WindowOperator -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Watermark Assigner -- Physical Window Pre Aggregation Operator --- Physical Window Sink --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateWindowQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(watermarkAssigner1);
    queryPlan->appendOperatorAsNewRoot(centralWindowOperator);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalWindowSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSlicePreAggregationOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalWatermarkAssignmentOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- SliceCreation -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Slice Pre Aggregation Operator --- Physical Slice Sink --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateSliceCreationQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(watermarkAssigner1);
    queryPlan->appendOperatorAsNewRoot(sliceCreationOperator);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSliceSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSlicePreAggregationOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalWatermarkAssignmentOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Slice Merging -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Slice Merging Operator --- Physical Slice Sink --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateSliceMergingQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(sliceMerging);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSliceSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSliceMergingOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Map -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Map Operator --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateMapQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(mapOp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMapOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Project -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Project Operator --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateProjectQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(projectPp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalProjectOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Source 1
 *            --- Source 2
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateTwoSourceQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sinkOp1->addChild(sourceOp2);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMultiplexOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateSinkSourceQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- CEPIteration -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical CEPIteration -- Physical Source 1
 *
 */
TEST_F(TranslateToPhysicalOperatorPhaseTest, translateCEPiteration) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(iterationCEPOp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG(queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create();
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG(queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalIterationCEPOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

}// namespace NES
