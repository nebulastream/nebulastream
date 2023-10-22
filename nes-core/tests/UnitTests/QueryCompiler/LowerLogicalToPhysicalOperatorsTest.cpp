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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/DumpHandler/DumpContext.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/PythonUDFDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Actions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Actions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/TriggerPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Operators/LogicalOperators/Windows/TriggerPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PythonUDFDescriptorBuilder.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalBatchJoinDefinition.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

class LowerLogicalToPhysicalOperatorsTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TranslateToPhysicalOperatorPhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TranslateToPhysicalOperatorPhaseTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));
        unionOp1 = LogicalOperatorFactory::createUnionOperator();
        sourceOp1 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        sourceOp2 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        sourceOp3 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        sourceOp4 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4 = LogicalOperatorFactory::createFilterOperator(pred4);
        filterOp5 = LogicalOperatorFactory::createFilterOperator(pred5);
        filterOp6 = LogicalOperatorFactory::createFilterOperator(pred6);
        filterOp7 = LogicalOperatorFactory::createFilterOperator(pred7);
        projectPp = LogicalOperatorFactory::createProjectionOperator({});
        {
            Windowing::WindowTriggerPolicyPtr triggerPolicy = Windowing::OnTimeTriggerPolicyDescription::create(1000);
            auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
            auto distrType = Windowing::DistributionCharacteristic::createCompleteWindowType();
            auto joinType = Join::LogicalJoinDefinition::JoinType::INNER_JOIN;
            Join::LogicalJoinDefinitionPtr joinDef = Join::LogicalJoinDefinition::create(
                FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
                FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
                Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), API::Milliseconds(10)),
                distrType,
                triggerPolicy,
                triggerAction,
                1,
                1,
                joinType);

            joinOp1 = LogicalOperatorFactory::createJoinOperator(joinDef)->as<JoinLogicalOperatorNode>();
        }
        sinkOp1 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        sinkOp2 = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
        auto windowType = TumblingWindow::of(EventTime(Attribute("test")), Seconds(10));
        auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
        auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
        auto windowDefinition = LogicalWindowDefinition::create({Sum(Attribute("test"))->aggregation},
                                                                windowType,
                                                                Windowing::DistributionCharacteristic::createCompleteWindowType(),
                                                                triggerPolicy,
                                                                triggerAction,
                                                                0);

        watermarkAssigner1 = LogicalOperatorFactory::createWatermarkAssignerOperator(
            Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        centralWindowOperator = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDefinition);
        centralWindowOperator->as<WindowOperatorNode>()->setInputOriginIds({0});
        sliceCreationOperator = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDefinition);
        sliceCreationOperator->as<WindowOperatorNode>()->setInputOriginIds({0});
        windowComputation = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDefinition);
        windowComputation->as<WindowOperatorNode>()->setInputOriginIds({0});
        sliceMerging = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDefinition);
        sliceMerging->as<WindowOperatorNode>()->setInputOriginIds({0});
        mapOp = LogicalOperatorFactory::createMapOperator(Attribute("id") = 10);
        auto javaUDFDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
        mapJavaUDFOp = LogicalOperatorFactory::createMapUDFLogicalOperator(javaUDFDescriptor);
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
        auto pythonUDFDescriptor = Catalogs::UDF::PythonUDFDescriptorBuilder::createDefaultPythonUDFDescriptor();
        mapPythonUDFOp = LogicalOperatorFactory::createMapUDFLogicalOperator(pythonUDFDescriptor);
#endif// NAUTILUS_PYTHON_UDF_ENABLED
    }

  protected:
    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorNodePtr sourceOp1, sourceOp2, sourceOp3, sourceOp4, unionOp1;
    LogicalOperatorNodePtr watermarkAssigner1, centralWindowOperator, sliceCreationOperator, windowComputation, sliceMerging;
    LogicalOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    LogicalOperatorNodePtr sinkOp1, sinkOp2;
    LogicalOperatorNodePtr mapOp, mapJavaUDFOp;
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
    LogicalOperatorNodePtr mapPythonUDFOp;
#endif// NAUTILUS_PYTHON_UDF_ENABLED
    LogicalOperatorNodePtr projectPp;
    JoinLogicalOperatorNodePtr joinOp1;
    QueryCompilation::QueryCompilerOptionsPtr options;
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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateFilterQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateDemultiplexBroadcastQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    auto broadcastOperator = LogicalOperatorFactory::createBroadcastOperator();
    queryPlan->appendOperatorAsNewRoot(broadcastOperator);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sinkOp2->addChild(broadcastOperator);
    queryPlan->addRootOperator(sinkOp2);

    NES_DEBUG("{}", queryPlan->toString());

    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateDemultiplexFilterQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sinkOp2->addChild(filterOp1);
    queryPlan->addRootOperator(sinkOp2);

    NES_DEBUG("{}", queryPlan->toString());

    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateFilterMultiplexQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(unionOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    unionOp1->addChild(sourceOp2);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateFilterImplicitMultiplexQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(filterOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    filterOp1->addChild(sourceOp2);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, DISABLED_translateSimpleJoinQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);

    auto leftSchema = Schema::create()->addField("left$f1", DataTypeFactory::createInt64());
    auto rightSchema = Schema::create()->addField("right$f2", DataTypeFactory::createInt64());
    joinOp1->setLeftInputSchema(leftSchema);
    joinOp1->setRightInputSchema(rightSchema);
    joinOp1->setOriginId(1);
    sourceOp1->setOutputSchema(leftSchema);
    queryPlan->appendOperatorAsNewRoot(joinOp1);

    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    joinOp1->addChild(sourceOp2);
    sourceOp2->setOutputSchema(rightSchema);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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

TEST_F(LowerLogicalToPhysicalOperatorsTest, DISABLED_translateSimpleBatchJoinQuery) {
    Experimental::BatchJoinLogicalOperatorNodePtr batchJoinOp1;
    {
        Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDef = Join::Experimental::LogicalBatchJoinDefinition::create(
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
            1,
            1);

        batchJoinOp1 =
            LogicalOperatorFactory::createBatchJoinOperator(batchJoinDef)->as<Experimental::BatchJoinLogicalOperatorNode>();
    }

    auto queryPlan = QueryPlan::create(sourceOp1);

    auto leftSchema = Schema::create()->addField("left$f1", DataTypeFactory::createInt64());
    auto rightSchema = Schema::create()->addField("right$f2", DataTypeFactory::createInt64());
    batchJoinOp1->setLeftInputSchema(leftSchema);
    batchJoinOp1->setRightInputSchema(rightSchema);
    sourceOp1->setOutputSchema(leftSchema);
    queryPlan->appendOperatorAsNewRoot(batchJoinOp1);

    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    batchJoinOp1->addChild(sourceOp2);
    sourceOp2->setOutputSchema(rightSchema);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
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
 *
 * Result Query plan:
 *
 * --- Physical Sink 1  --- Physical Join Sink --- Physical Join Build --- Physical Source 1
 *                                             \
 *                                             --- Physical Join Build --- Physical Multiplex Operator  --- Physical Source 1
 *                                                                                                      \
 *                                                                                                       --- Physical Source 2
 */
TEST_F(LowerLogicalToPhysicalOperatorsTest, DISABLED_translateJoinQueryWithMultiplex) {
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

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, DISABLED_translateJoinQueryWithMultiplex4Edges) {
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

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, DISABLED_translateWindowQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(watermarkAssigner1);
    queryPlan->appendOperatorAsNewRoot(centralWindowOperator);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateSliceCreationQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(watermarkAssigner1);
    queryPlan->appendOperatorAsNewRoot(sliceCreationOperator);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
 * --- Sink 1 --- Slice Merging -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Slice Merging Operator --- Physical Slice Sink --- Physical Source 1
 *
 */
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateSliceMergingQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(sliceMerging);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalWindowSinkOperator>());
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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateMapQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(mapOp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
 * --- Sink 1 --- Map Java Udf -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Map Java Udf Operator --- Physical Source 1
 *
 */
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateMapJavaUDFQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(mapJavaUDFOp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMapUDFOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

#ifdef NAUTILUS_PYTHON_UDF_ENABLED
/**
 * @brief Input Query Plan:
 *
 * --- Sink 1 --- Map Python Udf -- Source 1
 *
 * Result Query plan:
 *
 * --- Physical Sink 1 --- Physical Map Python Udf Operator --- Physical Source 1
 *
 */
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateMapPythonUDFQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(mapPythonUDFOp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalMapUDFOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}
#endif// NAUTILUS_PYTHON_UDF_ENABLED

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateProjectQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(projectPp);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateTwoSourceQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);
    sinkOp1->addChild(sourceOp2);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

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
TEST_F(LowerLogicalToPhysicalOperatorsTest, translateSinkSourceQuery) {
    auto queryPlan = QueryPlan::create(sourceOp1);
    queryPlan->appendOperatorAsNewRoot(sinkOp1);

    NES_DEBUG("{}", queryPlan->toString());
    auto physicalOperatorProvider = QueryCompilation::DefaultPhysicalOperatorProvider::create(options);
    auto phase = QueryCompilation::LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);

    phase->apply(queryPlan);
    NES_DEBUG("{}", queryPlan->toString());

    auto iterator = QueryPlanIterator(queryPlan).begin();

    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>());
    ++iterator;
    ASSERT_TRUE((*iterator)->instanceOf<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>());
}

}// namespace NES
