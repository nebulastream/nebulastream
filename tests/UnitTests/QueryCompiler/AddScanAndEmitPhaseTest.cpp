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

#include "../../util/NESTest.hpp"
#include <API/Query.hpp>
#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/OperatorNode.hpp>
#include <Sources/DefaultSource.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCompleteWindowOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/NeverFusePolicy.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

using namespace std;

namespace NES {

using namespace NES::API;
using namespace NES::QueryCompilation::PhysicalOperators;

class AddScanAndEmitPhaseTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("AddScanAndEmitPhase.log", NES::LOG_DEBUG);
        NES_INFO("Setup AddScanAndEmitPhase test class.");
    }

    void SetUp() {}

    void TearDown() { NES_DEBUG("Tear down AddScanAndEmitPhase Test."); }
};

/**
 * @brief Input Query Plan:
 * Input:
 * | Physical Source |
 *
 * Expected Result:
 * | Physical Source |
 *
 */
TEST_F(AddScanAndEmitPhaseTest, scanOperator) {
    auto pipelineQueryPlan = QueryCompilation::PipelineQueryPlan::create();
    auto operatorPlan = QueryCompilation::OperatorPipeline::createSourcePipeline();

    auto source =
        QueryCompilation::PhysicalOperators::PhysicalSourceOperator::create(SchemaPtr(), SchemaPtr(), SourceDescriptorPtr());
    operatorPlan->prependOperator(source);
    pipelineQueryPlan->addPipeline(operatorPlan);

    auto phase = QueryCompilation::AddScanAndEmitPhase::create();
    pipelineQueryPlan = phase->apply(pipelineQueryPlan);

    auto pipelineRootOperator = pipelineQueryPlan->getSourcePipelines()[0]->getQueryPlan()->getRootOperators()[0];

    ASSERT_INSTANCE_OF(pipelineRootOperator, PhysicalSourceOperator);
    ASSERT_EQ(pipelineRootOperator->getChildren().size(), 0);
}

/**
 * @brief Input Query Plan:
 * Input:
 * | Physical Sink |
 *
 * Expected Result:
 * | Physical Sink |
 *
 */
TEST_F(AddScanAndEmitPhaseTest, sinkOperator) {
    auto operatorPlan = QueryCompilation::OperatorPipeline::create();
    auto sink = QueryCompilation::PhysicalOperators::PhysicalSinkOperator::create(SchemaPtr(), SchemaPtr(), SinkDescriptorPtr());
    operatorPlan->prependOperator(sink);

    auto phase = QueryCompilation::AddScanAndEmitPhase::create();
    operatorPlan = phase->process(operatorPlan);

    auto pipelineRootOperator = operatorPlan->getQueryPlan()->getRootOperators()[0];

    ASSERT_INSTANCE_OF(pipelineRootOperator, PhysicalSinkOperator);
    ASSERT_EQ(pipelineRootOperator->getChildren().size(), 0);
}

/**
 * @brief Input Query Plan:
 * Input:
 * | Physical Filter |
 *
 * Expected Result:
 * | Physical Scan --- Physical Filter --- Physical Emit|
 *
 */
TEST_F(AddScanAndEmitPhaseTest, pipelineFilterQuery) {

    auto operatorPlan = QueryCompilation::OperatorPipeline::create();
    operatorPlan->prependOperator(PhysicalFilterOperator::create(SchemaPtr(), SchemaPtr(), ExpressionNodePtr()));

    auto phase = QueryCompilation::AddScanAndEmitPhase::create();
    operatorPlan = phase->process(operatorPlan);

    auto pipelineRootOperator = operatorPlan->getQueryPlan()->getRootOperators()[0];

    ASSERT_INSTANCE_OF(pipelineRootOperator, PhysicalScanOperator);
    auto filter = pipelineRootOperator->getChildren()[0];
    ASSERT_INSTANCE_OF(filter, PhysicalFilterOperator);
    auto emit = filter->getChildren()[0];
    ASSERT_INSTANCE_OF(emit, PhysicalEmitOperator);
}
}// namespace NES