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
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

#include <Catalogs/LogicalStream.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sources/DefaultSource.hpp>
#include <iostream>
#include <memory>

#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCompleteWindowOperator.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

using namespace std;

namespace NES {

using namespace NES::API;

class TranslateToGeneratableOperatorPhaseTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("QueryDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }

    void SetUp() {
        dumpContext = DumpContext::create();
        dumpContext->registerDumpHandler(ConsoleDumpHandler::create());

        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        sPtr = streamCatalog->getStreamForLogicalStreamOrThrowException("default_logical");
        SchemaPtr schema = sPtr->getSchema();
        auto sourceDescriptor = DefaultSourceDescriptor::create(schema, /*number of buffers*/ 0, /*frequency*/ 0u, 1);

        pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
        pred2 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "2"));
        pred3 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "3"));
        pred4 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "4"));
        pred5 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "5"));
        pred6 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "6"));
        pred7 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "7"));

        sourceOp = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
        filterOp1 = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2 = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3 = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4 = LogicalOperatorFactory::createFilterOperator(pred4);
        filterOp5 = LogicalOperatorFactory::createFilterOperator(pred5);
        filterOp6 = LogicalOperatorFactory::createFilterOperator(pred6);
        filterOp7 = LogicalOperatorFactory::createFilterOperator(pred7);

        filterOp1Copy = LogicalOperatorFactory::createFilterOperator(pred1);
        filterOp2Copy = LogicalOperatorFactory::createFilterOperator(pred2);
        filterOp3Copy = LogicalOperatorFactory::createFilterOperator(pred3);
        filterOp4Copy = LogicalOperatorFactory::createFilterOperator(pred4);
        filterOp5Copy = LogicalOperatorFactory::createFilterOperator(pred5);
        filterOp6Copy = LogicalOperatorFactory::createFilterOperator(pred6);
        filterOp7Copy = LogicalOperatorFactory::createFilterOperator(pred7);

        removed = false;
        replaced = false;
        children.clear();
        parents.clear();
    }

    void TearDown() { NES_DEBUG("Tear down LogicalOperatorNode Test."); }

  protected:
    bool removed;
    bool replaced;
    LogicalStreamPtr sPtr;
    DumpContextPtr dumpContext;

    ExpressionNodePtr pred1, pred2, pred3, pred4, pred5, pred6, pred7;
    LogicalOperatorNodePtr sourceOp;

    LogicalOperatorNodePtr filterOp1, filterOp2, filterOp3, filterOp4, filterOp5, filterOp6, filterOp7;
    LogicalOperatorNodePtr filterOp1Copy, filterOp2Copy, filterOp3Copy, filterOp4Copy, filterOp5Copy, filterOp6Copy,
        filterOp7Copy;

    std::vector<NodePtr> children{};
    std::vector<NodePtr> parents{};
};

TEST_F(TranslateToGeneratableOperatorPhaseTest, translateFilterQuery) {
    /**
     * Sink -> Filter -> Source
     */
    auto schema = Schema::create();
    auto printSinkDescriptorPtr = PrintSinkDescriptor::create();
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(printSinkDescriptorPtr);
    auto constValue = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));
    auto fieldRead = FieldAccessExpressionNode::create(DataTypeFactory::createInt8(), "FieldName");
    auto andNode = EqualsExpressionNode::create(constValue, fieldRead);
    auto filter = LogicalOperatorFactory::createFilterOperator(andNode);
    sinkOperator->addChild(filter);
    filter->addChild(sourceOp);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 30000, streamConf);
    ConsoleDumpHandler::create()->dump(sinkOperator, std::cout);
    // we pass null as the buffer manager as we just want to check if the topology is correct.
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableSinkOperator = translatePhase->transform(sinkOperator->as<OperatorNode>());

    ASSERT_TRUE(generatableSinkOperator->instanceOf<GeneratableSinkOperator>());

    auto generatableFilterOperator = generatableSinkOperator->getChildren()[0];
    ASSERT_TRUE(generatableFilterOperator->instanceOf<GeneratableFilterOperator>());

    auto generatableSourceOperator = generatableFilterOperator->getChildren()[0];
    ASSERT_TRUE(generatableSourceOperator->instanceOf<GeneratableScanOperator>());
}

TEST_F(TranslateToGeneratableOperatorPhaseTest, translateWindowQuery) {
    /**
     * Sink -> Window -> Source
     */
    auto printSinkDescriptorPtr = PrintSinkDescriptor::create();
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(printSinkDescriptorPtr);
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowOperator = LogicalOperatorFactory::createWindowOperator(LogicalWindowDefinition::create(
        Attribute("id"), Sum(Attribute("value")), TumblingWindow::of(IngestionTime(), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 0, trigger, triggerAction));
    sinkOperator->addChild(windowOperator);
    windowOperator->addChild(sourceOp);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto engine = NodeEngine::create("127.0.0.1", 30000, streamConf);
    ConsoleDumpHandler::create()->dump(sinkOperator, std::cout);
    // we pass null as the buffer manager as we just want to check if the topology is correct.

    auto typeInferencePhase = TypeInferencePhase::create(std::make_shared<StreamCatalog>());
    auto queryPlan = typeInferencePhase->execute(QueryPlan::create(sinkOperator));
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    queryPlan = distributeWindowRule->apply(queryPlan);

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableSinkOperator = translatePhase->transform(queryPlan->getRootOperators()[0]);
    ASSERT_TRUE(generatableSinkOperator->instanceOf<GeneratableSinkOperator>());

    auto generatableWindowScanOperator = generatableSinkOperator->getChildren()[0];
    ASSERT_TRUE(generatableWindowScanOperator->instanceOf<GeneratableScanOperator>());

    auto generatableWindowOperator = generatableWindowScanOperator->getChildren()[0];
    ASSERT_TRUE(generatableWindowOperator->instanceOf<GeneratableCompleteWindowOperator>());

    auto generatableSourceOperator = generatableWindowOperator->getChildren()[0];
    ASSERT_TRUE(generatableSourceOperator->instanceOf<GeneratableScanOperator>());
}

}// namespace NES
