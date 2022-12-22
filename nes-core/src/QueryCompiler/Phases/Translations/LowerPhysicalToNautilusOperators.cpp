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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/DivExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantInteger32Expression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/NegateExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalTumblingWindowSink.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSliceMergingOperatorHandler.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowMeasures/TimeUnit.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <utility>

namespace NES::QueryCompilation {

std::shared_ptr<LowerPhysicalToNautilusOperators> LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators::create() {
    return std::make_shared<LowerPhysicalToNautilusOperators>();
}

LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators() {}

PipelineQueryPlanPtr LowerPhysicalToNautilusOperators::apply(PipelineQueryPlanPtr pipelinedQueryPlan,
                                                             const Runtime::NodeEnginePtr& nodeEngine) {
    auto bufferSize = nodeEngine->getQueryManager()->getBufferManager()->getBufferSize();
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline, bufferSize);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr LowerPhysicalToNautilusOperators::apply(OperatorPipelinePtr operatorPipeline, size_t bufferSize) {
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;

    for (const auto& node : nodes) {
        parentOperator =
            lower(*pipeline, parentOperator, node->as<PhysicalOperators::PhysicalOperator>(), bufferSize, operatorHandlers);
    }
    for (auto& root : queryPlan->getRootOperators()) {
        queryPlan->removeAsRootOperator(root);
    }
    auto nautilusPipelineWrapper = NautilusPipelineOperator::create(pipeline, operatorHandlers);
    queryPlan->addRootOperator(nautilusPipelineWrapper);
    return operatorPipeline;
}
std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lower(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
                                        std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator,
                                        const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                        size_t bufferSize,
                                        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    if (operatorNode->instanceOf<PhysicalOperators::PhysicalScanOperator>()) {
        auto scan = lowerScan(pipeline, operatorNode, bufferSize);
        pipeline.setRootOperator(scan);
        return scan;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalEmitOperator>()) {
        auto emit = lowerEmit(pipeline, operatorNode, bufferSize);
        parentOperator->setChild(emit);
        return emit;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalFilterOperator>()) {
        auto filter = lowerFilter(pipeline, operatorNode);
        parentOperator->setChild(filter);
        return filter;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapOperator>()) {
        auto map = lowerMap(pipeline, operatorNode);
        parentOperator->setChild(map);
        return map;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalThresholdWindowOperator>()) {
        // TODO #3294: Support multiple aggregation functions
        auto aggregationType = operatorNode->as<PhysicalOperators::PhysicalThresholdWindowOperator>()
                                   ->getOperatorHandler()
                                   ->getWindowDefinition()
                                   ->getWindowAggregation()[0]
                                   ->getType();

        auto aggregationValue = std::make_unique<Runtime::Execution::Aggregation::AggregationValue>();

        switch (aggregationType) {
            case Windowing::WindowAggregationDescriptor::Avg:
                aggregationValue = std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue>();
                break;
            case Windowing::WindowAggregationDescriptor::Count:
                aggregationValue = std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue>();
                break;
            case Windowing::WindowAggregationDescriptor::Max:
                aggregationValue = std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue>();
                break;
            case Windowing::WindowAggregationDescriptor::Min:
                aggregationValue = std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue>();
                break;
            case Windowing::WindowAggregationDescriptor::Sum:
                aggregationValue = std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue>();
                break;
            default: NES_THROW_RUNTIME_ERROR("Unsupported aggregation type");
        }

        auto handler =
            std::make_shared<Runtime::Execution::Operators::ThresholdWindowOperatorHandler>(std::move(aggregationValue));
        operatorHandlers.push_back(handler);
        auto indexForThisHandler = operatorHandlers.size() - 1;

        auto thresholdWindow = lowerThresholdWindow(pipeline, operatorNode, indexForThisHandler);
        parentOperator->setChild(thresholdWindow);
        return thresholdWindow;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator>()) {
        auto preAggregationOperator = lowerGlobalThreadLocalPreAggregationOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(preAggregationOperator);
        return preAggregationOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalSliceMergingOperator>()) {
        return lowerGlobalSliceMergingOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalTumblingWindowSink>()) {
        // we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>()) {
        auto watermarkOperator = lowerWatermarkAssignmentOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(watermarkOperator);
        return watermarkOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalProjectOperator>()) {
        // we can ignore this operator for now.
        return parentOperator;
    }
    NES_NOT_IMPLEMENTED();
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerGlobalSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalGlobalSliceMergingOperator>();
    auto handler =
        std::get<std::shared_ptr<Runtime::Execution::Operators::GlobalSliceMergingHandler>>(physicalGSMO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto aggregationFunctions = lowerAggregations(physicalGSMO->getWindowDefinition()->getWindowAggregation());
    auto sliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::GlobalSliceMerging>(operatorHandlers.size() - 1, aggregationFunctions);
    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerGlobalThreadLocalPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator>();
    auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::GlobalSlicePreAggregationHandler>>(
        physicalGTLPAO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = physicalGTLPAO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);

    std::vector<Runtime::Execution::Expressions::ExpressionPtr> aggregationFields;
    std::transform(aggregations.cbegin(), aggregations.cend(), std::back_inserter(aggregationFields), [&](auto& agg) {
        return lowerExpression(agg->on());
    });
    auto timeWindow = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto timeCharacteristicField = timeWindow->getTimeCharacteristic()->getField()->getName();
    auto timeStampField = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeCharacteristicField);
    auto sliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::GlobalSlicePreAggregation>(operatorHandlers.size() - 1,
                                                                                   timeStampField,
                                                                                   aggregationFields,
                                                                                   aggregationFunctions);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerWatermarkAssignmentOperator(Runtime::Execution::PhysicalOperatorPipeline&,
                                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                                                   std::vector<Runtime::Execution::OperatorHandlerPtr>&) {
    auto wao = operatorPtr->as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>();
    auto eventTimeWatermarkStrategy =
        wao->getWatermarkStrategyDescriptor()->as<Windowing::EventTimeWatermarkStrategyDescriptor>();
    auto fieldExpression = lowerExpression(eventTimeWatermarkStrategy->getOnField());
    auto watermarkAssignmentOperator =
        std::make_shared<Runtime::Execution::Operators::EventTimeWatermarkAssignment>(fieldExpression);
    return watermarkAssignmentOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lowerScan(Runtime::Execution::PhysicalOperatorPipeline&,
                                            const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                            size_t bufferSize) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::ROW_LAYOUT, "Currently only row layout is supported");
    // pass buffer size here
    auto layout = std::make_shared<Runtime::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider =
        std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerEmit(Runtime::Execution::PhysicalOperatorPipeline&,
                                            const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                            size_t bufferSize) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::ROW_LAYOUT, "Currently only row layout is supported");
    // pass buffer size here
    auto layout = std::make_shared<Runtime::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider =
        std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerFilter(Runtime::Execution::PhysicalOperatorPipeline&,
                                              const PhysicalOperators::PhysicalOperatorPtr& operatorPtr) {
    auto filterOperator = operatorPtr->as<PhysicalOperators::PhysicalFilterOperator>();
    auto expression = lowerExpression(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(expression);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerMap(Runtime::Execution::PhysicalOperatorPipeline&,
                                           const PhysicalOperators::PhysicalOperatorPtr& operatorPtr) {
    auto mapOperator = operatorPtr->as<PhysicalOperators::PhysicalMapOperator>();
    auto assignmentField = mapOperator->getMapExpression()->getField();
    auto assignmentExpression = mapOperator->getMapExpression()->getAssignment();
    auto expression = lowerExpression(assignmentExpression);
    auto writeField =
        std::make_shared<Runtime::Execution::Expressions::WriteFieldExpression>(assignmentField->getFieldName(), expression);
    return std::make_shared<Runtime::Execution::Operators::Map>(writeField);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerThresholdWindow(Runtime::Execution::PhysicalOperatorPipeline&,
                                                       const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                                       uint64_t handlerIndex) {
    auto thresholdWindowOperator = operatorPtr->as<PhysicalOperators::PhysicalThresholdWindowOperator>();
    auto contentBasedWindowType = Windowing::ContentBasedWindowType::asContentBasedWindowType(
        thresholdWindowOperator->getOperatorHandler()->getWindowDefinition()->getWindowType());
    auto thresholdWindowType = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
    auto predicate = lowerExpression(thresholdWindowType->getPredicate());
    auto minCount = thresholdWindowType->getMinimumCount();

    auto aggregations = thresholdWindowOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction;
    // TODO 3280: Support multiple aggregation functions
    if (aggregations.size() != 1) {
        NES_NOT_IMPLEMENTED();
    } else {
        auto aggregation = aggregations[0];
        switch (aggregation->getType()) {

            case Windowing::WindowAggregationDescriptor::Avg:
                aggregationFunction = std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(
                    aggregation->getInputStamp(),
                    aggregation->getFinalAggregateStamp());
                break;
            case Windowing::WindowAggregationDescriptor::Count:
                // TODO 3280: Fix threhsold window with count, i.e., no aggregated field
//                NES_NOT_IMPLEMENTED();
                aggregationFunction = std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(
                    aggregation->getInputStamp(),
                    aggregation->getFinalAggregateStamp());
                break;
            case Windowing::WindowAggregationDescriptor::Max:
                aggregationFunction = std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(
                    aggregation->getInputStamp(),
                    aggregation->getFinalAggregateStamp());
                break;
            case Windowing::WindowAggregationDescriptor::Min:
                aggregationFunction = std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(
                    aggregation->getInputStamp(),
                    aggregation->getFinalAggregateStamp());
                break;
            case Windowing::WindowAggregationDescriptor::Sum:
                aggregationFunction = std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(
                    aggregation->getInputStamp(),
                    aggregation->getFinalAggregateStamp());
                break;
            default: NES_NOT_IMPLEMENTED();
        }
        // Obtain the field name used to store the aggregation result
        auto thresholdWindowResultSchema =
            operatorPtr->as<PhysicalOperators::PhysicalThresholdWindowOperator>()->getOperatorHandler()->getResultSchema();
        auto aggregationResultFieldName =
            thresholdWindowResultSchema->getSourceNameQualifier() + "$" + aggregation->getTypeAsString();

        auto aggregatedFieldAccess = lowerExpression(aggregation->on());

        return std::make_shared<Runtime::Execution::Operators::ThresholdWindow>(predicate,
                                                                                minCount,
                                                                                aggregatedFieldAccess,
                                                                                aggregationResultFieldName,
                                                                                aggregationFunction,
                                                                                handlerIndex);
    }
}

std::shared_ptr<Runtime::Execution::Expressions::Expression>
LowerPhysicalToNautilusOperators::lowerExpression(const ExpressionNodePtr& expressionNode) {
    if (auto andNode = expressionNode->as_if<AndExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(andNode->getLeft());
        auto rightNautilusExpression = lowerExpression(andNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::AndExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto orNode = expressionNode->as_if<OrExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(orNode->getLeft());
        auto rightNautilusExpression = lowerExpression(orNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::OrExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto lessNode = expressionNode->as_if<LessExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(lessNode->getLeft());
        auto rightNautilusExpression = lowerExpression(lessNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::LessThanExpression>(leftNautilusExpression,
                                                                                     rightNautilusExpression);
    } else if (auto equalsNode = expressionNode->as_if<EqualsExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(equalsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(equalsNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::EqualsExpression>(leftNautilusExpression,
                                                                                   rightNautilusExpression);
    } else if (auto greaterNode = expressionNode->as_if<GreaterExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(greaterNode->getLeft());
        auto rightNautilusExpression = lowerExpression(greaterNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::GreaterThanExpression>(leftNautilusExpression,
                                                                                        rightNautilusExpression);
    } else if (auto negateNode = expressionNode->as_if<NegateExpressionNode>()) {
        auto child = lowerExpression(negateNode->getChildren()[0]->as<ExpressionNode>());
        return std::make_shared<Runtime::Execution::Expressions::NegateExpression>(child);
    } else if (auto mulNode = expressionNode->as_if<MulExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(mulNode->getLeft());
        auto rightNautilusExpression = lowerExpression(mulNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::MulExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto addNode = expressionNode->as_if<AddExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(addNode->getLeft());
        auto rightNautilusExpression = lowerExpression(addNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::AddExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto subNode = expressionNode->as_if<SubExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(subNode->getLeft());
        auto rightNautilusExpression = lowerExpression(subNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::SubExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto divNode = expressionNode->as_if<DivExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(divNode->getLeft());
        auto rightNautilusExpression = lowerExpression(divNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::DivExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto constantValue = expressionNode->as_if<ConstantValueExpressionNode>()) {
        auto value = constantValue->getConstantValue();
        if (constantValue->getStamp()->isInteger()) {
            auto integerValue = std::stoi(value->as<BasicValue>()->value);
            return std::make_shared<Runtime::Execution::Expressions::ConstantInteger32Expression>(integerValue);
        } else {
            NES_NOT_IMPLEMENTED();
        }
    } else if (auto fieldAccess = expressionNode->as_if<FieldAccessExpressionNode>()) {
        return std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(fieldAccess->getFieldName());
    }
    NES_NOT_IMPLEMENTED();
}

std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
LowerPhysicalToNautilusOperators::lowerAggregations(const std::vector<Windowing::WindowAggregationPtr>& aggs) {
    std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>> aggregationFunctions;
    std::transform(aggs.cbegin(),
                   aggs.cend(),
                   std::back_inserter(aggregationFunctions),
                   [&](const auto& agg) -> std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction> {
                       switch (agg->getType()) {
                           case Windowing::WindowAggregationDescriptor::Avg:
                               return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(
                                   agg->getInputStamp(),
                                   agg->getFinalAggregateStamp());
                           case Windowing::WindowAggregationDescriptor::Count:
                               return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(
                                   agg->getInputStamp(),
                                   agg->getFinalAggregateStamp());
                           case Windowing::WindowAggregationDescriptor::Max:
                               return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(
                                   agg->getInputStamp(),
                                   agg->getFinalAggregateStamp());
                           case Windowing::WindowAggregationDescriptor::Min:
                               return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(
                                   agg->getInputStamp(),
                                   agg->getFinalAggregateStamp());
                           case Windowing::WindowAggregationDescriptor::Median:
                               // TODO add median aggregation function
                               break;
                           case Windowing::WindowAggregationDescriptor::Sum: {
                               return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(
                                   agg->getInputStamp(),
                                   agg->getFinalAggregateStamp());
                           }
                       };
                       NES_NOT_IMPLEMENTED();
                   });
    return aggregationFunctions;
}

}// namespace NES::QueryCompilation