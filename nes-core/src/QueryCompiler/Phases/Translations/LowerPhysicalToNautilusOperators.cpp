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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Common/ValueTypes/ValueType.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/ConstantInteger32Expression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/NegateExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <utility>

namespace NES::QueryCompilation {

std::shared_ptr<LowerPhysicalToNautilusOperators> LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators::create() {
    return std::make_shared<LowerPhysicalToNautilusOperators>();
}

LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators() {}

PipelineQueryPlanPtr LowerPhysicalToNautilusOperators::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr LowerPhysicalToNautilusOperators::apply(OperatorPipelinePtr operatorPipeline) {
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;
    for (const auto& node : nodes) {
        parentOperator = lower(*pipeline, parentOperator, node->as<PhysicalOperators::PhysicalOperator>());
    }
    for (auto& root : queryPlan->getRootOperators()) {
        queryPlan->removeAsRootOperator(root);
    }
    auto nautilusPipelineWrapper = NautilusPipelineOperator::create(pipeline);
    queryPlan->addRootOperator(nautilusPipelineWrapper);
    return operatorPipeline;
}
std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lower(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
                                        std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator,
                                        PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    if (operatorNode->instanceOf<PhysicalOperators::PhysicalScanOperator>()) {
        auto scan = lowerScan(pipeline, operatorNode);
        pipeline.setRootOperator(scan);
        return scan;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalEmitOperator>()) {
        auto emit = lowerEmit(pipeline, operatorNode);
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
    }
    NES_NOT_IMPLEMENTED();
}
std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lowerScan(Runtime::Execution::PhysicalOperatorPipeline&,
                                            PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::ROW_LAYOUT, "Currently only row layout is supported");
    // pass buffer size here
    auto layout = std::make_shared<Runtime::MemoryLayouts::RowLayout>(schema, 410000);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider =
        std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerEmit(Runtime::Execution::PhysicalOperatorPipeline&,
                                            PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::ROW_LAYOUT, "Currently only row layout is supported");
    // pass buffer size here
    auto layout = std::make_shared<Runtime::MemoryLayouts::RowLayout>(schema, 420000);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider =
        std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerFilter(Runtime::Execution::PhysicalOperatorPipeline&,
                                              PhysicalOperators::PhysicalOperatorPtr operatorPtr) {
    auto filterOperator = operatorPtr->as<PhysicalOperators::PhysicalFilterOperator>();
    auto expression = lowerExpression(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(expression);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerMap(Runtime::Execution::PhysicalOperatorPipeline&,
                                           PhysicalOperators::PhysicalOperatorPtr) {
    auto c1 = std::make_shared<Runtime::Execution::Expressions::ConstantIntegerExpression>(12);
    auto c2 = std::make_shared<Runtime::Execution::Expressions::ConstantIntegerExpression>(12);
    auto equals = std::make_shared<Runtime::Execution::Expressions::EqualsExpression>(c1, c2);
    return std::make_shared<Runtime::Execution::Operators::Selection>(equals);
}
std::shared_ptr<Runtime::Execution::Expressions::Expression>
LowerPhysicalToNautilusOperators::lowerExpression(ExpressionNodePtr expressionNode) {
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
    } else if (auto greaterNode = expressionNode->as_if<GreaterExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(greaterNode->getLeft());
        auto rightNautilusExpression = lowerExpression(greaterNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::GreaterThanExpression>(leftNautilusExpression,
                                                                                        rightNautilusExpression);
    } else if (auto negateNode = expressionNode->as_if<NegateExpressionNode>()) {
        auto child = lowerExpression(negateNode->getChildren()[0]->as<ExpressionNode>());
        return std::make_shared<Runtime::Execution::Expressions::NegateExpression>(child);
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

}// namespace NES::QueryCompilation