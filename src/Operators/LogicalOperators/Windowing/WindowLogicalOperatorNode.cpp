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

#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <sstream>
#include <z3++.h>

namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id)
    : WindowOperatorNode(windowDefinition, id) {
}

const std::string WindowLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW(" << id << ")";
    return ss.str();
}

bool WindowLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    bool eq = equal(rhs);
    bool idCmp = rhs->as<WindowLogicalOperatorNode>()->getId() == id;
    bool typeInfer = rhs->instanceOf<CentralWindowOperator>();
    return eq && idCmp && !typeInfer;
}

bool WindowLogicalOperatorNode::equal(const NodePtr rhs) const {
    return rhs->instanceOf<WindowLogicalOperatorNode>();
}

OperatorNodePtr WindowLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createWindowOperator(windowDefinition, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

bool WindowLogicalOperatorNode::inferSchema() {

    WindowOperatorNode::inferSchema();
    // infer the default input and output schema
    NES_DEBUG("SliceCreationOperator: TypeInferencePhase: infer types for window operator with input schema " << inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    windowAggregation->inferStamp(inputSchema);

    auto windowType = windowDefinition->getWindowType();
    if (windowDefinition->isKeyed()) {
        // infer the data type of the key field.
        windowDefinition->getOnKey()->inferStamp(inputSchema);
        outputSchema = Schema::create()
                           ->addField(createField("start", UINT64))
                           ->addField(createField("end", UINT64))
                           ->addField(AttributeField::create(windowDefinition->getOnKey()->getFieldName(), windowDefinition->getOnKey()->getStamp()))
                           ->addField(AttributeField::create(windowAggregation->as()->as<FieldAccessExpressionNode>()->getFieldName(), windowAggregation->on()->getStamp()));
        return true;
    } else {
        NES_THROW_RUNTIME_ERROR("SliceCreationOperator: type inference for non keyed streams is not supported");
    }
}

void WindowLogicalOperatorNode::inferZ3Expression(z3::ContextPtr context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    expr = std::make_shared<z3::expr>(OperatorToZ3ExprUtil::createForOperator(operatorNode, *context));
}

}// namespace NES
