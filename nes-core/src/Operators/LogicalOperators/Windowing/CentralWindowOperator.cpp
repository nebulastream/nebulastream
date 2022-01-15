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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

#include <z3++.h>

namespace NES {

CentralWindowOperator::CentralWindowOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id)
    : OperatorNode(id), WindowOperatorNode(windowDefinition, id) {
    windowDefinition->setDistributionCharacteristic(Windowing::DistributionCharacteristic::createCompleteWindowType());
}

std::string CentralWindowOperator::toString() const {
    std::stringstream ss;
    ss << "CENTRALWINDOW(" << id << ")";
    return ss.str();
}

bool CentralWindowOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<CentralWindowOperator>()->getId() == id;
}

bool CentralWindowOperator::equal(NodePtr const& rhs) const {
    return rhs->instanceOf<CentralWindowOperator>()
        && rhs->as<WindowOperatorNode>()->getWindowDefinition()->equal(this->getWindowDefinition());
}

OperatorNodePtr CentralWindowOperator::copy() {
    auto copy = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDefinition, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}
bool CentralWindowOperator::inferSchema() {
    if (!WindowOperatorNode::inferSchema()) {
        return false;
    }
    // infer the default input and output schema
    NES_DEBUG("SliceCreationOperator: TypeInferencePhase: infer types for window operator with input schema "
              << inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    windowAggregation->inferStamp(inputSchema);
    auto windowType = windowDefinition->getWindowType();
    windowType->inferStamp(inputSchema);

    //Construct output schema
    outputSchema->clear();

    outputSchema =
        outputSchema
            ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start", UINT64))
            ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "end", UINT64));

    if (windowDefinition->isKeyed()) {
        // infer the data type of the key field.
        windowDefinition->getOnKey()->inferStamp(inputSchema);
        outputSchema->addField(
            AttributeField::create(windowDefinition->getOnKey()->getFieldName(), windowDefinition->getOnKey()->getStamp()));
    }
    outputSchema->addField(AttributeField::create(windowAggregation->as()->as<FieldAccessExpressionNode>()->getFieldName(),
                                                  windowAggregation->getFinalAggregateStamp()));
    return true;
}

void CentralWindowOperator::inferStringSignature() { NES_NOT_IMPLEMENTED(); }

std::string CentralWindowOperator::getClassName(){
    return "CentralWindowOperator";
}

}// namespace NES
