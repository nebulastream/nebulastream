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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES {

WindowComputationOperator::WindowComputationOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id)
    : OperatorNode(id), WindowOperatorNode(windowDefinition, id) {
    this->windowDefinition->setDistributionCharacteristic(windowDefinition->getDistributionType());
    this->windowDefinition->setNumberOfInputEdges(windowDefinition->getNumberOfInputEdges());
    this->windowDefinition->setTriggerPolicy(windowDefinition->getTriggerPolicy());
    this->windowDefinition->setWindowAggregation(windowDefinition->getWindowAggregation());
    this->windowDefinition->setWindowType(windowDefinition->getWindowType());
    this->windowDefinition->setOnKey(windowDefinition->getOnKey());
    this->windowDefinition->setOriginId(id);
}

std::string WindowComputationOperator::toString() const {
    std::stringstream ss;
    ss << "WindowComputationOperator(" << id << ")";
    return ss.str();
}

bool WindowComputationOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<WindowComputationOperator>()->getId() == id;
}

bool WindowComputationOperator::equal(NodePtr const& rhs) const { return rhs->instanceOf<WindowComputationOperator>(); }

OperatorNodePtr WindowComputationOperator::copy() {
    auto copy = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDefinition, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}
bool WindowComputationOperator::inferSchema() {
    if (!WindowOperatorNode::inferSchema()) {
        return false;
    }
    // infer the default input and output schema
    NES_DEBUG("WindowComputationOperator: TypeInferencePhase: infer types for window operator with input schema "
              << inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    windowAggregation->inferStamp(inputSchema);

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
                                                  windowAggregation->on()->getStamp()));
    return true;
}

void WindowComputationOperator::inferStringSignature() { NES_NOT_IMPLEMENTED(); }
}// namespace NES