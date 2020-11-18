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
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>

namespace NES {

SliceMergingOperator::SliceMergingOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id)
    : WindowOperatorNode(windowDefinition, id) {}

const std::string SliceMergingOperator::toString() const {
    std::stringstream ss;
    ss << "SliceMergingOperator(" << id << ")";
    return ss.str();
}

bool SliceMergingOperator::isIdentical(NodePtr rhs) const { return equal(rhs) && rhs->as<SliceMergingOperator>()->getId() == id; }

bool SliceMergingOperator::equal(const NodePtr rhs) const { return rhs->instanceOf<SliceMergingOperator>(); }

OperatorNodePtr SliceMergingOperator::copy() {
    auto copy = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDefinition, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
bool SliceMergingOperator::inferSchema() {
    // infer the default input and output schema

    WindowOperatorNode::inferSchema();

    NES_DEBUG("WindowLogicalOperatorNode: TypeInferencePhase: infer types for window operator with input schema "
              << inputSchema->toString());
    return true;
}

}// namespace NES
