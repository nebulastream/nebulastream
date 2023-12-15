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

#include "Operators/LogicalOperators/Operators/LogicalBroadcastOperator.hpp"
#include "API/AttributeField.hpp"
#include "API/Schema.hpp"
#include "Operators/Expressions/FieldAssignmentExpressionNode.hpp"
#include "Util/Logger/Logger.hpp"
namespace NES {

LogicalBroadcastOperator::LogicalBroadcastOperator(OperatorId id)
    : OperatorNode(id), ExchangeOperatorNode(id), LogicalOperator(id) {}

bool LogicalBroadcastOperator::isIdentical(NodePtr const& rhs) const {
    return rhs->as<LogicalBroadcastOperator>()->getId() == id;
}

bool LogicalBroadcastOperator::equal(NodePtr const& rhs) const { return rhs->instanceOf<LogicalBroadcastOperator>(); };

std::string LogicalBroadcastOperator::toString() const {
    std::stringstream ss;
    ss << "BROADCAST(" << outputSchema->toString() << ")";
    return ss.str();
}

OperatorNodePtr LogicalBroadcastOperator::copy() {
    auto copy = LogicalOperatorFactory::createBroadcastOperator(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalBroadcastOperator::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("ExchangeOperatorNode: this node should have at least one child operator");
    }

    for (const auto& child : children) {
        if (!child->as<LogicalOperator>()->inferSchema()) {
            return false;
        }
    }

    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("ExchangeOperatorNode: infer schema failed. The schema has to be the same across all child operators.");
            return false;
        }
    }

    inputSchema = childSchema->copy();
    outputSchema = childSchema->copy();
    return true;
}

void LogicalBroadcastOperator::inferStringSignature() { NES_NOT_IMPLEMENTED(); }
void LogicalBroadcastOperator::inferInputOrigins() {
    // in the default case we collect all input origins from the children/upstream operators
    std::vector<uint64_t> inputOriginIds;
    for (auto child : this->children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperator>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        inputOriginIds.insert(inputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    //TODO add a check to check that we don't include the same origin id multiple times as this would show an invalid query plan
    this->inputOriginIds = inputOriginIds;
}

}// namespace NES
