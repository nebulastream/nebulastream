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
#include  <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {

LogicalUnaryOperatorNode::LogicalUnaryOperatorNode(OperatorId id)
    : OperatorNode(id), LogicalOperatorNode(id), UnaryOperatorNode(id) {}

bool LogicalUnaryOperatorNode::inferSchema() {

    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        if (!child->as<LogicalOperatorNode>()->inferSchema()) {
            return false;
        }
    }

    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("UnaryOperatorNode: this node should have at least one child operator");
    }

    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("UnaryOperatorNode: infer schema failed. The schema has to be the same across all child operators."
                      "this op schema= {} child schema={}",
                      child->as<OperatorNode>()->getOutputSchema()->toString(),
                      childSchema->toString());
            return false;
        }
    }

    //Reset and reinitialize the input and output schemas
    inputSchema->clear();
    inputSchema = inputSchema->copyFields(childSchema);
    inputSchema->setLayoutType(childSchema->getLayoutType());
    outputSchema->clear();
    outputSchema = outputSchema->copyFields(childSchema);
    outputSchema->setLayoutType(childSchema->getLayoutType());
    return true;
}

void LogicalUnaryOperatorNode::inferInputOrigins() {
    // in the default case we collect all input origins from the children/upstream operators
    std::vector<uint64_t> inputOriginIds;
    for (auto child : this->children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        inputOriginIds.insert(inputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->inputOriginIds = inputOriginIds;
}

}// namespace NES