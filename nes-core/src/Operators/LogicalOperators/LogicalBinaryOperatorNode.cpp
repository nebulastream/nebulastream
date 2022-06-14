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
#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
namespace NES {

LogicalBinaryOperatorNode::LogicalBinaryOperatorNode(OperatorId id)
    : OperatorNode(id), LogicalOperatorNode(id), BinaryOperatorNode(id) {}

bool LogicalBinaryOperatorNode::inferSchema() {

    distinctSchemas.clear();
    //Check the number of child operators
    if (children.size() < 2) {
        NES_ERROR("BinaryOperatorNode: this operator should have at least two child operators");
        throw TypeInferenceException("BinaryOperatorNode: this node should have at least two child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        if (!child->as<LogicalOperatorNode>()->inferSchema()) {
            NES_ERROR("BinaryOperatorNode: failed inferring the schema of the child operator");
            throw TypeInferenceException("BinaryOperatorNode: failed inferring the schema of the child operator");
        }
    }

    //Identify different type of schemas from children operators
    for (auto& child : children) {
        auto childOutputSchema = child->as<OperatorNode>()->getOutputSchema();
        auto found = std::find_if(distinctSchemas.begin(), distinctSchemas.end(), [&](const SchemaPtr& distinctSchema) {
            return childOutputSchema->equals(distinctSchema, false);
        });
        if (found == distinctSchemas.end()) {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() > 2) {
        throw TypeInferenceException("BinaryOperatorNode: Found " + std::to_string(distinctSchemas.size())
                                     + " distinct schemas but expected 2 or less distinct schemas.");
    }

    return true;
}

std::vector<OperatorNodePtr> LogicalBinaryOperatorNode::getOperatorsByIds(const std::vector<OperatorId>& operatorIds) {
    std::vector<OperatorNodePtr> operators;
    for (const auto& child : getChildren()) {
        auto childOperator = child->as<OperatorNode>();

        //Check if the child operator matches an id in the operator id vector
        auto found = std::find_if(operatorIds.begin(), operatorIds.end(), [childOperator](const OperatorId operatorId) {
            return childOperator->getId() == operatorId;
        });

        //Add the matched child to the vector of operators to return
        if (found != operatorIds.end()) {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<OperatorNodePtr> LogicalBinaryOperatorNode::getLeftUpstreamOperators() {
    return getOperatorsByIds(leftUpStreamOperatorIds);
}

std::vector<OperatorNodePtr> LogicalBinaryOperatorNode::getRightUpstreamOperators() {
    return getOperatorsByIds(rightUpStreamOperatorIds);
}

void LogicalBinaryOperatorNode::inferInputOrigins() {
    // in the default case we collect all input origins from the children/upstream operators
    std::vector<uint64_t> leftInputOriginIds;
    for (const auto& child : this->getLeftUpstreamOperators()) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        leftInputOriginIds.insert(leftInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->leftInputOriginIds = leftInputOriginIds;

    std::vector<uint64_t> rightInputOriginIds;
    for (const auto& child : this->getRightUpstreamOperators()) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        rightInputOriginIds.insert(rightInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->rightInputOriginIds = rightInputOriginIds;
}

void LogicalBinaryOperatorNode::addLeftUpStreamOperatorId(OperatorId operatorId) {
    leftUpStreamOperatorIds.push_back(operatorId);
}

void LogicalBinaryOperatorNode::addRightUpStreamOperatorId(OperatorId operatorId) {
    rightUpStreamOperatorIds.push_back(operatorId);
}

std::vector<OperatorId> LogicalBinaryOperatorNode::getLeftUpStreamOperatorIds() { return leftUpStreamOperatorIds; }

std::vector<OperatorId> LogicalBinaryOperatorNode::getRightUpStreamOperatorIds() { return rightUpStreamOperatorIds; }

void LogicalBinaryOperatorNode::removeLeftUpstreamOperatorId(OperatorId operatorId) {
    auto found = std::find(leftUpStreamOperatorIds.begin(), leftUpStreamOperatorIds.end(), operatorId);
    if (found != leftUpStreamOperatorIds.end()) {
        leftUpStreamOperatorIds.erase(found);
    }
}

void LogicalBinaryOperatorNode::removeRightUpstreamOperatorId(OperatorId operatorId) {
    auto found = std::find(rightUpStreamOperatorIds.begin(), rightUpStreamOperatorIds.end(), operatorId);
    if (found != rightUpStreamOperatorIds.end()) {
        rightUpStreamOperatorIds.erase(found);
    }
}

bool LogicalBinaryOperatorNode::isLeftUpstreamOperatorId(OperatorId operatorId) {
    auto found = std::find(leftUpStreamOperatorIds.begin(), leftUpStreamOperatorIds.end(), operatorId);
    return found != leftUpStreamOperatorIds.end();
}

bool LogicalBinaryOperatorNode::isRightUpstreamOperatorId(OperatorId operatorId) {
    auto found = std::find(rightUpStreamOperatorIds.begin(), rightUpStreamOperatorIds.end(), operatorId);
    return found != rightUpStreamOperatorIds.end();
}

}// namespace NES