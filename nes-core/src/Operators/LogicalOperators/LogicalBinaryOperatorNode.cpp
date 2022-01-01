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

std::vector<OperatorNodePtr> LogicalBinaryOperatorNode::getOperatorsBySchema(const SchemaPtr& schema) {
    std::vector<OperatorNodePtr> operators;
    for (const auto& child : getChildren()) {
        auto childOperator = child->as<OperatorNode>();
        if (childOperator->getOutputSchema()->equals(schema, false)) {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<OperatorNodePtr> LogicalBinaryOperatorNode::getLeftOperators() { return getOperatorsBySchema(getLeftInputSchema()); }

std::vector<OperatorNodePtr> LogicalBinaryOperatorNode::getRightOperators() {
    return getOperatorsBySchema(getRightInputSchema());
}

}// namespace NES