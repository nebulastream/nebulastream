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
#include <Operators/AbstractOperators/Arity/TernaryOperator.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalTernaryOperator.hpp>
#include <Util/Logger/Logger.hpp>

#include <algorithm>
#include <fmt/format.h>

namespace NES {

// Constructor Implementation
LogicalTernaryOperator::LogicalTernaryOperator(const ExpressionNodePtr left,
                                               const ExpressionNodePtr middle,
                                               const ExpressionNodePtr right,
                                               const std::string function,
                                               OperatorId id)
    : Operator(id),       // Initialize base class TernaryOperator with id
      TernaryOperator(id),// Initialize base class Operator with id
      left(left), middle(middle), right(right), function(function) {}

OperatorPtr LogicalTernaryOperator::copy() {
    // Create a deep copy of the LogicalTernaryOperator
    return std::make_shared<LogicalTernaryOperator>(left, middle, right, function, this->getId());
}

bool LogicalTernaryOperator::inferSchema() {
    distinctSchemas.clear();

    // Check the number of child operators
    if (children.size() < 3) {
        NES_ERROR("TernaryOperator: this operator should have at least three child operators");
        throw TypeInferenceException("TernaryOperator: this node should have at least three child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        // Safely cast to LogicalOperator
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (!logicalChild || !logicalChild->inferSchema()) {
            NES_ERROR("TernaryOperator: failed inferring the schema of a child operator");
            throw TypeInferenceException("TernaryOperator: failed inferring the schema of a child operator");
        }
    }

    // Identify distinct schemas from child operators
    for (const auto& child : children) {
        auto childOperator = std::dynamic_pointer_cast<Operator>(child);
        if (!childOperator) {
            NES_ERROR("TernaryOperator: child operator cast failed");
            throw TypeInferenceException("TernaryOperator: child operator cast failed");
        }
        auto childOutputSchema = childOperator->getOutputSchema();
        auto it = std::find_if(distinctSchemas.begin(), distinctSchemas.end(), [&](const SchemaPtr& schema) {
            return childOutputSchema->equals(schema, false);
        });
        if (it == distinctSchemas.end()) {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    // Validate that there are at most two distinct schemas
    if (distinctSchemas.size() > 2) {
        throw TypeInferenceException(
            fmt::format("TernaryOperator: Found {} distinct schemas but expected 2 or fewer distinct schemas.",
                        distinctSchemas.size()));
    }

    return true;
}

std::vector<OperatorPtr> LogicalTernaryOperator::getOperatorsBySchema(const SchemaPtr& schema) const {
    std::vector<OperatorPtr> operators;
    for (const auto& child : children) {
        auto childOperator = std::dynamic_pointer_cast<Operator>(child);
        if (childOperator && childOperator->getOutputSchema()->equals(schema, false)) {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<OperatorPtr> LogicalTernaryOperator::getLeftOperators() { return getOperatorsBySchema(leftInputSchema); }

std::vector<OperatorPtr> LogicalTernaryOperator::getRightOperators() { return getOperatorsBySchema(rightInputSchema); }

void LogicalTernaryOperator::inferInputOrigins() {
    // Collect input origins from left operators
    std::vector<OriginId> leftInputOriginIds;
    for (const auto& child : getLeftOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            leftInputOriginIds.insert(leftInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->leftInputOriginIds = leftInputOriginIds;

    // Collect input origins from right operators
    std::vector<OriginId> rightInputOriginIds;
    for (const auto& child : getRightOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            rightInputOriginIds.insert(rightInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->rightInputOriginIds = rightInputOriginIds;
}

}// namespace NES
