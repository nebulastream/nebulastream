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
#include <Operators/AbstractOperators/Arity/QuaternaryOperator.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalQuaternaryOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

LogicalQuaternaryOperator::LogicalQuaternaryOperator(const ExpressionNodePtr left,
                                                     const ExpressionNodePtr middle,
                                                     const ExpressionNodePtr right,
                                                     const std::string function)
    : Operator(id),          // Initialize base class Operator
      QuaternaryOperator(id),// Initialize base class QuaternaryOperator
      left(left), middle(middle), right(right), function(function) {}

OperatorPtr LogicalQuaternaryOperator::copy() {
    return std::make_shared<LogicalQuaternaryOperator>(left, middle, right, function);
}

bool LogicalQuaternaryOperator::inferSchema() {
    distinctSchemas.clear();
    // Check the number of child operators
    if (children.size() < 3) {
        NES_ERROR("TernaryOperator: this operator should have at least three child operators");
        throw TypeInferenceException("TernaryOperator: this node should have at least three child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        if (!child->as<LogicalOperator>()->inferSchema()) {
            NES_ERROR("TernaryOperator: failed inferring the schema of the child operator");
            throw TypeInferenceException("TernaryOperator: failed inferring the schema of the child operator");
        }
    }

    // Identify different types of schemas from child operators
    for (const auto& child : children) {
        auto childOutputSchema = child->as<Operator>()->getOutputSchema();
        auto found = std::find_if(distinctSchemas.begin(), distinctSchemas.end(), [&](const SchemaPtr& distinctSchema) {
            return childOutputSchema->equals(distinctSchema, false);
        });
        if (found == distinctSchemas.end()) {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    // Validate that only two different types of schemas were present
    if (distinctSchemas.size() > 2) {
        throw TypeInferenceException(
            fmt::format("TernaryOperator: Found {} distinct schemas but expected 2 or less distinct schemas.",
                        distinctSchemas.size()));
    }

    return true;
}

std::vector<OperatorPtr> LogicalQuaternaryOperator::getOperatorsBySchema(const SchemaPtr& schema) const {
    std::vector<OperatorPtr> operators;
    for (const auto& child : getChildren()) {
        auto childOperator = child->as<Operator>();
        if (childOperator->getOutputSchema()->equals(schema, false)) {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<OperatorPtr> LogicalQuaternaryOperator::getLeftOperators() const {
    return getOperatorsBySchema(getLeftInputSchema());
}

std::vector<OperatorPtr> LogicalQuaternaryOperator::getRightOperators() const {
    return getOperatorsBySchema(getRightInputSchema());
}

void LogicalQuaternaryOperator::inferInputOrigins() {
    // In the default case, we collect all input origins from the children/upstream operators
    std::vector<OriginId> leftInputOriginIds;
    for (auto child : this->getLeftOperators()) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        leftInputOriginIds.insert(leftInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->leftInputOriginIds = leftInputOriginIds;

    std::vector<OriginId> rightInputOriginIds;
    for (auto child : this->getRightOperators()) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        rightInputOriginIds.insert(rightInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->rightInputOriginIds = rightInputOriginIds;
}

}// namespace NES
