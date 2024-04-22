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
#include  <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {

LogicalBinaryOperator::LogicalBinaryOperator(OperatorId id)
    : Operator(id), LogicalOperator(id), BinaryOperator(id) {}

bool LogicalBinaryOperator::inferSchema() {

    distinctSchemas.clear();
    //Check the number of child operators
    if (children.size() < 2) {
        NES_ERROR("BinaryOperator: this operator should have at least two child operators");
        throw TypeInferenceException("BinaryOperator: this node should have at least two child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        if (!child->as<LogicalOperator>()->inferSchema()) {
            NES_ERROR("BinaryOperator: failed inferring the schema of the child operator");
            throw TypeInferenceException("BinaryOperator: failed inferring the schema of the child operator");
        }
    }

    //Identify different type of schemas from children operators
    for (auto& child : children) {
        auto childOutputSchema = child->as<Operator>()->getOutputSchema();
        auto found = std::find_if(distinctSchemas.begin(), distinctSchemas.end(), [&](const SchemaPtr& distinctSchema) {
            return childOutputSchema->equals(distinctSchema, false);
        });
        if (found == distinctSchemas.end()) {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() > 2) {
        throw TypeInferenceException("BinaryOperator: Found " + std::to_string(distinctSchemas.size())
                                     + " distinct schemas but expected 2 or less distinct schemas.");
    }

    return true;
}

std::vector<OperatorPtr> LogicalBinaryOperator::getOperatorsBySchema(const SchemaPtr& schema) {
    std::vector<OperatorPtr> operators;
    for (const auto& child : getChildren()) {
        auto childOperator = child->as<Operator>();
        if (childOperator->getOutputSchema()->equals(schema, false)) {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<OperatorPtr> LogicalBinaryOperator::getLeftOperators() { return getOperatorsBySchema(getLeftInputSchema()); }

std::vector<OperatorPtr> LogicalBinaryOperator::getRightOperators() {
    return getOperatorsBySchema(getRightInputSchema());
}

void LogicalBinaryOperator::inferInputOrigins() {
    // in the default case we collect all input origins from the children/upstream operators
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