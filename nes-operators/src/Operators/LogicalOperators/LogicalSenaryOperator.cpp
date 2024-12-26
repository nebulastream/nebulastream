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
#include <Operators/AbstractOperators/Arity/SenaryOperator.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalSenaryOperator.hpp>
#include <Util/Logger/Logger.hpp>

#include <algorithm>
#include <fmt/format.h>

namespace NES {

// Constructor Implementation
LogicalSenaryOperator::LogicalSenaryOperator(const ExpressionNodePtr one,
                                            const ExpressionNodePtr two,
                                            const ExpressionNodePtr three,
                                            const ExpressionNodePtr four,
                                            const ExpressionNodePtr five,
                                            const ExpressionNodePtr six,
                                             const std::string function,
                                             OperatorId id)
    : Operator(id),       // Initialize base class SenaryOperator with id
      SenaryOperator(id),// Initialize base class Operator with id
      one(one), two(two), three(three), four(four), five(five), six(six), function(function) {}

OperatorPtr LogicalSenaryOperator::copy() {
    // Create a deep copy of the LogicalSenaryOperator
    return std::make_shared<LogicalSenaryOperator>(one, two, three, four, five, six, function, this->getId());
}

bool LogicalSenaryOperator::inferSchema() {
    distinctSchemas.clear();

    // Check the number of child operators
    if (children.size() < 6) {
        NES_ERROR("SenaryOperator: this operator should have at least six child operators");
        throw TypeInferenceException("SenaryOperator: this node should have at least six child operators");
    }

    // Infer schema of all child operators
    for (const auto& child : children) {
        // Safely cast to LogicalOperator
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (!logicalChild || !logicalChild->inferSchema()) {
            NES_ERROR("SenaryOperator: failed inferring the schema of a child operator");
            throw TypeInferenceException("SenaryOperator: failed inferring the schema of a child operator");
        }
    }

    // Identify distinct schemas from child operators
    for (const auto& child : children) {
        auto childOperator = std::dynamic_pointer_cast<Operator>(child);
        if (!childOperator) {
            NES_ERROR("SenaryOperator: child operator cast failed");
            throw TypeInferenceException("SenaryOperator: child operator cast failed");
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
            fmt::format("SenaryOperator: Found {} distinct schemas but expected 2 or fewer distinct schemas.",
                        distinctSchemas.size()));
    }

    return true;
}

std::vector<OperatorPtr> LogicalSenaryOperator::getOperatorsBySchema(const SchemaPtr& schema) const {
    std::vector<OperatorPtr> operators;
    for (const auto& child : children) {
        auto childOperator = std::dynamic_pointer_cast<Operator>(child);
        if (childOperator && childOperator->getOutputSchema()->equals(schema, false)) {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<OperatorPtr> LogicalSenaryOperator::getoneOperators() const {
    return getOperatorsBySchema(distinctSchemas[0]);
}

std::vector<OperatorPtr> LogicalSenaryOperator::gettwoOperators() const {
    return getOperatorsBySchema(distinctSchemas[1]);
}

std::vector<OperatorPtr> LogicalSenaryOperator::getthreeOperators() const {
    return getOperatorsBySchema(distinctSchemas[2]);
}

std::vector<OperatorPtr> LogicalSenaryOperator::getfourOperators() const {
    return getOperatorsBySchema(distinctSchemas[3]);
}

std::vector<OperatorPtr> LogicalSenaryOperator::getfiveOperators() const {
    return getOperatorsBySchema(distinctSchemas[4]);
}

std::vector<OperatorPtr> LogicalSenaryOperator::getsixOperators() const {
    return getOperatorsBySchema(distinctSchemas[5]);
}

void LogicalSenaryOperator::inferInputOrigins() {
    // Collect input origins from left operators
    std::vector<OriginId> oneInputOriginIds;
    for (const auto& child : getoneOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            oneInputOriginIds.insert(oneInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->oneInputOriginIds = oneInputOriginIds;

    // Collect input origins from two operators
    std::vector<OriginId> twoInputOriginIds;
    for (const auto& child : gettwoOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            twoInputOriginIds.insert(twoInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->twoInputOriginIds = twoInputOriginIds;

    // Collect input origins from three operators

    std::vector<OriginId> threeInputOriginIds;
    for (const auto& child : getthreeOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            threeInputOriginIds.insert(threeInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->threeInputOriginIds = threeInputOriginIds;

    // Collect input origins from four operators

    std::vector<OriginId> fourInputOriginIds;
    for (const auto& child : getfourOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            fourInputOriginIds.insert(fourInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }

    this->fourInputOriginIds = fourInputOriginIds;

    // Collect input origins from five operators

    std::vector<OriginId> fiveInputOriginIds;
    for (const auto& child : getfiveOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            fiveInputOriginIds.insert(fiveInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->fiveInputOriginIds = fiveInputOriginIds;

    // Collect input origins from six operators

    std::vector<OriginId> sixInputOriginIds;
    for (const auto& child : getsixOperators()) {
        auto logicalChild = std::dynamic_pointer_cast<LogicalOperator>(child);
        if (logicalChild) {
            logicalChild->inferInputOrigins();
            auto childOrigins = logicalChild->getOutputOriginIds();
            sixInputOriginIds.insert(sixInputOriginIds.end(), childOrigins.begin(), childOrigins.end());
        }
    }
    this->sixInputOriginIds = sixInputOriginIds;
}

}// namespace NES
