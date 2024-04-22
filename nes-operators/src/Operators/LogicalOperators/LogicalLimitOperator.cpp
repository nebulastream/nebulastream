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

#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {

LogicalLimitOperator::LogicalLimitOperator(uint64_t limit, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), limit(limit) {}

uint64_t LogicalLimitOperator::getLimit() const { return limit; }

bool LogicalLimitOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalLimitOperator>()->getId() == id;
}

bool LogicalLimitOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalLimitOperator>()) {
        auto limitOperator = rhs->as<LogicalLimitOperator>();
        return limit == limitOperator->limit;
    }
    return false;
};

std::string LogicalLimitOperator::toString() const {
    std::stringstream ss;
    ss << "LIMIT" << id << ")";
    return ss.str();
}

bool LogicalLimitOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    return true;
}

OperatorPtr LogicalLimitOperator::copy() {
    auto copy = LogicalOperatorFactory::createLimitOperator(limit, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setStatisticId(statisticId);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalLimitOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalLimitOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalLimitOperator: Limit should have children");

    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "LIMIT(" << limit << ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
