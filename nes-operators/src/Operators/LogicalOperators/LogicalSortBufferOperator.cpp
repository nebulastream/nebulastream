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

#include <Operators/LogicalOperators/LogicalSortBufferOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {

LogicalSortBufferOperator::LogicalSortBufferOperator(std::string const& sortFieldIdentifier, std::string const& sortOrder, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), sortFieldIdentifier(sortFieldIdentifier), sortOrder(sortOrder) {}

std::string LogicalSortBufferOperator::getSortFieldIdentifier() const { return sortFieldIdentifier; }

std::string LogicalSortBufferOperator::getSortOrder() const { return sortOrder; }

bool LogicalSortBufferOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && NES::Util::as<LogicalSortBufferOperator>(rhs)->getId() == id;
}

bool LogicalSortBufferOperator::equal(NodePtr const& rhs) const {
    if (NES::Util::instanceOf<LogicalSortBufferOperator>(rhs)) {
        auto sortBufferOperator = NES::Util::as<LogicalSortBufferOperator>(rhs);
        return sortFieldIdentifier==sortBufferOperator->sortFieldIdentifier && sortOrder==sortBufferOperator->sortOrder;
    }
    return false;
};

std::string LogicalSortBufferOperator::toString() const {
    std::stringstream ss;
    // ss << "SORTBUFFER(opId: " << id << ", statisticId: " << statisticId << ", sortFieldIdentifier: " << sortFieldIdentifier << ", sortOrder: " << sortOrder << ")";
    ss << "SORTBUFFER(opId: " << id << ", sortFieldIdentifier: " << sortFieldIdentifier << ", sortOrder: " << sortOrder << ")";
    return ss.str();
}

bool LogicalSortBufferOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    return true;
}

OperatorPtr LogicalSortBufferOperator::copy() {
    auto copy = std::make_shared<LogicalSortBufferOperator>(sortFieldIdentifier, sortOrder, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    // copy->setStatisticId(statisticId);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalSortBufferOperator::inferStringSignature() {
    OperatorPtr operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalSortBufferOperator: Inferring String signature for {}", *operatorNode);
    NES_ASSERT(!children.empty(), "LogicalSortBufferOperator: SortBuffer should have children");

    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "SORTBUFFER(" + sortFieldIdentifier + ", " + sortOrder + ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}// namespace NES