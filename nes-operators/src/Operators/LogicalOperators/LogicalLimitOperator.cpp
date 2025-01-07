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

#include <memory>
#include <utility>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

LogicalLimitOperator::LogicalLimitOperator(uint64_t limit, OperatorId id) : Operator(id), LogicalUnaryOperator(id), limit(limit)
{
}

uint64_t LogicalLimitOperator::getLimit() const
{
    return limit;
}

bool LogicalLimitOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalLimitOperator>(rhs)->getId() == id;
}

bool LogicalLimitOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalLimitOperator>(rhs))
    {
        auto limitOperator = NES::Util::as<LogicalLimitOperator>(rhs);
        return limit == limitOperator->limit;
    }
    return false;
};

std::string LogicalLimitOperator::toString() const
{
    std::stringstream ss;
    ss << "LIMIT" << id << ")";
    return ss.str();
}

bool LogicalLimitOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Operator> LogicalLimitOperator::copy()
{
    auto copy = std::make_shared<LogicalLimitOperator>(limit, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalLimitOperator::inferStringSignature()
{
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalLimitOperator: Inferring String signature for {}", *operatorNode);
    NES_ASSERT(!children.empty(), "LogicalLimitOperator: Limit should have children");

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "LIMIT(" << limit << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
