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
#include <ostream>
#include <utility>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
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

bool LogicalLimitOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalLimitOperator>(rhs)->getId() == id;
}

bool LogicalLimitOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalLimitOperator>(rhs))
    {
        const auto limitOperator = NES::Util::as<LogicalLimitOperator>(rhs);
        return limit == limitOperator->limit;
    }
    return false;
};

std::ostream& LogicalLimitOperator::toDebugString(std::ostream& os) const
{
    return os << "LIMIT(" << id << ")";
}

std::ostream& LogicalLimitOperator::toQueryPlanString(std::ostream& os) const
{
    return os << "LIMIT";
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
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalLimitOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalLimitOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "Limit should have children");

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "LIMIT(" << limit << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
