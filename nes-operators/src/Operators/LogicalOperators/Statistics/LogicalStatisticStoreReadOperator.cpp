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

#include <Operators/LogicalOperators/Statistics/LogicalStatisticStoreReadOperator.hpp>

namespace NES
{

LogicalStatisticStoreReadOperator::LogicalStatisticStoreReadOperator(OperatorId id) : Operator(id), LogicalUnaryOperator(id)
{
}

bool LogicalStatisticStoreReadOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalStatisticStoreReadOperator>(rhs)->getId() == id;
}

bool LogicalStatisticStoreReadOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalStatisticStoreReadOperator>(rhs))
    {
        return true;
    }
    return false;
};

std::ostream& LogicalStatisticStoreReadOperator::toDebugString(std::ostream& os) const
{
    return os << "SSREAD(" << id << ")";
}

std::ostream& LogicalStatisticStoreReadOperator::toQueryPlanString(std::ostream& os) const
{
    return os << "SSREAD";
}

bool LogicalStatisticStoreReadOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Operator> LogicalStatisticStoreReadOperator::copy()
{
    // TODO(nikla44): check this function
    auto copy = std::make_shared<LogicalStatisticStoreReadOperator>(id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalStatisticStoreReadOperator::inferStringSignature()
{
    // TODO(nikla44): check this function
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalStatisticStoreReadOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "StatisticStoreRead should have children");

    ///Infer query signatures for child operator
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    /// Infer signature for this operator.
    std::stringstream signatureStream;
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "SSREAD()." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
