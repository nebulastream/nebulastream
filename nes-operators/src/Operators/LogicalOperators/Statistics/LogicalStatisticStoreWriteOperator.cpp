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

#include <Operators/LogicalOperators/Statistics/LogicalStatisticStoreWriteOperator.hpp>

namespace NES
{

LogicalStatisticStoreWriteOperator::LogicalStatisticStoreWriteOperator(OperatorId id) : Operator(id), LogicalUnaryOperator(id)
{
}

bool LogicalStatisticStoreWriteOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalStatisticStoreWriteOperator>(rhs)->getId() == id;
}

bool LogicalStatisticStoreWriteOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalStatisticStoreWriteOperator>(rhs))
    {
        return true;
    }
    return false;
};

std::string LogicalStatisticStoreWriteOperator::toString() const
{
    std::stringstream ss;
    ss << "SSWRITE(" << id << ")";
    return ss.str();
}

bool LogicalStatisticStoreWriteOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Operator> LogicalStatisticStoreWriteOperator::copy()
{
    // TODO(nikla44): check this function
    auto copy = std::make_shared<LogicalStatisticStoreWriteOperator>(id);
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

void LogicalStatisticStoreWriteOperator::inferStringSignature()
{
    // TODO(nikla44): check this function
    NES_TRACE("LogicalStatisticStoreWriteOperator: Inferring String signature for {}", toString());
    INVARIANT(!children.empty(), "StatisticStoreWrite should have children");

    ///Infer query signatures for child operator
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    /// Infer signature for this operator.
    std::stringstream signatureStream;
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "SSWRITE()." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
