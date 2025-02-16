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

#include <string>
#include <Operators/LogicalOperators/LogicalShuffleTuplesOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

LogicalShuffleTuplesOperator::LogicalShuffleTuplesOperator(OperatorId id) : Operator(id), LogicalUnaryOperator(id)
{
}

bool LogicalShuffleTuplesOperator::isIdentical(std::shared_ptr<Node> const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalShuffleTuplesOperator>(rhs)->getId() == id;
}

bool LogicalShuffleTuplesOperator::equal(std::shared_ptr<Node> const& rhs) const
{
    if (NES::Util::instanceOf<LogicalShuffleTuplesOperator>(rhs))
    {
        auto DelayTuplesOperator = NES::Util::as<LogicalShuffleTuplesOperator>(rhs);
        return true;
    }
    return false;
};

std::string LogicalShuffleTuplesOperator::toString() const
{
    std::stringstream ss;
    ss << "DelayTuples(opId: " << id << ")";
    return ss.str();
}

bool LogicalShuffleTuplesOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Operator> LogicalShuffleTuplesOperator::copy()
{
    auto copy = std::make_shared<LogicalShuffleTuplesOperator>(id);
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

void LogicalShuffleTuplesOperator::inferStringSignature()
{
    auto operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalShuffleTuplesOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "LogicalShuffleTuplesOperator: DelayTuples should have children");

    //Infer query signatures for child operators
    for (const auto& child : children)
    {
        const auto childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "DelayTuples()." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

} // namespace NES