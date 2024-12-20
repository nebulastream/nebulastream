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
#include <Operators/LogicalOperators/LogicalDelayTuplesOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

LogicalDelayTuplesOperator::LogicalDelayTuplesOperator(OperatorId id) : Operator(id), LogicalUnaryOperator(id)
{
}

bool LogicalDelayTuplesOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalDelayTuplesOperator>(rhs)->getId() == id;
}

bool LogicalDelayTuplesOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalDelayTuplesOperator>(rhs))
    {
        auto DelayTuplesOperator = NES::Util::as<LogicalDelayTuplesOperator>(rhs);
        return true;
    }
    return false;
};

std::string LogicalDelayTuplesOperator::toString() const
{
    std::stringstream ss;
    ss << "DelayTuples(opId: " << id << ")";
    return ss.str();
}

bool LogicalDelayTuplesOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    return true;
}

OperatorPtr LogicalDelayTuplesOperator::copy()
{
    auto copy = std::make_shared<LogicalDelayTuplesOperator>(id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalDelayTuplesOperator::inferStringSignature()
{
    OperatorPtr operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalDelayTuplesOperator: Inferring String signature for {}", *operatorNode);
    NES_ASSERT(!children.empty(), "LogicalDelayTuplesOperator: DelayTuples should have children");

    //Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
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