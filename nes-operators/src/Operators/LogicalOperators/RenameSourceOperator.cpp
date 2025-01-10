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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

RenameSourceOperator::RenameSourceOperator(const std::string& newSourceName, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), newSourceName(newSourceName)
{
}

bool RenameSourceOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<RenameSourceOperator>(rhs)->getId() == id;
}

bool RenameSourceOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<RenameSourceOperator>(rhs))
    {
        auto otherRename = NES::Util::as<RenameSourceOperator>(rhs);
        return newSourceName == otherRename->newSourceName;
    }
    return false;
};

std::string RenameSourceOperator::toString() const
{
    std::stringstream ss;
    ss << "RENAME_STREAM(" << id << ", newSourceName=" << newSourceName << ")";
    return ss.str();
}

bool RenameSourceOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    ///Update output schema by changing the qualifier and corresponding attribute names
    auto newQualifierName = newSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (const auto& field : *outputSchema)
    {
        ///Extract field name without qualifier
        auto fieldName = field->getName();
        ///Add new qualifier name to the field and update the field name
        field->setName(newQualifierName + fieldName);
    }
    return true;
}

std::string RenameSourceOperator::getNewSourceName() const
{
    return newSourceName;
}

std::shared_ptr<Operator> RenameSourceOperator::copy()
{
    auto copy = std::make_shared<RenameSourceOperator>(newSourceName, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void RenameSourceOperator::inferStringSignature()
{
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("RenameSourceOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "RenameSourceOperator: Rename Source should have children.");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "RENAME_STREAM(newStreamName=" << newSourceName << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
