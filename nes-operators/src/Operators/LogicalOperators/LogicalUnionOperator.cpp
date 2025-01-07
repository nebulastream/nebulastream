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
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES
{

LogicalUnionOperator::LogicalUnionOperator(OperatorId id) : Operator(id), LogicalBinaryOperator(id)
{
}

bool LogicalUnionOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalUnionOperator>(rhs)->getId() == id;
}

std::string LogicalUnionOperator::toString() const
{
    std::stringstream ss;
    if (properties.contains("PINNED_WORKER_ID"))
    {
        ss << "unionWith(" << id << " PINNED, STATE = " << magic_enum::enum_name(operatorState) << " )";
    }
    else
    {
        ss << "unionWith(" << id << ")";
    }
    return ss.str();
}

bool LogicalUnionOperator::inferSchema()
{
    if (!LogicalBinaryOperator::inferSchema())
    {
        return false;
    }

    leftInputSchema->clear();
    rightInputSchema->clear();
    if (distinctSchemas.size() == 1)
    {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[0]);
    }
    else
    {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[1]);
    }

    if (!(*leftInputSchema == *rightInputSchema))
    {
        throw CannotInferSchema(
            "Found Schema mismatch for left and right schema types. Left schema {} and Right schema {}",
            leftInputSchema->toString(),
            rightInputSchema->toString());
    }

    if (leftInputSchema->getLayoutType() != rightInputSchema->getLayoutType())
    {
        throw CannotInferSchema("Left and right should have same memory layout");
    }

    ///Copy the schema of left input
    outputSchema->clear();
    outputSchema->copyFields(leftInputSchema);
    outputSchema->setLayoutType(leftInputSchema->getLayoutType());
    return true;
}

std::shared_ptr<Operator> LogicalUnionOperator::copy()
{
    auto copy = std::make_shared<LogicalUnionOperator>(id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOutputSchema(outputSchema);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalUnionOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalUnionOperator>(rhs))
    {
        auto rhsUnion = NES::Util::as<LogicalUnionOperator>(rhs);
        return (*leftInputSchema == *rhsUnion->getLeftInputSchema()) && (*outputSchema == *rhsUnion->getOutputSchema());
    }
    return false;
}

void LogicalUnionOperator::inferStringSignature()
{
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalUnionOperator: Inferring String signature for {}", *operatorNode);
    NES_ASSERT(!children.empty() && children.size() == 2, "LogicalUnionOperator: Union should have 2 children.");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        NES::Util::as<LogicalOperator>(child)->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "UNION(";
    auto rightChildSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    auto leftChildSignature = NES::Util::as<LogicalOperator>(children[1])->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

void LogicalUnionOperator::inferInputOrigins()
{
    /// in the default case we collect all input origins from the children/upstream operators
    std::vector<OriginId> combinedInputOriginIds;
    for (auto child : this->children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        combinedInputOriginIds.insert(combinedInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->leftInputOriginIds = combinedInputOriginIds;
}

}
