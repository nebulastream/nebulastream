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

#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SourceLogicalOperator::SourceLogicalOperator(std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), sourceDescriptor(std::move(sourceDescriptor))
{
}

SourceLogicalOperator::SourceLogicalOperator(
    std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id, OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), sourceDescriptor(std::move(sourceDescriptor))
{
}

bool SourceLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<SourceLogicalOperator>(rhs)->getId() == id;
}

bool SourceLogicalOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<SourceLogicalOperator>(rhs))
    {
        auto sourceOperator = NES::Util::as<SourceLogicalOperator>(rhs);
        return sourceOperator->getSourceDescriptorRef() == *sourceDescriptor;
    }
    return false;
}

std::string SourceLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ": originid: " << originId;
    if (sourceDescriptor)
    {
        ss << ", " << sourceDescriptor->logicalSourceName << "," << *sourceDescriptor;
    }
    ss << ")";

    return ss.str();
}

const Sources::SourceDescriptor& SourceLogicalOperator::getSourceDescriptorRef() const
{
    return *sourceDescriptor;
}

bool SourceLogicalOperator::inferSchema()
{
    inputSchema = sourceDescriptor->schema;
    outputSchema = sourceDescriptor->schema;
    return true;
}

void SourceLogicalOperator::setSourceDescriptor(std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
{
    this->sourceDescriptor = std::move(sourceDescriptor);
}

OperatorPtr SourceLogicalOperator::copy()
{
    auto descriptorPtrCopy = this->sourceDescriptor;
    auto copy = std::make_shared<SourceLogicalOperator>(std::move(descriptorPtrCopy), id, this->getOriginId());
    copy->setOriginId(this->getOriginId());
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

void SourceLogicalOperator::inferStringSignature()
{
    ///Update the signature
    throw FunctionNotImplemented("Not supporting 'inferStringSignature' for SourceLogicalOperator.");
}

void SourceLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
}

std::vector<OriginId> SourceLogicalOperator::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

}
