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
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(
    std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor, const OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), sourceDescriptor(std::move(sourceDescriptor))
{
}
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(
    std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor, const OperatorId id, const OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), sourceDescriptor(std::move(sourceDescriptor))
{
}

bool SourceDescriptorLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<const SourceDescriptorLogicalOperator>(rhs)->getId() == id;
}

bool SourceDescriptorLogicalOperator::equal(NodePtr const& rhs) const
{
    if (Util::instanceOf<SourceDescriptorLogicalOperator>(rhs))
    {
        const auto sourceOperator = Util::as<SourceDescriptorLogicalOperator>(rhs);
        return sourceOperator->getSourceDescriptorRef() == *sourceDescriptor;
    }
    return false;
}

std::string SourceDescriptorLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ": originid: " << originId << ", " << sourceDescriptor << ")";

    return ss.str();
}

const Sources::SourceDescriptor& SourceDescriptorLogicalOperator::getSourceDescriptorRef() const
{
    return *sourceDescriptor;
}

bool SourceDescriptorLogicalOperator::inferSchema()
{
    inputSchema = sourceDescriptor->schema;
    outputSchema = sourceDescriptor->schema;
    return true;
}

OperatorPtr SourceDescriptorLogicalOperator::copy()
{
    auto sourceDescriptorPtrCopy = sourceDescriptor;
    auto result = std::make_shared<SourceDescriptorLogicalOperator>(std::move(sourceDescriptorPtrCopy), id);
    result->setOriginId(getOriginId());
    result->inferSchema();
    result->addAllProperties(properties);
    return result;
}

void SourceDescriptorLogicalOperator::inferStringSignature()
{
    ///Update the signature
    throw FunctionNotImplemented("Not supporting 'inferStringSignature' for SourceDescriptorLogicalOperator.");
}

void SourceDescriptorLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins. Therefore, we do not infer any input origins!");
}
std::vector<OriginId> SourceDescriptorLogicalOperator::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

}
