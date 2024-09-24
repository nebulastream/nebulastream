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
#include <Operators/LogicalOperators/Sources/OperatorLogicalSourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

OperatorLogicalSourceDescriptor::OperatorLogicalSourceDescriptor(
    std::unique_ptr<Sources::DescriptorSource>&& DescriptorSource, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), descriptorSource(std::move(DescriptorSource))
{
}

OperatorLogicalSourceDescriptor::OperatorLogicalSourceDescriptor(
    std::unique_ptr<Sources::DescriptorSource>&& DescriptorSource, OperatorId id, OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), descriptorSource(std::move(DescriptorSource))
{
}

bool OperatorLogicalSourceDescriptor::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && rhs->as<OperatorLogicalSourceDescriptor>()->getId() == id;
}

bool OperatorLogicalSourceDescriptor::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<OperatorLogicalSourceDescriptor>())
    {
        const auto sourceOperator = rhs->as<OperatorLogicalSourceDescriptor>();
        return *this->descriptorSource == sourceOperator->getDescriptorSourceRef();
    }
    return false;
}

std::string OperatorLogicalSourceDescriptor::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ": originid: " << originId << ", " << descriptorSource << ")";

    return ss.str();
}

const Sources::DescriptorSource& OperatorLogicalSourceDescriptor::getDescriptorSourceRef() const
{
    return *descriptorSource;
}

bool OperatorLogicalSourceDescriptor::inferSchema()
{
    inputSchema = descriptorSource->schema;
    outputSchema = descriptorSource->schema;
    return true;
}

OperatorPtr OperatorLogicalSourceDescriptor::copy()
{
    auto exception = InvalidUseOfOperatorFunction(
        "OperatorLogicalSourceDescriptor does not support copy, because holds a unique pointer to a DescriptorSource.");
    throw exception;
}

void OperatorLogicalSourceDescriptor::inferStringSignature()
{
    ///Update the signature
    auto exception = FunctionNotImplemented();
    exception.what() += "Not supporting 'inferStringSignature' for OperatorLogicalSourceDescriptor.";
    throw exception;
}

void OperatorLogicalSourceDescriptor::inferInputOrigins()
{
    /// Data sources have no input origins.
}

std::vector<OriginId> OperatorLogicalSourceDescriptor::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

}
