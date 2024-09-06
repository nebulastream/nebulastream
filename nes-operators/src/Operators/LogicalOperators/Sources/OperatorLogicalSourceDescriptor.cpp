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
    std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), sourceDescriptor(std::move(sourceDescriptor))
{
}

OperatorLogicalSourceDescriptor::OperatorLogicalSourceDescriptor(
    std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id, OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), sourceDescriptor(std::move(sourceDescriptor))
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
        return sourceOperator->getSourceDescriptorRef() == *sourceDescriptor;
    }
    return false;
}

std::string OperatorLogicalSourceDescriptor::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ": originid: " << originId << ", " << sourceDescriptor << ")";

    return ss.str();
}

const Sources::SourceDescriptor& OperatorLogicalSourceDescriptor::getSourceDescriptorRef() const
{
    return *sourceDescriptor;
}

bool OperatorLogicalSourceDescriptor::inferSchema()
{
    inputSchema = sourceDescriptor->schema;
    outputSchema = sourceDescriptor->schema;
    return true;
}

void OperatorLogicalSourceDescriptor::setSourceDescriptor(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
{
    this->sourceDescriptor = std::move(sourceDescriptor);
}

OperatorPtr OperatorLogicalSourceDescriptor::copy()
{
    auto exception = InvalidUseOfOperatorFunction(
        "OperatorLogicalSourceDescriptor does not support copy, because holds a unique pointer to a SourceDescriptor.");
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

} /// namespace NES
