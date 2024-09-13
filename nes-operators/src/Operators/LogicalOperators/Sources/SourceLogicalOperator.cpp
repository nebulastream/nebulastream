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
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SourceLogicalOperator::SourceLogicalOperator(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id), sourceDescriptor(std::move(sourceDescriptor))
{
}

SourceLogicalOperator::SourceLogicalOperator(
    std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor, OperatorId id, OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), sourceDescriptor(std::move(sourceDescriptor))
{
}

bool SourceLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && rhs->as<SourceLogicalOperator>()->getId() == id;
}

bool SourceLogicalOperator::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<SourceLogicalOperator>())
    {
        auto sourceOperator = rhs->as<SourceLogicalOperator>();
        return sourceOperator->getSourceDescriptor()->equal(*sourceDescriptor);
    }
    return false;
}

std::string SourceLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ": originid: " << originId;
    if (sourceDescriptor)
    {
        ss << ", " << sourceDescriptor->getLogicalSourceName() << "," << sourceDescriptor->toString();
    }
    ss << ")";

    return ss.str();
}

std::unique_ptr<Sources::SourceDescriptor> SourceLogicalOperator::getSourceDescriptor()
{
    return std::move(this->sourceDescriptor);
}
Sources::SourceDescriptor& SourceLogicalOperator::getSourceDescriptorRef()
{
    return *sourceDescriptor;
}

bool SourceLogicalOperator::inferSchema()
{
    inputSchema = sourceDescriptor->getSchema();
    outputSchema = sourceDescriptor->getSchema();
    return true;
}

void SourceLogicalOperator::setSourceDescriptor(std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
{
    this->sourceDescriptor = std::move(sourceDescriptor);
}

void SourceLogicalOperator::setProjectSchema(SchemaPtr schema)
{
    projectSchema = std::move(schema);
}

OperatorPtr SourceLogicalOperator::copy()
{
    throw InvalidUseOfOperatorFunction(
        "SourceLogicalOperator does not support copy, because holds a unique pointer to a SourceDescriptor.");
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

} /// namespace NES
