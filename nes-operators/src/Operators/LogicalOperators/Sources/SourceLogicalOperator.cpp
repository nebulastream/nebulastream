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

SourceLogicalOperator::SourceLogicalOperator(std::string logicalSourceName, OperatorId id, OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), logicalSourceName(std::move(logicalSourceName))
{
}

SourceLogicalOperator::SourceLogicalOperator(std::string logicalSourceName, SchemaPtr schema, OperatorId id, OriginId originId)
    : Operator(id)
    , LogicalUnaryOperator(id)
    , OriginIdAssignmentOperator(id, originId)
    , logicalSourceName(std::move(logicalSourceName))
    , schema(std::move(schema))
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
        auto rhsAsSourceLogicalOperator = rhs->as<SourceLogicalOperator>();
        return this->getSchema() == rhsAsSourceLogicalOperator->getSchema()
            && this->getLogicalSourceName() == rhsAsSourceLogicalOperator->getLogicalSourceName();
    }
    return false;
}

std::string SourceLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "LogicalSource(opId: " << id << ": originid: " << originId << ")";

    return ss.str();
}

bool SourceLogicalOperator::inferSchema()
{
    inputSchema = schema;
    outputSchema = schema;
    return true;
}

OperatorPtr SourceLogicalOperator::copy()
{
    auto copy = LogicalOperatorFactory::createSourceOperator(logicalSourceName, id, originId);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    for (const auto& pair : properties)
    {
        copy->addProperty(pair.first, pair.second);
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
std::string SourceLogicalOperator::getLogicalSourceName() const
{
    return logicalSourceName;
}
std::shared_ptr<Schema> SourceLogicalOperator::getSchema() const
{
    return schema;
}
void SourceLogicalOperator::setSchema(std::shared_ptr<Schema> schema)
{
    this->schema = std::move(schema);
}

} /// namespace NES
