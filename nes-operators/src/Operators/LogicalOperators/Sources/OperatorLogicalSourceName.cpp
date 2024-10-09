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
#include <Operators/LogicalOperators/Sources/OperatorLogicalSourceName.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

OperatorLogicalSourceName::OperatorLogicalSourceName(std::string logicalSourceName, OperatorId id, OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), logicalSourceName(std::move(logicalSourceName))
{
}

OperatorLogicalSourceName::OperatorLogicalSourceName(std::string logicalSourceName, SchemaPtr schema, OperatorId id, OriginId originId)
    : Operator(id)
    , LogicalUnaryOperator(id)
    , OriginIdAssignmentOperator(id, originId)
    , logicalSourceName(std::move(logicalSourceName))
    , schema(std::move(schema))
{
}

bool OperatorLogicalSourceName::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<OperatorLogicalSourceName>(rhs)->getId() == id;
}

bool OperatorLogicalSourceName::equal(NodePtr const& rhs) const
{
    if (Util::instanceOf<OperatorLogicalSourceName>(rhs))
    {
        auto rhsAsOperatorLogicalSourceName = Util::as<OperatorLogicalSourceName>(rhs);
        return this->getSchema() == rhsAsOperatorLogicalSourceName->getSchema()
            && this->getLogicalSourceName() == rhsAsOperatorLogicalSourceName->getLogicalSourceName();
    }
    return false;
}

std::string OperatorLogicalSourceName::toString() const
{
    std::stringstream ss;
    ss << "LogicalSource(opId: " << id << ": originid: " << originId << ")";

    return ss.str();
}

bool OperatorLogicalSourceName::inferSchema()
{
    inputSchema = schema;
    outputSchema = schema;
    return true;
}

OperatorPtr OperatorLogicalSourceName::copy()
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

void OperatorLogicalSourceName::inferStringSignature()
{
    ///Update the signature
    throw FunctionNotImplemented("Not supporting 'inferStringSignature' for OperatorLogicalSourceName.");
}

void OperatorLogicalSourceName::inferInputOrigins()
{
    /// Data sources have no input origins.
}

std::vector<OriginId> OperatorLogicalSourceName::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}
std::string OperatorLogicalSourceName::getLogicalSourceName() const
{
    return logicalSourceName;
}
std::shared_ptr<Schema> OperatorLogicalSourceName::getSchema() const
{
    return schema;
}
void OperatorLogicalSourceName::setSchema(std::shared_ptr<Schema> schema)
{
    this->schema = std::move(schema);
}

} /// namespace NES
