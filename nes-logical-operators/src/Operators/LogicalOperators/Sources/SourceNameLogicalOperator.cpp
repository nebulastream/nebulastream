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
#include <ostream>
#include <utility>
#include <API/Schema.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName, const OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), logicalSourceName(std::move(logicalSourceName))
{
}

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName, std::shared_ptr<Schema> schema, const OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), logicalSourceName(std::move(logicalSourceName)), schema(std::move(schema))
{
}

bool SourceNameLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const SourceNameLogicalOperator*>(&rhs)->getId() == id;
}

bool SourceNameLogicalOperator::operator==(const Operator& rhs) const
{
    if (auto rhsOperator = dynamic_cast<const SourceNameLogicalOperator*>(&rhs))
    {
        return this->getSchema() == rhsOperator->getSchema() && this->getLogicalSourceName() == rhsOperator->getLogicalSourceName();
    }
    return false;
}

std::ostream& SourceNameLogicalOperator::toDebugString(std::ostream& os) const
{
    return os << fmt::format("SOURCE(opId: {}, name: {})", id, logicalSourceName);
}

std::ostream& SourceNameLogicalOperator::toQueryPlanString(std::ostream& os) const
{
    std::string originIds;
    if (!inputOriginIds.empty())
    {
        originIds = fmt::format(", {}", fmt::join(inputOriginIds.begin(), inputOriginIds.end(), ", "));
    }
    return os << fmt::format("SOURCE({}{})", logicalSourceName, originIds);
}

bool SourceNameLogicalOperator::inferSchema()
{
    inputSchema = schema;
    outputSchema = schema;
    return true;
}

std::shared_ptr<Operator> SourceNameLogicalOperator::clone() const
{
    auto copy = std::make_shared<SourceNameLogicalOperator>(logicalSourceName, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& pair : properties)
    {
        copy->addProperty(pair.first, pair.second);
    }
    return copy;
}

void SourceNameLogicalOperator::inferStringSignature()
{
    ///Update the signature
    throw FunctionNotImplemented("Not supporting infering signatures for SourceNameLogicalOperator.");
}

void SourceNameLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins, so inferInputOrigins is a noop.");
}

std::string SourceNameLogicalOperator::getLogicalSourceName() const
{
    return logicalSourceName;
}
std::shared_ptr<Schema> SourceNameLogicalOperator::getSchema() const
{
    return schema;
}
void SourceNameLogicalOperator::setSchema(std::shared_ptr<Schema> schema)
{
    this->schema = std::move(schema);
}

}
