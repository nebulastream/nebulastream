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

#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <memory>
#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName)
    : Operator(), UnaryLogicalOperator(), logicalSourceName(std::move(logicalSourceName))
{
}

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName, const Schema& schema)
    : Operator(), UnaryLogicalOperator(), logicalSourceName(std::move(logicalSourceName)), schema(schema)
{
}

bool SourceNameLogicalOperator::isIdentical(Operator const& rhs) const
{
    return *this == rhs && dynamic_cast<const SourceNameLogicalOperator*>(&rhs)->id == id;
}

bool SourceNameLogicalOperator::operator==(Operator const& rhs) const
{
    if (auto rhsOperator = dynamic_cast<const SourceNameLogicalOperator*>(&rhs)) {
        return this->getSchema() == rhsOperator->getSchema()
            && this->getName() == rhsOperator->getName();
    }
    return false;
}

std::string SourceNameLogicalOperator::toString() const
{
    return fmt::format("SOURCE(opId: {}, name: {})", id, logicalSourceName);
}

bool SourceNameLogicalOperator::inferSchema()
{
    inputSchema = schema;
    outputSchema = schema;
    return true;
}

std::unique_ptr<Operator> SourceNameLogicalOperator::clone() const
{
    auto copy = std::make_unique<SourceNameLogicalOperator>(logicalSourceName);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}


void SourceNameLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins, so inferInputOrigins is a noop.");
}

std::string_view SourceNameLogicalOperator::getName() const noexcept
{
    return logicalSourceName;
}

Schema SourceNameLogicalOperator::getSchema() const
{
    return schema;
}
void SourceNameLogicalOperator::setSchema(const Schema& schema)
{
    this->schema = schema;
}

SerializableOperator SourceNameLogicalOperator::serialize() const
{
    PRECONDITION(false, "no serialize for SourceNameLogicalOperator defined");
}

}
