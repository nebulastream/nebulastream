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
#include <string_view>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableOperator.pb.h>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName)
    : logicalSourceName(std::move(logicalSourceName))
{
}

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName, const Schema& schema)
    : logicalSourceName(std::move(logicalSourceName)), schema(std::move(schema))
{
}

bool SourceNameLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (auto* rhsOperator = dynamic_cast<const SourceNameLogicalOperator*>(&rhs)) {
        return this->getSchema() == rhsOperator->getSchema()
            && this->getName() == rhsOperator->getName();
    }
    return false;

}

LogicalOperator SourceNameLogicalOperator::withInferredSchema(Schema) const
{
    auto copy = *this;
    copy.inputSchema = schema;
    copy.outputSchema = schema;
    return copy;
}

std::string SourceNameLogicalOperator::toString() const
{
    return fmt::format("SOURCE(opId: {}, name: {})", id, logicalSourceName);
}

void SourceNameLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins, so inferInputOrigins is a noop.");
}

std::string_view SourceNameLogicalOperator::getName() const noexcept
{
    return "Source";
}

Schema SourceNameLogicalOperator::getSchema() const
{
    return schema;
}
void SourceNameLogicalOperator::setSchema(const Schema& schema)
{
    this->schema = schema;
}

Optimizer::TraitSet SourceNameLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator SourceNameLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SourceNameLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SourceNameLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> SourceNameLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> SourceNameLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void SourceNameLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void SourceNameLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> SourceNameLogicalOperator::getChildren() const
{
    return children;
}

std::string SourceNameLogicalOperator::getLogicalSourceName() const
{
        return logicalSourceName;
}

SerializableOperator SourceNameLogicalOperator::serialize() const
{
    PRECONDITION(false, "no serialize for SourceNameLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
}

}
