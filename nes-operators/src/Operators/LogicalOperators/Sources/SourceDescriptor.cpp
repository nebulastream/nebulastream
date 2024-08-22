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

#include <utility>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES
{

SourceDescriptor::SourceDescriptor(SchemaPtr schema)
{
    this->schema = schema->copy();
}

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string logicalSourceName)
    : schema(std::move(schema)), logicalSourceName(std::move(logicalSourceName)), sourceName("INVALID")
{
}

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string logicalSourceName, std::string sourceName)
    : schema(std::move(schema)), logicalSourceName(std::move(logicalSourceName)), sourceName(std::move(sourceName))
{
}

SchemaPtr SourceDescriptor::getSchema() const
{
    return schema;
}

std::string SourceDescriptor::getLogicalSourceName() const
{
    return logicalSourceName;
}

void SourceDescriptor::setSchema(const SchemaPtr& schema)
{
    this->schema = schema;
}

std::string SourceDescriptor::getSourceName() const
{
    return sourceName;
}

void SourceDescriptor::setSourceName(std::string sourceName)
{
    this->sourceName = std::move(sourceName);
}

} /// namespace NES
