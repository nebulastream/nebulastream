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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>

namespace NES
{

CSVSourceDescriptor::CSVSourceDescriptor(SchemaPtr schema, CSVSourceTypePtr sourceConfig, const std::string& logicalSourceName)
    : SourceDescriptor(std::move(schema), logicalSourceName), csvSourceType(std::move(sourceConfig))
{
}

SourceDescriptorPtr CSVSourceDescriptor::create(SchemaPtr schema, CSVSourceTypePtr csvSourceType, const std::string& logicalSourceName)
{
    return std::make_shared<CSVSourceDescriptor>(CSVSourceDescriptor(std::move(schema), std::move(csvSourceType), logicalSourceName));
}

SourceDescriptorPtr CSVSourceDescriptor::create(SchemaPtr schema, CSVSourceTypePtr csvSourceType)
{
    return std::make_shared<CSVSourceDescriptor>(CSVSourceDescriptor(std::move(schema), std::move(csvSourceType), ""));
}

CSVSourceTypePtr CSVSourceDescriptor::getSourceConfig() const
{
    return csvSourceType;
}

bool CSVSourceDescriptor::equal(SourceDescriptorPtr const& other) const
{
    if (!other->instanceOf<CSVSourceDescriptor>())
    {
        return false;
    }
    auto otherSource = other->as<CSVSourceDescriptor>();
    return csvSourceType->equal(otherSource->getSourceConfig());
}

std::string CSVSourceDescriptor::toString() const
{
    return "CsvSourceDescriptor(" + csvSourceType->toString() + ")";
}

SourceDescriptorPtr CSVSourceDescriptor::copy()
{
    auto copy = CSVSourceDescriptor::create(schema->copy(), csvSourceType, logicalSourceName);
    return copy;
}

} /// namespace NES
