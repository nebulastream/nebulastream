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
    : SourceDescriptor(std::move(schema), logicalSourceName, PLUGIN_NAME_CSV), csvSourceType(std::move(sourceConfig))
{
}

std::unique_ptr<SourceDescriptor>
CSVSourceDescriptor::create(SchemaPtr schema, CSVSourceTypePtr csvSourceType, const std::string& logicalSourceName)
{
    return std::make_unique<CSVSourceDescriptor>(CSVSourceDescriptor(std::move(schema), std::move(csvSourceType), logicalSourceName));
}

std::unique_ptr<SourceDescriptor> CSVSourceDescriptor::create(SchemaPtr schema, CSVSourceTypePtr csvSourceType)
{
    return std::make_unique<CSVSourceDescriptor>(CSVSourceDescriptor(std::move(schema), std::move(csvSourceType), ""));
}

CSVSourceTypePtr CSVSourceDescriptor::getSourceConfig() const
{
    return csvSourceType;
}

bool CSVSourceDescriptor::equal(SourceDescriptor& other) const
{
    if (!dynamic_cast<CSVSourceDescriptor*>(&other))
    {
        return false;
    }
    const auto otherLogicalSourceName = dynamic_cast<CSVSourceDescriptor*>(&other)->getLogicalSourceName();
    return getLogicalSourceName() == otherLogicalSourceName;
}

std::string CSVSourceDescriptor::toString() const
{
    return "CsvSourceDescriptor(" + csvSourceType->toString() + ")";
}

} /// namespace NES
