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

#include <ostream>
#include <sstream>
#include <API/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
namespace NES::Sources
{

SourceDescriptor::SourceDescriptor(
    std::shared_ptr<Schema> schema,
    std::string logicalSourceName,
    std::string sourceType,
    InputFormatterConfig inputFormatterConfig,
    Configurations::DescriptorConfig::Config&& config)
    : Descriptor(std::move(config))
    , schema(std::move(schema))
    , logicalSourceName(std::move(logicalSourceName))
    , sourceType(std::move(sourceType))
    , inputFormatterConfig(std::move(inputFormatterConfig))
{
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor)
{
    const auto schemaString = ((sourceDescriptor.schema) ? sourceDescriptor.schema->toString() : "NULL");
    const auto inputFormatterConfigString = fmt::format(
        "type: {}, tupleDelimiter: {}, stringDelimiter: {}",
        sourceDescriptor.inputFormatterConfig.type,
        sourceDescriptor.inputFormatterConfig.tupleDelimiter,
        sourceDescriptor.inputFormatterConfig.fieldDelimiter);
    return out << fmt::format(
               "SourceDescriptor( logicalSourceName: {}, sourceType: {}, schema: {}, inputFormatterConfig: {}, config: {})",
               sourceDescriptor.logicalSourceName,
               sourceDescriptor.sourceType,
               schemaString,
               inputFormatterConfigString,
               sourceDescriptor.toStringConfig());
}

bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.schema == rhs.schema && lhs.sourceType == rhs.sourceType && lhs.config == rhs.config;
}

}
