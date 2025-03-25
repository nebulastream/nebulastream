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
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>

namespace NES::Sources
{

SourceDescriptor::SourceDescriptor(
    std::shared_ptr<Schema> schema,
    std::string logicalSourceName,
    std::string sourceType,
    const int numberOfBuffersInSourceLocalBufferPool,
    ParserConfig parserConfig,
    Configurations::DescriptorConfig::Config&& config)
    : Descriptor(std::move(config))
    , schema(std::move(schema))
    , logicalSourceName(std::move(logicalSourceName))
    , sourceType(std::move(sourceType))
    , numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool)
    , parserConfig(std::move(parserConfig))
{
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor)
{
    const auto schemaString = ((sourceDescriptor.schema) ? sourceDescriptor.schema->toString() : "NULL");
    const auto parserConfigString = fmt::format(
        "type: {}, tupleDelimiter: '{}', stringDelimiter: '{}'",
        sourceDescriptor.parserConfig.parserType,
        Util::escapeSpecialCharacters(sourceDescriptor.parserConfig.tupleDelimiter),
        Util::escapeSpecialCharacters(sourceDescriptor.parserConfig.fieldDelimiter));
    return out << fmt::format(
               "SourceDescriptor( logicalSourceName: {}, sourceType: {}, schema: {}, parserConfig: {}, config: {})",
               sourceDescriptor.logicalSourceName,
               sourceDescriptor.sourceType,
               schemaString,
               parserConfigString,
               sourceDescriptor.toStringConfig());
}

bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.schema == rhs.schema && lhs.sourceType == rhs.sourceType && lhs.config == rhs.config;
}

}
