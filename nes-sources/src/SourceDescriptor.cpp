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
namespace NES::Sources
{

SourceDescriptor::SourceDescriptor(
    std::shared_ptr<Schema> schema,
    std::string logicalSourceName,
    std::string sourceType,
    Configurations::InputFormat inputFormat,
    Configurations::DescriptorConfig::Config&& config)
    : Descriptor(std::move(config))
    , schema(std::move(schema))
    , logicalSourceName(std::move(logicalSourceName))
    , sourceType(std::move(sourceType))
    , inputFormat(std::move(inputFormat))
{
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor)
{
    return out << "SourceDescriptor:"
               << "\nlogical source name: " << sourceDescriptor.logicalSourceName << "\nSource type: " << sourceDescriptor.sourceType
               << "\nSchema: " << ((sourceDescriptor.schema) ? sourceDescriptor.schema->toString() : "NULL")
               << "\nInputformat: " << std::string(magic_enum::enum_name(sourceDescriptor.inputFormat)) << "\nConfig:\n"
               << sourceDescriptor.toStringConfig();
}

bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.schema == rhs.schema && lhs.sourceType == rhs.sourceType && lhs.config == rhs.config;
}

}
