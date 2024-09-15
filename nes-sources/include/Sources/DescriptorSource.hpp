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

#pragma once

#include <ostream>
#include <string>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

struct DescriptorSource : public Configurations::Descriptor
{
    /// Used by Sources to create a valid DescriptorSource.
    explicit DescriptorSource(
        std::shared_ptr<Schema> schema,
        std::string logicalSourceName,
        std::string sourceType,
        Configurations::InputFormat inputFormat,
        Configurations::DescriptorConfig::Config&& config);

    ~DescriptorSource() = default;
    const std::shared_ptr<Schema> schema;
    const std::string logicalSourceName;
    const std::string sourceType;
    const Configurations::InputFormat inputFormat{};

    friend std::ostream& operator<<(std::ostream& out, const DescriptorSource& descriptorSource);
    friend bool operator==(const DescriptorSource& lhs, const DescriptorSource& rhs);
};

}

/// Specializing the fmt ostream_formatter to accept DescriptorSource objects.
/// Allows to call fmt::format("DescriptorSource: {}", DescriptorSourceObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Sources::DescriptorSource> : ostream_formatter
{
};
}
