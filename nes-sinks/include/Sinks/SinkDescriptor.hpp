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
#include <unordered_map>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableOperator.pb.h>

namespace NES::Sinks
{

struct SinkDescriptor final : NES::Configurations::Descriptor
{
    explicit SinkDescriptor(std::string sinkType, NES::Configurations::DescriptorConfig::Config&& config, bool addTimestamp);
    explicit SinkDescriptor();
    ~SinkDescriptor() = default;

    /// Iterates over all config pairs to create a DescriptorConfig::Config containing only strings.
    static NES::Configurations::DescriptorConfig::Config
    validateAndFormatConfig(const std::string& sinkType, std::unordered_map<std::string, std::string> configPairs);

    [[nodiscard]] NES::SerializableSinkDescriptor serialize() const;

    const std::string sinkType;
    Schema schema;
    bool addTimestamp;

    friend std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor);
    friend bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs);
};
}
FMT_OSTREAM(NES::Sinks::SinkDescriptor);
