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


#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{
class SerializableQueryPlan;
class OperatorSerializationUtil;
class SinkCatalog;
namespace IntegrationTestUtil
{
};

}

namespace NES::Sinks
{

class SinkDescriptor final : public NES::Configurations::Descriptor
{
    friend SinkCatalog;
    friend OperatorSerializationUtil;

public:
    ~SinkDescriptor() = default;

    [[nodiscard]] SerializableSinkDescriptor serialize() const;
    friend std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor);
    friend bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs);

    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] std::shared_ptr<const Schema> getSchema() const;
    [[nodiscard]] std::string getSinkName() const;

private:
    explicit SinkDescriptor(
        std::string sinkName,
        std::shared_ptr<const Schema>&& schema,
        std::string_view sinkType,
        NES::Configurations::DescriptorConfig::Config&& config);


    std::string sinkName;
    std::shared_ptr<const Schema> schema;
    std::string sinkType;

public:
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<bool> ADD_TIMESTAMP{
        "addTimestamp",
        false,
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(ADD_TIMESTAMP, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<uint64_t> LOCATION{
        "location",
        INVALID<WorkerId>.getRawValue(),
        [](const std::unordered_map<std::string, std::string>& config)
        {
            if (config.contains(LOCATION.name) && config.at(LOCATION.name) == "local")
            {
                return std::optional{INITIAL<WorkerId>.getRawValue()};
            }
            return NES::Configurations::DescriptorConfig::tryGet(LOCATION, config);
        }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(ADD_TIMESTAMP, LOCATION);

    /// Well-known property for any sink that sends its data to a file
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "filePath",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(FILE_PATH, config); }};

    static std::optional<NES::Configurations::DescriptorConfig::Config>
    validateAndFormatConfig(std::string_view sinkType, std::unordered_map<std::string, std::string> configPairs);
};
}

FMT_OSTREAM(NES::Sinks::SinkDescriptor);
