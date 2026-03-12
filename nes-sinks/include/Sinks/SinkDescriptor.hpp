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


#include <cctype>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Reflection.hpp>

namespace NES
{
class OperatorSerializationUtil;
class SinkCatalog;
}

namespace NES
{

class SinkDescriptor final : public Descriptor
{
    friend SinkCatalog;
    friend OperatorSerializationUtil;
    friend Unreflector<SinkDescriptor>;

public:
    ~SinkDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor);
    friend bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs);

    /// Will return "Native" as fallback, if the sink config does not use the OUTPUT_FORMAT parameter.
    [[nodiscard]] std::string getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] std::shared_ptr<const Schema> getSchema() const;
    [[nodiscard]] std::string getSinkName() const;
    [[nodiscard]] bool isInline() const;
    [[nodiscard]] std::unordered_map<std::string, std::string> getOutputFormatterConfig() const;

private:
    explicit SinkDescriptor(
        std::variant<std::string, uint64_t> sinkName,
        const Schema& schema,
        std::string_view sinkType,
        const std::unordered_map<std::string, std::string>& formatConfig,
        DescriptorConfig::Config config);

    std::variant<std::string, uint64_t> sinkName;
    std::shared_ptr<const Schema> schema;
    std::string sinkType;
    std::unordered_map<std::string, std::string> formatConfig;

    friend Reflector<SinkDescriptor>;

public:
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(OUTPUT_FORMAT, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<bool> ADD_TIMESTAMP{
        "add_timestamp",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ADD_TIMESTAMP, config); }};


    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(OUTPUT_FORMAT, ADD_TIMESTAMP);

    static std::optional<DescriptorConfig::Config>
    validateAndFormatConfig(std::string_view sinkType, std::unordered_map<std::string, std::string> configPairs);

    friend struct SinkLogicalOperator;
};

template <>
struct Reflector<SinkDescriptor>
{
    Reflected operator()(const SinkDescriptor& descriptor) const;
};

template <>
struct Unreflector<SinkDescriptor>
{
    SinkDescriptor operator()(const Reflected& reflected) const;
};


}

template <>
struct std::hash<NES::SinkDescriptor>
{
    size_t operator()(const NES::SinkDescriptor& sinkDescriptor) const noexcept
    {
        return std::hash<std::string>{}(sinkDescriptor.getSinkName());
    }
};

namespace NES::detail
{
struct ReflectedSinkDescriptor
{
    std::variant<std::string, uint64_t> sinkName;
    Schema schema;
    std::string sinkType;
    std::unordered_map<std::string, std::string> formatConfig;
    Reflected config;
};
}

FMT_OSTREAM(NES::SinkDescriptor);
