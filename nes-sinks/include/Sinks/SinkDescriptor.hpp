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
#include <Util/ReflectionFwd.hpp>

namespace NES
{
class OperatorSerializationUtil;
class SinkCatalog;
}

namespace NES
{

enum class InputFormat : uint8_t
{
    CSV,
    JSON
};

class SinkDescriptor final : public Descriptor
{
    friend SinkCatalog;
    friend OperatorSerializationUtil;
    friend Unreflector<SinkDescriptor>;

public:
    ~SinkDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor);
    friend bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs);

    /// Optional, since not every sink type uses an INPUT_FORMAT parameter.
    [[nodiscard]] std::optional<std::string_view> getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] std::shared_ptr<const Schema> getSchema() const;
    [[nodiscard]] std::string getSinkName() const;
    [[nodiscard]] bool isInline() const;

private:
    explicit SinkDescriptor(
        std::variant<std::string, uint64_t> sinkName, const Schema& schema, std::string_view sinkType, DescriptorConfig::Config config);

    std::variant<std::string, uint64_t> sinkName;
    std::shared_ptr<const Schema> schema;
    std::string sinkType;

    friend Reflector<SinkDescriptor>;

public:
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormat> INPUT_FORMAT{
        "input_format",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FORMAT, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<bool> ADD_TIMESTAMP{
        "add_timestamp",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ADD_TIMESTAMP, config); }};


    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(INPUT_FORMAT, ADD_TIMESTAMP);

    /// Well-known property for any sink that sends its data to a file
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

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
    SinkDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
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
    Reflected config;
};
}

FMT_OSTREAM(NES::SinkDescriptor);
