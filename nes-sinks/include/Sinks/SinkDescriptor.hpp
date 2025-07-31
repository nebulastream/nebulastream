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
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
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

class NamedSinkDescriptor final : public Descriptor
{
    friend SinkCatalog;
    friend OperatorSerializationUtil;

public:
    ~NamedSinkDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const NamedSinkDescriptor& sinkDescriptor);
    friend bool operator==(const NamedSinkDescriptor& lhs, const NamedSinkDescriptor& rhs);

    [[nodiscard]] std::optional<std::string_view> getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> getSchema() const;
    [[nodiscard]] Identifier getSinkName() const;

private:
    explicit NamedSinkDescriptor(
        Identifier name,
        Schema<UnqualifiedUnboundField, Ordered> nameWithSchema,
        std::string_view sinkType,
        DescriptorConfig::Config config);

    Identifier name;
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> schema;
    std::string sinkType;

    friend Unreflector<NamedSinkDescriptor>;
};

class InlineSinkDescriptor final : public Descriptor
{
    friend SinkCatalog;
    friend OperatorSerializationUtil;
    friend struct SinkLogicalOperator;
    friend class CalcTargetOrderPhase;

public:
    ~InlineSinkDescriptor() = default;


    friend std::ostream& operator<<(std::ostream& out, const InlineSinkDescriptor& sinkDescriptor);
    friend bool operator==(const InlineSinkDescriptor& lhs, const InlineSinkDescriptor& rhs);

    [[nodiscard]] std::optional<std::string_view> getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] std::variant<
        std::monostate,
        std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
        std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>
    getSchema() const;
    [[nodiscard]] uint64_t getSinkId() const;

private:
    explicit InlineSinkDescriptor(
        uint64_t sinkId,
        std::variant<std::monostate, Schema<UnqualifiedUnboundField, Unordered>, Schema<UnqualifiedUnboundField, Ordered>> schema,
        std::string_view sinkType,
        DescriptorConfig::Config config);

    InlineSinkDescriptor withSchemaOrder(const Schema<UnqualifiedUnboundField, Ordered>& newSchema) const;

    uint64_t sinkId;
    std::variant<
        std::monostate,
        std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
        std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>
        schema;
    std::string sinkType;

    friend Unreflector<InlineSinkDescriptor>;
};

class SinkDescriptor final : public Descriptor
{
    friend SinkCatalog;
    friend OperatorSerializationUtil;
    friend Unreflector<SinkDescriptor>;
    friend class CalcTargetOrderPhase;

public:
    ~SinkDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor);
    friend bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs);

    /// Optional, since not every sink type uses an INPUT_FORMAT parameter.
    [[nodiscard]] std::optional<std::string_view> getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]]
    std::variant<
        std::monostate,
        std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
        std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>> getSchema() const;
    [[nodiscard]] Identifier getSinkName() const;
    [[nodiscard]] bool isInline() const;
    [[nodiscard]] const std::variant<NamedSinkDescriptor, InlineSinkDescriptor>& getUnderlying() const;

private:
    explicit SinkDescriptor(std::variant<NamedSinkDescriptor, InlineSinkDescriptor> underlying);
    std::variant<NamedSinkDescriptor, InlineSinkDescriptor> underlying;

    friend Reflector<SinkDescriptor>;

public:
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormat> INPUT_FORMAT{
        "INPUT_FORMAT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FORMAT, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<bool> ADD_TIMESTAMP{
        "ADD_TIMESTAMP",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ADD_TIMESTAMP, config); }};


    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(INPUT_FORMAT, ADD_TIMESTAMP);

    /// Well-known property for any sink that sends its data to a file
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "FILE_PATH",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static std::optional<DescriptorConfig::Config>
    validateAndFormatConfig(std::string_view sinkType, std::unordered_map<Identifier, std::string> configPairs);

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

namespace detail
{
struct ReflectedInlineSinkDescriptor
{
    uint64_t sinkId;
    std::variant<std::monostate, Schema<UnqualifiedUnboundField, Unordered>, Schema<UnqualifiedUnboundField, Ordered>> schema;
    std::string sinkType;
    Reflected config;
};

struct ReflectedNamedSinkDescriptor
{
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::string sinkType;
    Reflected config;
};
}

template <>
struct Reflector<NamedSinkDescriptor>
{
    Reflected operator()(const NamedSinkDescriptor& descriptor) const;
};

template <>
struct Unreflector<NamedSinkDescriptor>
{
    NamedSinkDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<InlineSinkDescriptor>
{
    Reflected operator()(const InlineSinkDescriptor& descriptor) const;
};

template <>
struct Unreflector<InlineSinkDescriptor>
{
    InlineSinkDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::SinkDescriptor>
{
    size_t operator()(const NES::SinkDescriptor& sinkDescriptor) const noexcept
    {
        return std::hash<NES::Identifier>{}(sinkDescriptor.getSinkName());
    }
};

FMT_OSTREAM(NES::SinkDescriptor);
