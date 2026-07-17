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
#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <fmt/core.h>
#include <folly/hash/Hash.h>
#include <InputFormatterDescriptor.hpp>
#include "Configurations/ConfigField.hpp"
#include "Identifiers/Identifier.hpp"
#include "Util/Variant.hpp"

namespace NES
{
class SourceCatalog;
class OperatorSerializationUtil;

class PluginSourceConfiguration
{
public:
    PluginSourceConfiguration(Identifier type, std::any pluginData) : type(type), pluginData(std::move(pluginData)) {}
    [[nodiscard]] const Identifier& getType() const { return type; }
    [[nodiscard]] const std::any& getPluginData() const { return pluginData; }
private:
    Identifier type;
    std::any pluginData;
};

class SourceDescriptor final
{
public:
    ~SourceDescriptor() = default;
    SourceDescriptor(const SourceDescriptor& other) = default;
    /// Deleted, because the descriptors have a const field
    SourceDescriptor& operator=(const SourceDescriptor& other) = delete;
    SourceDescriptor(SourceDescriptor&& other) noexcept = default;
    SourceDescriptor& operator=(SourceDescriptor&& other) noexcept = delete;

    friend std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs);
    /// The type-erased plugin config (std::any) is not comparable; two descriptors are considered
    /// equal if their identity and wiring match.
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
    {
        return lhs.physicalSourceId == rhs.physicalSourceId;
    }


    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor);

    [[nodiscard]] const Identifier& getSourceType() const;
    [[nodiscard]] const Identifier& getInputFormatType() const;
    [[nodiscard]] InputFormatterDescriptor getInputFormatterDescriptor() const;

    [[nodiscard]] Host getHost() const;

    [[nodiscard]] const Schema<UnqualifiedUnboundField, Ordered>& getSchema() const;
    ///@returns either a number greater than zero, or a nullopt, indicating that the default value should be used
    [[nodiscard]] std::optional<size_t> getMaxInflightBuffers() const;
    [[nodiscard]] PhysicalSourceId getPhysicalSourceId() const;
    /// The source-defined config struct (e.g. GeneratorSourceConfig), type-erased. Produced by the
    /// source's SourceConfigRegistry entry, so the source factory can safely any_cast it back.
    [[nodiscard]] const std::any& getPluginData() const;

    [[nodiscard]] const std::optional<Identifier>& getLogicalSourceName() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    friend class SourceCatalog;
    friend class PhysicalSourceBuilder;
    friend OperatorSerializationUtil;
    friend struct Unreflector<SourceDescriptor>;
    friend struct Reflector<SourceDescriptor>;

    PhysicalSourceId physicalSourceId;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    Host host;
    std::optional<size_t> maxInflightBuffers;
    PluginSourceConfiguration pluginSourceConfig;
    InputFormatterDescriptor inputFormatterDescriptor;

    std::optional<Identifier> logicalSourceName;

    /// Used by the SourceCatalog (and descriptor unreflection) to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        PhysicalSourceId physicalSourceId,
        Schema<UnqualifiedUnboundField, Ordered> schema,
        Host host,
        std::optional<size_t> maxInflightBuffers,
        PluginSourceConfiguration pluginData,
        InputFormatterDescriptor inputFormatterDescriptor,
        std::optional<Identifier> logicalSourceName);

public:

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<std::optional<Schema<UnqualifiedUnboundField, Ordered>>> SCHEMA{
        "SCHEMA",
            [](const ConfigLiteral& literal){ return tryGetOr<Schema<UnqualifiedUnboundField, Ordered>>(literal, expectedType<Schema<UnqualifiedUnboundField, Ordered>>()); },
            std::nullopt
        };

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<std::optional<size_t>> MAX_INFLIGHT_BUFFERS{
        Identifier::parse("MAX_INFLIGHT_BUFFERS"),
        [](const ConfigLiteral& literal)
        { return tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(downcastConfigValue<int64_t, size_t>).transform([](const size_t& value) -> std::optional<size_t>
        {
            if (value == 0)
            {
                return std::nullopt;
            }
            return value;
        }); },
        std::nullopt};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<std::optional<Host>> HOST{
        "HOST",
        [](const ConfigLiteral& literal) -> std::expected<Host, Exception>
        {
            return tryGetOr<std::string>(literal, expectedType<std::string>())
                .transform([](std::string&& value) { return Host{std::move(value)}; });
        },
        /// Optional: attached workers pass the host out of band (e.g. as a catalog argument).
        std::nullopt};

    static inline auto configSchema = createConfigSchema(Identifier::parse("SOURCE"), MAX_INFLIGHT_BUFFERS, HOST, SCHEMA);
};

template <>
struct Reflector<SourceDescriptor>
{
    Reflected operator()(const SourceDescriptor& sourceDescriptor) const;
};

template <>
struct Unreflector<SourceDescriptor>
{
    SourceDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

template <>
struct Reflector<PluginSourceConfiguration> {
    Reflected operator()(const PluginSourceConfiguration& config) const;
};

template <>
struct Unreflector<PluginSourceConfiguration> {
    PluginSourceConfiguration operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::SourceDescriptor>
{
    size_t operator()(const NES::SourceDescriptor& sourceDescriptor) const noexcept
    {
        return folly::hash::hash_combine(sourceDescriptor.getSchema(), sourceDescriptor.getPhysicalSourceId());
    }
};

namespace NES::detail
{
struct ReflectedPluginSourceConfiguration
{
    Identifier type;
    Reflected pluginData;
};

struct ReflectedSourceDescriptor
{
    PhysicalSourceId physicalSourceId;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    Host host;
    std::optional<size_t> maxInflightBuffers;
    PluginSourceConfiguration pluginSourceConfig;
    InputFormatterDescriptor inputFormatterDescriptor;

    std::optional<Identifier> logicalSourceName;
};
}

FMT_OSTREAM(NES::SourceDescriptor);
