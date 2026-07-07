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
#include "Configurations/ConfigValue.hpp"
#include "Identifiers/Identifier.hpp"
#include "Util/Variant.hpp"

namespace NES
{
class SourceCatalog;
class OperatorSerializationUtil;

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
        return lhs.physicalSourceId == rhs.physicalSourceId && lhs.logicalSource == rhs.logicalSource
            && lhs.sourceType == rhs.sourceType && lhs.host == rhs.host && lhs.maxInflightBuffers == rhs.maxInflightBuffers
            && lhs.inputFormatterDescriptor == rhs.inputFormatterDescriptor;
    }


    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor);

    [[nodiscard]] LogicalSource getLogicalSource() const;
    [[nodiscard]] std::string getSourceType() const;
    [[nodiscard]] std::string getInputFormatType() const;
    [[nodiscard]] InputFormatterDescriptor getInputFormatterDescriptor() const;

    [[nodiscard]] Host getHost() const;
    [[nodiscard]] size_t getMaxInflightBuffers() const;
    [[nodiscard]] PhysicalSourceId getPhysicalSourceId() const;
    [[nodiscard]] const InstantiatedConfig& getPluginConfig() const;
    /// The source-defined config struct (e.g. GeneratorSourceConfig), type-erased. Produced by the
    /// source's SourceConfigRegistry entry, so the source factory can safely any_cast it back.
    [[nodiscard]] const std::any& getPluginData() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    friend class SourceCatalog;
    friend OperatorSerializationUtil;
    friend struct Unreflector<SourceDescriptor>;
    friend struct Reflector<SourceDescriptor>;

    PhysicalSourceId physicalSourceId;
    LogicalSource logicalSource;
    std::string sourceType;
    Host host;
    size_t maxInflightBuffers;
    std::any pluginData;
    InstantiatedConfig pluginConfig;
    InputFormatterDescriptor inputFormatterDescriptor;


    /// Used by the SourceCatalog (and descriptor unreflection) to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        PhysicalSourceId physicalSourceId,
        LogicalSource logicalSource,
        std::string_view sourceType,
        Host host,
        size_t maxInflightBuffers,
        InstantiatedConfig pluginConfig,
        std::any pluginData,
        const InputFormatterDescriptor& inputFormatterDescriptor);

public:
    /// Per default, we set an 'invalid' number of max inflight buffers. We choose zero as an invalid number as giving zero buffers to a source would make it unusable.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise, the source-specific configuration takes priority.
    static constexpr size_t INVALID_MAX_INFLIGHT_BUFFERS = 0;
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<size_t> MAX_INFLIGHT_BUFFERS{
        Identifier::parse("MAX_INFLIGHT_BUFFERS"),
        [](const ConfigLiteral& literal)
        { return tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(downcastConfigValue<int64_t, size_t>); },
        INVALID_MAX_INFLIGHT_BUFFERS};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<Host> HOST{
        "HOST",
        [](const ConfigLiteral& literal) -> std::expected<Host, Exception>
        {
            return tryGetOr<std::string>(literal, expectedType<std::string>())
                .transform([](std::string&& value) { return Host{std::move(value)}; });
        },
        /// Optional: attached workers pass the host out of band (e.g. as a catalog argument).
        Host{""}};


    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline auto configSchema = createConfigSchema(Identifier::parse("SOURCE"), MAX_INFLIGHT_BUFFERS, HOST);
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

}

template <>
struct std::hash<NES::SourceDescriptor>
{
    size_t operator()(const NES::SourceDescriptor& sourceDescriptor) const noexcept
    {
        return folly::hash::hash_combine(sourceDescriptor.getLogicalSource(), sourceDescriptor.getPhysicalSourceId());
    }
};

namespace NES::detail
{
struct ReflectedSourceDescriptor
{
    uint64_t physicalSourceId;
    LogicalSource logicalSource;
    std::string type;
    Host host;
    uint64_t maxInflightBuffers;
    InputFormatterDescriptor inputFormatterDescriptor;
    /// The source-defined config struct, reflected by the source's SourceConfigRegistry entry.
    /// The generic InstantiatedConfig does not travel; only the typed config does.
    Reflected config;
};
}

FMT_OSTREAM(NES::SourceDescriptor);
