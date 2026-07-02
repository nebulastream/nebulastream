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

class SourceDescriptor final : public Descriptor
{
public:
    ~SourceDescriptor() = default;
    SourceDescriptor(const SourceDescriptor& other) = default;
    /// Deleted, because the descriptors have a const field
    SourceDescriptor& operator=(const SourceDescriptor& other) = delete;
    SourceDescriptor(SourceDescriptor&& other) noexcept = default;
    SourceDescriptor& operator=(SourceDescriptor&& other) noexcept = delete;

    friend std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs) = default;


    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor);

    [[nodiscard]] LogicalSource getLogicalSource() const;
    [[nodiscard]] std::string getSourceType() const;
    [[nodiscard]] std::string getInputFormatType() const;
    [[nodiscard]] InputFormatterDescriptor getInputFormatterDescriptor() const;

    [[nodiscard]] Host getHost() const;
    [[nodiscard]] PhysicalSourceId getPhysicalSourceId() const;

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
    InputFormatterDescriptor inputFormatterDescriptor;


    /// Used by Sources to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        PhysicalSourceId physicalSourceId,
        LogicalSource logicalSource,
        std::string_view sourceType,
        Host host,
        DescriptorConfig::Config config,
        const InputFormatterDescriptor& inputFormatterDescriptor);

public:
    /// Per default, we set an 'invalid' number of max inflight buffers. We choose zero as an invalid number as giving zero buffers to a source would make it unusable.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise, the source-specific configuration takes priority.
    static constexpr size_t INVALID_MAX_INFLIGHT_BUFFERS = 0;
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<size_t> MAX_INFLIGHT_BUFFERS{
        Identifier::parse("MAX_INFLIGHT_BUFFERS"),
        [](const ConfigLiteral& literal) { return tryGetOr<size_t>(literal, expectedType<size_t>());},
        INVALID_MAX_INFLIGHT_BUFFERS
    };


    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline auto configSchema = createConfigSchema(Identifier::parse("SOURCE"), MAX_INFLIGHT_BUFFERS);
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
    InputFormatterDescriptor inputFormatterDescriptor;
    Reflected config;
};
}

FMT_OSTREAM(NES::SourceDescriptor);
