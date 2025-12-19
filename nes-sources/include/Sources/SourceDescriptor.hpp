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
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/SerializedData.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/core.h>
#include <folly/hash/Hash.h>
#include <SerializableOperator.pb.h>

namespace NES
{
class SerializedUtils;
class SourceCatalog;
class OperatorSerializationUtil;

struct ParserConfig
{
    std::string parserType;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    friend bool operator==(const ParserConfig& lhs, const ParserConfig& rhs) = default;
    friend std::ostream& operator<<(std::ostream& os, const ParserConfig& obj);
    static ParserConfig create(std::unordered_map<std::string, std::string> configMap);
};

struct SerializedSourceDescriptor
{
    uint64_t physicalSourceId;
    std::string name;
    std::string type;
    SerializedSchema schema;
    rfl::Box<ParserConfig> parserConfig;
    rfl::Generic config;
};

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
    [[nodiscard]] ParserConfig getParserConfig() const;

    [[nodiscard]] PhysicalSourceId getPhysicalSourceId() const;

    [[nodiscard]] SerializableSourceDescriptor serialize() const;
    SerializedSourceDescriptor serialized() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    friend class SourceCatalog;
    friend OperatorSerializationUtil;
    friend SerializedUtils;

    PhysicalSourceId physicalSourceId;
    LogicalSource logicalSource;
    std::string sourceType;
    ParserConfig parserConfig;

    /// Used by Sources to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        PhysicalSourceId physicalSourceId,
        LogicalSource logicalSource,
        std::string_view sourceType,
        DescriptorConfig::Config config,
        ParserConfig parserConfig);

public:
    /// Per default, we set an 'invalid' number of max inflight buffers. We choose zero as an invalid number as giving zero buffers to a source would make it unusable.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise, the source-specific configuration takes priority.
    static constexpr size_t INVALID_MAX_INFLIGHT_BUFFERS = 0;
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_INFLIGHT_BUFFERS{
        "max_inflight_buffers",
        INVALID_MAX_INFLIGHT_BUFFERS,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_INFLIGHT_BUFFERS, config); }};


    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(MAX_INFLIGHT_BUFFERS);
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

FMT_OSTREAM(NES::SourceDescriptor);
FMT_OSTREAM(NES::ParserConfig);
