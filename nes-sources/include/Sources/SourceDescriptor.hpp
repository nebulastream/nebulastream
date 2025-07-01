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

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/core.h>
#include <folly/hash/Hash.h>
#include <SerializableOperator.pb.h>

namespace NES
{
class SourceCatalog;
class OperatorSerializationUtil;

struct ParserConfig
{
    std::string parserType;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    friend bool operator==(const ParserConfig& lhs, const ParserConfig& rhs) = default;
    static ParserConfig create(std::unordered_map<std::string, std::string> configMap);
};

class SourceDescriptor final : public NES::Configurations::Descriptor
{
public:
    /// Per default, we set an 'invalid' number of buffers in source local buffer pool.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise the source-specific configuration takes priority.
    static constexpr int INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL = -1;

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

    [[nodiscard]] WorkerId getWorkerId() const;
    [[nodiscard]] PhysicalSourceId getPhysicalSourceId() const;
    [[nodiscard]] int32_t getBuffersInLocalPool() const;

    [[nodiscard]] SerializableSourceDescriptor serialize() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    friend class SourceCatalog;
    friend OperatorSerializationUtil;
    PhysicalSourceId physicalSourceId;
    LogicalSource logicalSource;
    WorkerId workerId;
    std::string sourceType;
    ParserConfig parserConfig;
    int32_t numberOfBuffersInLocalPool;

    /// Used by Sources to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        LogicalSource logicalSource,
        PhysicalSourceId physicalSourceId,
        WorkerId workerId,
        std::string sourceType,
        int32_t numberOfBuffersInLocalPool,
        NES::Configurations::DescriptorConfig::Config&& config,
        ParserConfig parserConfig);
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
