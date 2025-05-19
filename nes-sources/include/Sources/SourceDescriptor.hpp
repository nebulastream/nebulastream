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
#include <type_traits>
#include <unordered_map>

#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/base.h>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <folly/hash/Hash.h>

namespace NES
{
namespace Catalogs::Source
{
class SourceCatalog;
}
class OperatorSerializationUtil;
}
namespace NES::Sources
{

struct ParserConfig
{
    std::string parserType;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    friend bool operator==(const ParserConfig& lhs, const ParserConfig& rhs);
    friend bool operator!=(const ParserConfig& lhs, const ParserConfig& rhs);
    static ParserConfig create(std::unordered_map<std::string, std::string> configMap);
};
static_assert(std::is_copy_assignable_v<ParserConfig>);

class SourceDescriptor : public Configurations::Descriptor
{
    friend class Catalogs::Source::SourceCatalog;
    friend OperatorSerializationUtil;
    uint64_t physicalSourceID;
    LogicalSource logicalSource;
    WorkerId workerID;
    std::string sourceType;
    ParserConfig parserConfig;
    int buffersInLocalPool;

    /// Per default, we set an 'invalid' number of buffers in source local buffer pool.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise the source-specific configuration takes priority.

    /// Used by Sources to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        LogicalSource logicalSource,
        uint64_t physicalSourceID,
        WorkerId workerID,
        std::string sourceType,
        int numberOfBuffersInSourceLocalBufferPool,
        Configurations::DescriptorConfig::Config&& config,
        ParserConfig parserConfig);

public:
    static constexpr int INVALID_BUFFERS_IN_LOCAL_POOL = -1;
    ~SourceDescriptor() = default;
    SourceDescriptor(const SourceDescriptor& other) = default;
    /// Deleted, because the descriptors have a const field
    SourceDescriptor& operator=(const SourceDescriptor& other) = delete;
    SourceDescriptor(SourceDescriptor&& other) noexcept = default;
    SourceDescriptor& operator=(SourceDescriptor&& other) noexcept = delete;

    friend std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);


    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor);

    [[nodiscard]] LogicalSource getLogicalSource() const;
    [[nodiscard]] std::string getSourceType() const;
    [[nodiscard]] ParserConfig getParserConfig() const;

    [[nodiscard]] WorkerId getWorkerId() const;
    [[nodiscard]] uint64_t getPhysicalSourceId() const;
    [[nodiscard]] int32_t getBuffersInLocalPool() const;
};

}

template <>
struct std::hash<NES::Sources::SourceDescriptor>
{
    size_t operator()(const NES::Sources::SourceDescriptor& sourceDescriptor) const noexcept
    {
        return folly::hash::hash_combine(sourceDescriptor.getLogicalSource(), sourceDescriptor.getPhysicalSourceId());
    }
};
/// Specializing the fmt ostream_formatter to accept SourceDescriptor objects.
/// Allows to call fmt::format("SourceDescriptor: {}", SourceDescriptorObject); and therefore also works with our logging.
template <>
struct fmt::formatter<NES::Sources::SourceDescriptor> : ostream_formatter
{
};
