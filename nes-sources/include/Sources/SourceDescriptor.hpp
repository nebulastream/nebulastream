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
#include <ostream>
#include <string>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
class OperatorSerializationUtil;
}
namespace Catalogs::Source
{
class SourceCatalog;
}
namespace NES::Sources
{

struct ParserConfig
{
    std::string parserType;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
};
static_assert(std::is_copy_assignable_v<ParserConfig>);

class SourceDescriptor : public Configurations::Descriptor
{
    friend Catalogs::Source::SourceCatalog;
    friend OperatorSerializationUtil;
    uint64_t physicalSourceID;
    LogicalSource logicalSource;
    WorkerId workerID;
    std::string sourceType;
    /// is const data member, because 'SourceDescriptor' should be immutable and 'const' communicates more clearly then private+getter
    ParserConfig parserConfig;

    explicit SourceDescriptor(
        Configurations::DescriptorConfig::Config&& config,
        uint64_t physicalSourceID,
        WorkerId workerID,
        const LogicalSource& logicalSource,
        const std::string& sourceType,
        const ParserConfig& parserConfig);

public:
    /// Used by Sources to create a valid SourceDescriptor.

    ~SourceDescriptor() = default;
    SourceDescriptor(const SourceDescriptor& other) = default;
    //Deleted, because the descriptors have a const field
    SourceDescriptor& operator=(const SourceDescriptor& other) = delete;
    SourceDescriptor(SourceDescriptor&& other) noexcept = default;
    SourceDescriptor& operator=(SourceDescriptor&& other) noexcept = delete;

    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);
    friend bool operator!=(const SourceDescriptor& lhs, const SourceDescriptor& rhs) { return !(lhs == rhs); }

    [[nodiscard]] LogicalSource getLogicalSource() const { return logicalSource; }
    [[nodiscard]] std::shared_ptr<const Schema> getSchema() const { return logicalSource.getSchema(); }
    [[nodiscard]] std::string getSourceType() const { return sourceType; }
    [[nodiscard]] ParserConfig getParserConfig() const { return parserConfig; }

    [[nodiscard]] WorkerId getWorkerID() const { return workerID; }
    [[nodiscard]] uint64_t getPhysicalSourceID() const { return physicalSourceID; };
};


}

/// Specializing the fmt ostream_formatter to accept SourceDescriptor objects.
/// Allows to call fmt::format("SourceDescriptor: {}", SourceDescriptorObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct fmt::formatter<NES::Sources::SourceDescriptor> : formatter<string_view>
{
    format_context::iterator format(const NES::Sources::SourceDescriptor& sourceDescriptor, format_context& ctx) const;
};
}
