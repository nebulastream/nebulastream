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
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/hash/Hash.h>

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
    friend bool operator==(const ParserConfig& lhs, const ParserConfig& rhs);
    friend bool operator!=(const ParserConfig& lhs, const ParserConfig& rhs);
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
    ParserConfig parserConfig;

    explicit SourceDescriptor(
        Configurations::DescriptorConfig::Config&& config,
        uint64_t physicalSourceID,
        WorkerId workerID,
        const LogicalSource& logicalSource,
        std::string sourceType,
        ParserConfig parserConfig);

public:
    ~SourceDescriptor() = default;
    SourceDescriptor(const SourceDescriptor& other) = default;
    //Deleted, because the descriptors have a const field
    SourceDescriptor& operator=(const SourceDescriptor& other) = delete;
    SourceDescriptor(SourceDescriptor&& other) noexcept = default;
    SourceDescriptor& operator=(SourceDescriptor&& other) noexcept = delete;

    friend std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);


    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor);

    [[nodiscard]] LogicalSource getLogicalSource() const;
    [[nodiscard]] std::shared_ptr<const Schema> getSchema() const;
    [[nodiscard]] std::string getSourceType() const;
    [[nodiscard]] ParserConfig getParserConfig() const;

    [[nodiscard]] WorkerId getWorkerID() const;
    [[nodiscard]] uint64_t getPhysicalSourceID() const;

    void setSchema(const Schema& newSchema);
};

}

template <>
struct std::hash<NES::Sources::SourceDescriptor>
{
    size_t operator()(const NES::Sources::SourceDescriptor& sourceDescriptor) const noexcept
    {
        return folly::hash::hash_combine(sourceDescriptor.getLogicalSource(), sourceDescriptor.getPhysicalSourceID());
    }
};
/// Specializing the fmt ostream_formatter to accept SourceDescriptor objects.
/// Allows to call fmt::format("SourceDescriptor: {}", SourceDescriptorObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct fmt::formatter<NES::Sources::SourceDescriptor> : ostream_formatter
{
};
}
