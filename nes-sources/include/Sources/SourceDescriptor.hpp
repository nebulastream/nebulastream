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

#include <ostream>
#include <string>
#include <API/Schema.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES::Sources
{

struct ParserConfig
{
    std::string parserType;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
};

struct SourceDescriptor final : public NES::Configurations::Descriptor
{
    /// Per default, we set an 'invalid' number of buffers in source local buffer pool.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise the source-specific configuration takes priority.
    static constexpr int INVALID_NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL = -1;

    /// Used by Sources to create a valid SourceDescriptor.
    explicit SourceDescriptor(
        Schema schema,
        std::string logicalSourceName,
        std::string sourceType,
        int numberOfBuffersInSourceLocalBufferPool,
        ParserConfig parserConfig,
        NES::Configurations::DescriptorConfig::Config&& config);

    ~SourceDescriptor() = default;
    const Schema schema;
    const std::string logicalSourceName;
    const std::string sourceType;
    const int numberOfBuffersInSourceLocalBufferPool;
    /// is const data member, because 'SourceDescriptor' should be immutable and 'const' communicates more clearly then private+getter
    const ParserConfig parserConfig;

    [[nodiscard]] SerializableSourceDescriptor serialize() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    friend std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor);
    friend bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs);
};

}

/// Specializing the fmt ostream_formatter to accept SourceDescriptor objects.
/// Allows to call fmt::format("SourceDescriptor: {}", SourceDescriptorObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Sources::SourceDescriptor> : ostream_formatter
{
};
}
