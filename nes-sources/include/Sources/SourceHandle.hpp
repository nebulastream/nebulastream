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

#include <memory>
#include <SourceParsers/SourceParserCSV.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/SourceThread.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

namespace NES::Sources
{

/// Interface class to handle sources.
/// Created from a source descriptor via the SourceProvider.
/// start(): The underlying source starts consuming data. All queries using the source start processing.
/// stop(): The underlying source stops consuming data, notifying the QueryManager,
/// that decides whether to keep queries, which used the particular source, alive.
class SourceHandle
{
public:
    explicit SourceHandle(
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool,
        SourceReturnType::EmitFunction&&,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImplementation,
        std::unique_ptr<SourceParsers::SourceParser> sourceParser);

    ~SourceHandle() = default;

    bool start() const;
    bool stop() const;

    friend std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle);

    /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
    [[nodiscard]] OriginId getSourceId() const;

private:
    /// Used to print the data source via the overloaded '<<' operator.
    std::unique_ptr<SourceThread> sourceThread;
};

}

/// Specializing the fmt ostream_formatter to accept SourceHandle objects.
/// Allows to call fmt::format("SourceHandle: {}", sourceHandleObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Sources::SourceHandle> : ostream_formatter
{
};
}
