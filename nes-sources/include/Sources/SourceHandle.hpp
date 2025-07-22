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

#include <chrono>
#include <cstddef>
#include <memory>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <BackpressureChannel.hpp>

namespace NES::Sources
{

/// Hides SourceThread implementation.
class SourceThread;

/// Interface class to handle sources.
/// Created from a source descriptor via the SourceProvider.
/// start(): The underlying source starts consuming data. All queries using the source start processing.
/// stop(): The underlying source stops consuming data, notifying the QueryEngine,
/// that decides whether to keep queries, which used the particular source, alive.
class SourceHandle
{
public:
    explicit SourceHandle(
        Ingestion ingestion,
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool,
        size_t numberOfBuffersInLocalPool,
        std::unique_ptr<Source> sourceImplementation);

    ~SourceHandle();

    bool start(SourceReturnType::EmitFunction&& emitFunction) const;
    void stop() const;

    /// Tries to stop the source within a given timeout.
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) const;

    friend std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle);

    /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
    [[nodiscard]] OriginId getSourceId() const;

private:
    /// Used to print the data source via the overloaded '<<' operator.
    std::unique_ptr<SourceThread> sourceThread;
};

}

FMT_OSTREAM(NES::Sources::SourceHandle);
