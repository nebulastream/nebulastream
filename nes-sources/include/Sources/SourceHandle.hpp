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
#include <variant>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/TokioSource.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <BackpressureChannel.hpp>

namespace NES
{

/// Hides SourceThread implementation.
class SourceThread;

struct SourceRuntimeConfiguration
{
    size_t inflightBufferLimit;
};

/// Interface class to handle sources.
/// Created from a source descriptor via the SourceProvider.
/// start(): The underlying source starts consuming data. All queries using the source start processing.
/// stop(): The underlying source stops consuming data, notifying the QueryEngine,
/// that decides whether to keep queries, which used the particular source, alive.
///
/// Holds either a SourceThread (C++ source) or TokioSource (Rust async source) via std::variant.
/// All method dispatch uses std::visit so RunningSource and QueryEngine remain unchanged.
class SourceHandle
{
public:
    /// Construct a SourceHandle wrapping a C++ SourceThread (existing path).
    explicit SourceHandle(
        BackpressureListener backpressureListener,
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        SourceRuntimeConfiguration configuration,
        std::shared_ptr<AbstractBufferProvider> bufferPool,
        std::unique_ptr<Source> sourceImplementation);

    /// Construct a SourceHandle wrapping a Rust TokioSource.
    explicit SourceHandle(
        SourceRuntimeConfiguration configuration,
        std::unique_ptr<TokioSource> tokioSource);

    ~SourceHandle();

    bool start(SourceReturnType::EmitFunction&& emitFunction) const;
    void stop() const;

    /// Tries to stop the source within a given timeout.
    [[nodiscard]] NES::SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) const;

    friend std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle);

    /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
    [[nodiscard]] OriginId getSourceId() const;

    const SourceRuntimeConfiguration& getRuntimeConfiguration() const { return configuration; }

private:
    SourceRuntimeConfiguration configuration;

    /// Variant holding either a C++ SourceThread or a Rust TokioSource.
    /// All method dispatch uses std::visit with the Overloaded{} pattern.
    std::variant<std::unique_ptr<SourceThread>, std::unique_ptr<TokioSource>> impl_;
};

}

FMT_OSTREAM(NES::SourceHandle);
