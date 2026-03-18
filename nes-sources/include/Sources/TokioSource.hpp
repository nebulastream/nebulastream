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
#include <cstdint>
#include <memory>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceReturnType.hpp>

namespace NES {

struct EmitContext;
struct ErrorContext;

/// C++ wrapper around a Rust async source managed by the Tokio runtime.
///
/// TokioSource provides the same interface as SourceThread (start/stop/tryStop/getSourceId)
/// so that SourceHandle can dispatch to either via std::variant without changing
/// RunningSource or QueryEngine.
///
/// Lifecycle:
///   1. Constructed with originId, inflightLimit, and bufferProvider
///   2. start(EmitFunction&&) creates EmitContext/ErrorContext, calls spawn_source FFI
///   3. stop() calls stop_source FFI (non-blocking, cancels CancellationToken)
///   4. Destructor calls stop() if still running, then drops Rust SourceTaskHandle
class TokioSource
{
public:
    explicit TokioSource(
        OriginId originId,
        uint32_t inflightLimit,
        std::shared_ptr<AbstractBufferProvider> bufferProvider);

    ~TokioSource();

    // Non-copyable, non-movable (Rust handle is pinned)
    TokioSource(const TokioSource&) = delete;
    TokioSource(TokioSource&&) = delete;
    TokioSource& operator=(const TokioSource&) = delete;
    TokioSource& operator=(TokioSource&&) = delete;

    /// Start the Rust async source. Creates EmitContext/ErrorContext and calls
    /// spawn_source FFI. Returns true on success, false on failure.
    [[nodiscard]] bool start(SourceReturnType::EmitFunction&& emitFunction);

    /// Request non-blocking cooperative stop via CancellationToken.
    void stop();

    /// Attempt to stop within timeout. Since Rust sources stop asynchronously
    /// via CancellationToken, this calls stop() and returns SUCCESS.
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout);

    /// Get the origin ID for this source.
    [[nodiscard]] OriginId getSourceId() const;

    friend std::ostream& operator<<(std::ostream& out, const TokioSource& source);

private:
    OriginId originId;
    uint32_t inflightLimit;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;

    /// Opaque Rust handle returned by spawn_source, stored as PIMPL.
    /// The actual rust::Box<SourceHandle> is managed in the cpp file
    /// to avoid exposing CXX types in the public header.
    struct RustHandleImpl;
    std::unique_ptr<RustHandleImpl> rustHandle;

    std::unique_ptr<EmitContext> emitContext;
    std::unique_ptr<ErrorContext> errorContext;
    bool started{false};
    bool stopped{false};
};

} // namespace NES
