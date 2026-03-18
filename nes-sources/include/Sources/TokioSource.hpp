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
#include <functional>
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
/// The SpawnFn captures the Rust source type and its configuration. Different Rust
/// source implementations (GeneratorSource, etc.) provide their own SpawnFn that
/// calls the appropriate FFI entry point with the right parameters.
///
/// Lifecycle:
///   1. Constructed with originId, inflightLimit, bufferProvider, and a SpawnFn
///   2. start(EmitFunction&&) creates EmitContext/ErrorContext, calls SpawnFn
///   3. stop() calls stop_source FFI (non-blocking, cancels CancellationToken)
///   4. Destructor calls stop() if still running, then drops Rust SourceTaskHandle
class TokioSource
{
public:
    /// Opaque Rust handle returned by the spawn function.
    /// Defined in the .cpp to avoid exposing CXX types.
    struct RustHandleImpl;

    /// Function that spawns a Rust async source via FFI.
    /// Arguments: source_id, buffer_provider_ptr, inflight_limit,
    ///            emit_fn_ptr, emit_ctx_ptr, error_fn_ptr, error_ctx_ptr
    /// Returns: opaque Rust handle (wrapped in RustHandleImpl by start())
    using SpawnFn = std::function<std::unique_ptr<RustHandleImpl>(
        uint64_t, uintptr_t, uint32_t,
        uintptr_t, uintptr_t, uintptr_t, uintptr_t)>;

    explicit TokioSource(
        OriginId originId,
        uint32_t inflightLimit,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        SpawnFn spawnFn);

    ~TokioSource();

    // Non-copyable, non-movable (Rust handle is pinned)
    TokioSource(const TokioSource&) = delete;
    TokioSource(TokioSource&&) = delete;
    TokioSource& operator=(const TokioSource&) = delete;
    TokioSource& operator=(TokioSource&&) = delete;

    /// Start the Rust async source. Creates EmitContext/ErrorContext and calls
    /// the SpawnFn. Returns true on success, false on failure.
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
    SpawnFn spawnFn;

    std::unique_ptr<RustHandleImpl> rustHandle;

    std::unique_ptr<EmitContext> emitContext;
    std::unique_ptr<ErrorContext> errorContext;
    bool started{false};
    bool stopped{false};
};

} // namespace NES
