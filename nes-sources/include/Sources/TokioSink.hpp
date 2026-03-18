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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>

namespace NES
{

/// Backpressure handler for TokioSink.
/// Duplicated from NetworkSink's BackpressureHandler to avoid cross-module dependency.
/// Buffers TupleBuffers internally when the Rust channel is full, applying/releasing
/// upstream backpressure at configurable hysteresis thresholds.
class SinkBackpressureHandler
{
    struct State
    {
        bool hasBackpressure = false;
        std::deque<TupleBuffer> buffered;
        SequenceNumber pendingSequenceNumber = INVALID<SequenceNumber>;
        ChunkNumber pendingChunkNumber = INVALID<ChunkNumber>;
    };

    folly::Synchronized<State> stateLock;
    size_t upperThreshold;
    size_t lowerThreshold;

public:
    /// @param upperThreshold Number of buffered items at which backpressure is applied.
    /// @param lowerThreshold Number of buffered items at which backpressure is released.
    explicit SinkBackpressureHandler(size_t upperThreshold = 1, size_t lowerThreshold = 0);

    /// Called when sink_send_buffer returns Full.
    /// Buffers the TupleBuffer internally. Applies backpressure if threshold exceeded.
    /// Returns a buffer to retry via repeatTask, or nullopt if one is already pending.
    std::optional<TupleBuffer> onFull(TupleBuffer buffer, BackpressureController& bpc);

    /// Called when sink_send_buffer returns Success.
    /// Clears pending state, releases backpressure if below threshold.
    /// Returns next buffered item to send, or nullopt if backlog is empty.
    std::optional<TupleBuffer> onSuccess(BackpressureController& bpc);

    /// Returns true if internal backlog is empty.
    bool empty() const;

    /// Pop front item from backlog for drain during stop().
    /// Returns nullopt if empty.
    std::optional<TupleBuffer> popFront();
};

struct ErrorContext;

/// C++ sink operator that bridges pipeline execution to a Rust AsyncSink via CXX FFI.
///
/// Lifecycle:
///   1. Constructed with BackpressureController, SpawnFn, channel capacity, hysteresis thresholds
///   2. start() calls SpawnFn to spawn the Rust sink task via CXX bridge
///   3. execute() sends buffers via sink_send_buffer FFI, with backpressure hysteresis
///   4. stop() drains backlog, sends Close, polls is_sink_done via repeatTask
///
/// The 3-phase stop state machine:
///   RUNNING -> DRAINING (drain backlog) -> CLOSING (send Close) -> WAITING_DONE (poll completion)
class TokioSink final : public Sink
{
public:
    /// Opaque Rust handle (defined in .cpp to hide CXX types)
    struct RustSinkHandleImpl;

    /// Function that spawns a Rust async sink via FFI.
    /// Arguments: sink_id, channel_capacity, error_fn_ptr, error_ctx_ptr
    /// Returns: opaque Rust handle wrapped in RustSinkHandleImpl
    using SpawnFn = std::function<std::unique_ptr<RustSinkHandleImpl>(
        uint64_t sink_id, uint32_t channel_capacity,
        uintptr_t error_fn_ptr, uintptr_t error_ctx_ptr)>;

    /// Retry interval for repeatTask when channel is full (matches NetworkSink)
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    /// Default channel capacity
    static constexpr uint32_t DEFAULT_CHANNEL_CAPACITY = 64;

    TokioSink(BackpressureController backpressureController,
              SpawnFn spawnFn,
              uint32_t channelCapacity = DEFAULT_CHANNEL_CAPACITY,
              size_t upperThreshold = 1,
              size_t lowerThreshold = 0);
    ~TokioSink() override;

    // Non-copyable, non-movable
    TokioSink(const TokioSink&) = delete;
    TokioSink& operator=(const TokioSink&) = delete;
    TokioSink(TokioSink&&) = delete;
    TokioSink& operator=(TokioSink&&) = delete;

    void start(PipelineExecutionContext& pec) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    /// Stop state machine phases
    enum class StopPhase : uint8_t
    {
        RUNNING,       /// Normal operation
        DRAINING,      /// Draining backlog into channel
        CLOSING,       /// Sending Close message
        WAITING_DONE   /// Polling is_sink_done
    };

    SpawnFn spawnFn;
    uint32_t channelCapacity;
    std::unique_ptr<RustSinkHandleImpl> rustHandle;
    SinkBackpressureHandler backpressureHandler;
    std::unique_ptr<ErrorContext> errorContext;
    std::atomic<bool> stopInitiated{false};
    StopPhase stopPhase{StopPhase::RUNNING};
    bool started{false};
    uint64_t sinkId{0}; /// Assigned in start() from pipeline context
};

/// Create a SpawnFn for a DevNull sink (test helper and default implementation).
TokioSink::SpawnFn makeDevNullSinkSpawnFn();

/// Create a SpawnFn for a file sink that writes raw binary to the given path.
TokioSink::SpawnFn makeFileSinkSpawnFn(std::string filePath);

} // namespace NES
