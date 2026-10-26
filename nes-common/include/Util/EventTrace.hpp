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

/// Backend-independent engine events for latency analysis. Spans are recorded live with begin() and end() on the thread they
/// happen on, marks and links are points in time. Events are keyed by the buffer identity (originId, sequenceNumber, chunkNumber)
/// and the pipeline they belong to.
///
/// Backends are chosen at build time with -DNES_EVENT_TRACE_BACKENDS="ring;itt" (default: none, all macros compile to nothing):
///  - ring: per-thread ring buffers drained into `<prefix>.<pid>.bin`, stitched offline by scripts/latency_trace.py.
///          The prefix comes from NES_LATENCY_TRACE_PREFIX (default: `nes-latency-trace`).
///  - itt:  Intel ITT tasks, markers and relations, shown by VTune when it is attached.
/// NES_LATENCY_TRACE_QUERY restricts tracing to queries whose printed QueryId contains it, e.g. a distributed query name.
/// It sets the initial state of a query, which can be changed while it runs (nes-cli trace on|off).

#if defined(NES_EVENT_TRACE_RING) || defined(NES_EVENT_TRACE_ITT)
    #define NES_EVENT_TRACE_ENABLED

    #include <chrono>
    #include <cstdint>
    #include <functional>
    #include <string_view>

namespace NES::EventTrace
{

/// The numeric values are part of the ring file format.
enum class Stage : uint16_t
{
    /// Source thread: backpressure wait, buffer acquisition and buffer fill (until the buffer is handed to the engine).
    /// For SourceFill, `extra` holds the time the first data arrived, or 0 if the source does not report it.
    SourceBackpressure = 0,
    SourceBufferWait = 1,
    SourceFill = 2,
    /// From emitWork entry until the task is in the task queue.
    Enqueue = 3,
    /// A pipeline execution with the task's input buffer as key. `extra` holds the number of tuples.
    Exec = 4,
    /// Reserved for links, see link().
    Emit = 5,
    /// Buffer handed to / received from the network layer. NetRecv carries the pipeline the network source feeds.
    NetSend = 6,
    NetRecv = 7,
    /// A sample of a CounterUpdater. The pipeline field holds the counter id, `extra` the value.
    Counter = 8,
};

struct Key
{
    uint64_t origin = 0;
    uint64_t seq = 0;
    uint32_t chunk = 0;
};

template <typename Buffer>
Key keyOf(const Buffer& buffer)
{
    return {
        .origin = buffer.getOriginId().getRawValue(),
        .seq = buffer.getSequenceNumber().getRawValue(),
        .chunk = static_cast<uint32_t>(buffer.getChunkNumber().getRawValue())};
}

/// Nanoseconds on the steady clock. All timestamps passed to this API use it.
inline uint64_t now() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// A span of `stage` starts or ends now on the calling thread. Spans of the same stage on one thread do not overlap, so an end
/// closes the last begin of its stage on that thread. The key and pipeline of a begin may be unknown ({} and 0); the end carries them.
void begin(Stage stage, const Key& key, uint64_t pipeline) noexcept;
void end(Stage stage, const Key& key, uint64_t pipeline, uint64_t extra = 0) noexcept;
/// Something that happened to the buffer `key` at time t.
void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept;
/// `pipeline` produced the buffer `output` at time t while processing the buffer `input`.
void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept;

/// Samples a value, e.g. a queue length, every millisecond on the event trace's sampler thread while the CounterUpdater is alive.
/// Samples are recorded when the value changes. Not tied to a query, so the samples show what happened around traced buffers.
///
/// The destructor waits for a running sample to finish, so `sample` may use everything that outlives the CounterUpdater; declare
/// it after the members it captures. `sample` must be cheap, must not throw, and must not block on a lock that is held while a
/// CounterUpdater is created or destroyed.
class CounterUpdater
{
public:
    CounterUpdater(std::string_view name, std::function<int64_t()> sample);
    ~CounterUpdater();

    CounterUpdater(const CounterUpdater&) = delete;
    CounterUpdater(CounterUpdater&&) = delete;
    CounterUpdater& operator=(const CounterUpdater&) = delete;
    CounterUpdater& operator=(CounterUpdater&&) = delete;

private:
    uint32_t id;
};

/// Whether the query with this printed QueryId is traced.
bool traces(std::string_view queryDescription);
/// Pipeline graph of a started query, for the offline stitcher.
void writePlan(std::string_view plan);

/// Set by the worker thread for the task it executes, for code that runs inside a pipeline without knowing its query.
inline thread_local bool currentTaskTraced = false;

/// A span over the lifetime of the scope.
class Scope
{
public:
    Scope(bool enabled, Stage stage, const Key& key, uint64_t pipeline, uint64_t extra = 0)
        : enabled(enabled), stage(stage), key(key), pipeline(pipeline), extra(extra)
    {
        if (enabled)
        {
            begin(stage, key, pipeline);
        }
    }

    Scope(const Scope&) = delete;
    Scope(Scope&&) = delete;
    Scope& operator=(const Scope&) = delete;
    Scope& operator=(Scope&&) = delete;

    ~Scope()
    {
        if (enabled)
        {
            end(stage, key, pipeline, extra);
        }
    }

private:
    bool enabled;
    Stage stage;
    Key key;
    uint64_t pipeline;
    uint64_t extra;
};

/// State of the source thread. The buffer's key is only known once the buffer is emitted, so the source spans begin without it
/// and the fill span ends with it when the source emits the buffer.
struct SourceState
{
    /// Whether the query of the source was traced when it emitted its last buffer. Gates the begins and ends of the next buffer,
    /// so both see the same value.
    bool traced = false;
    uint64_t firstData = 0;
    bool networkSource = false;
};

inline thread_local SourceState sourceState;

/// Ends the fill span of a buffer that the source thread hands to the engine. `pipeline` is the first successor of the source.
/// A network source forwards buffers that keep their upstream key; its receive time is recorded for each successor.
template <typename Successors>
void sourceEmitted(const Key& key, const Successors& successors) noexcept
{
    auto& state = sourceState;
    if (state.traced)
    {
        if (state.networkSource)
        {
            for (const auto& successor : successors)
            {
                mark(Stage::NetRecv, key, successor->id.getRawValue(), state.firstData);
            }
        }
        end(Stage::SourceFill, key, successors.front()->id.getRawValue(), state.firstData);
    }
    state.traced = false;
    for (const auto& successor : successors)
    {
        state.traced = state.traced || successor->latencyTraced;
    }
    state.firstData = 0;
}

}

    /// Wraps statements that only exist when event tracing is enabled.
    #define NES_EVENT(...) __VA_ARGS__
    /// Begins or ends a source span of the buffer the source thread is producing.
    #define NES_EVENT_SOURCE_BEGIN(stage) \
        (::NES::EventTrace::sourceState.traced ? ::NES::EventTrace::begin(::NES::EventTrace::Stage::stage, {}, 0) : (void)0)
    #define NES_EVENT_SOURCE_END(stage) \
        (::NES::EventTrace::sourceState.traced ? ::NES::EventTrace::end(::NES::EventTrace::Stage::stage, {}, 0) : (void)0)
    /// Sources that block waiting for input call this once data arrived, so the stitcher can start a buffer's latency there.
    #define NES_EVENT_FIRST_DATA() \
        (::NES::EventTrace::sourceState.firstData == 0 ? (void)(::NES::EventTrace::sourceState.firstData = ::NES::EventTrace::now()) \
                                                       : (void)0)
#else
    #define NES_EVENT(...)
    #define NES_EVENT_SOURCE_BEGIN(stage) ((void)0)
    #define NES_EVENT_SOURCE_END(stage) ((void)0)
    #define NES_EVENT_FIRST_DATA() ((void)0)
#endif
