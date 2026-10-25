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

/// Backend-independent engine events for latency analysis. Events are spans, marks and links keyed by the buffer identity
/// (originId, sequenceNumber, chunkNumber) and the pipeline they belong to.
///
/// Backends are chosen at build time with -DNES_EVENT_TRACE_BACKENDS="ring;itt" (default: none, all macros compile to nothing):
///  - ring: per-thread ring buffers drained into `<prefix>.<pid>.bin`, stitched offline by scripts/latency_trace.py.
///          The prefix comes from NES_LATENCY_TRACE_PREFIX (default: `nes-latency-trace`).
///  - itt:  Intel ITT tasks, markers and relations, shown by VTune when it is attached.
/// NES_LATENCY_TRACE_QUERY restricts tracing to queries whose printed QueryId contains it, e.g. a distributed query name.
/// Whether a query is traced is decided once when its pipelines are created, so untraced queries record nothing.

#if defined(NES_EVENT_TRACE_RING) || defined(NES_EVENT_TRACE_ITT)
    #define NES_EVENT_TRACE_ENABLED

    #include <chrono>
    #include <cstdint>
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

/// Something that happened to the buffer `key` in `pipeline` between t0 and t1.
void span(Stage stage, const Key& key, uint64_t pipeline, uint64_t t0, uint64_t t1, uint64_t extra = 0) noexcept;
/// Something that happened to the buffer `key` at time t.
void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept;
/// `pipeline` produced the buffer `output` at time t while processing the buffer `input`.
void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept;

/// Whether the query with this printed QueryId is traced.
bool traces(std::string_view queryDescription);
/// Pipeline graph of a started query, for the offline stitcher.
void writePlan(std::string_view plan);

/// Set by the worker thread for the task it executes, for code that runs inside a pipeline without knowing its query.
inline thread_local bool currentTaskTraced = false;

/// A span over the lifetime of the scope, recorded when it ends.
class Scope
{
public:
    Scope(bool enabled, Stage stage, const Key& key, uint64_t pipeline, uint64_t extra = 0)
        : enabled(enabled), stage(stage), key(key), pipeline(pipeline), extra(extra), start(enabled ? now() : 0)
    {
    }

    Scope(const Scope&) = delete;
    Scope(Scope&&) = delete;
    Scope& operator=(const Scope&) = delete;
    Scope& operator=(Scope&&) = delete;

    ~Scope()
    {
        if (enabled)
        {
            span(stage, key, pipeline, start, now(), extra);
        }
    }

private:
    bool enabled;
    Stage stage;
    Key key;
    uint64_t pipeline;
    uint64_t extra;
    uint64_t start;
};

/// Timestamps the source thread collects while producing a buffer. The buffer's key is only known once the buffer is emitted,
/// which is when the engine records the source spans on the same thread.
struct SourceStamps
{
    uint64_t backpressureStart = 0;
    uint64_t backpressureEnd = 0;
    uint64_t bufferStart = 0;
    uint64_t bufferEnd = 0;
    uint64_t fillStart = 0;
    uint64_t firstData = 0;
    bool networkSource = false;
};

inline thread_local SourceStamps sourceStamps;

/// Records the source spans of a buffer that the source thread hands to the engine for `pipeline`.
/// A network source forwards buffers that keep their upstream key; its receive time stands in for the fill.
inline void sourceEmitted(const Key& key, uint64_t pipeline, uint64_t emitted) noexcept
{
    const auto& stamps = sourceStamps;
    if (stamps.networkSource)
    {
        mark(Stage::NetRecv, key, pipeline, stamps.firstData);
        return;
    }
    span(Stage::SourceBackpressure, key, pipeline, stamps.backpressureStart, stamps.backpressureEnd);
    span(Stage::SourceBufferWait, key, pipeline, stamps.bufferStart, stamps.bufferEnd);
    span(Stage::SourceFill, key, pipeline, stamps.fillStart, emitted, stamps.firstData);
}

}

    /// Wraps statements that only exist when event tracing is enabled.
    #define NES_EVENT(...) __VA_ARGS__
    /// Stores the current time in a field of the thread's SourceStamps.
    #define NES_EVENT_STAMP(field) (::NES::EventTrace::sourceStamps.field = ::NES::EventTrace::now())
    /// Sources that block waiting for input call this once data arrived, so the stitcher can start a buffer's latency there.
    #define NES_EVENT_FIRST_DATA() (::NES::EventTrace::sourceStamps.firstData == 0 ? NES_EVENT_STAMP(firstData) : 0)
#else
    #define NES_EVENT(...)
    #define NES_EVENT_STAMP(field) ((void)0)
    #define NES_EVENT_FIRST_DATA() ((void)0)
#endif
