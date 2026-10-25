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

/// Backend-independent engine events for latency analysis. An event type is a trivially copyable struct of the fields it records,
/// with a `name` and a `category`. Spans are recorded live with begin() and end() on the thread they happen on; instants are points
/// in time. Recording an event only copies the struct; the backends describe the types once, at startup.
///
/// Backends are chosen at build time with -DNES_EVENT_TRACE_BACKENDS="ring;itt" (default: none, all macros compile to nothing):
///  - ring: per-thread ring buffers drained into `<prefix>.<pid>.bin`, a self-describing format read by scripts/latency_trace.py.
///          The prefix comes from NES_LATENCY_TRACE_PREFIX (default: `nes-latency-trace`).
///  - itt:  Intel ITT tasks, markers and counters with the fields as task metadata, shown by VTune when it is attached.
/// NES_LATENCY_TRACE_QUERY restricts per-buffer events to queries whose printed QueryId contains it, e.g. a distributed query name.
/// It sets the initial state of a query, which can be changed while it runs (nes-cli trace on|off).

#if defined(NES_EVENT_TRACE_RING) || defined(NES_EVENT_TRACE_ITT)
    #define NES_EVENT_TRACE_ENABLED

    #include <chrono>
    #include <cstdint>
    #include <functional>
    #include <optional>
    #include <string_view>
    #include <tuple>
    #include <type_traits>

namespace NES::EventTrace
{

/// Whether a span is useful work or waiting, so the report and VTune can separate the two.
enum class Category : uint8_t
{
    Work = 0,
    Wait = 1,
    Other = 2,
};

enum class Kind : uint8_t
{
    Begin = 0,
    End = 1,
    Instant = 2,
};

/// Identifies a tuple buffer.
struct Key
{
    uint64_t origin = 0;
    uint64_t seq = 0;
    uint64_t chunk = 0;
};

template <typename Buffer>
Key keyOf(const Buffer& buffer)
{
    return {
        .origin = buffer.getOriginId().getRawValue(),
        .seq = buffer.getSequenceNumber().getRawValue(),
        .chunk = buffer.getChunkNumber().getRawValue()};
}

/// Identifies a network channel in events, because buffers of different queries can have the same key. FNV-1a of the channel id,
/// never 0; scripts/latency_trace.py computes the same from the channel ids in the plan.
constexpr uint64_t channelTag(std::string_view channelId) noexcept
{
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (const char character : channelId)
    {
        hash = (hash ^ static_cast<uint8_t>(character)) * 0x100000001b3ULL;
    }
    return hash == 0 ? 1 : hash;
}

/// Nanoseconds on the steady clock. All event times use it.
inline uint64_t now() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// The steady-clock time of a time that a library reports in nanoseconds of the system clock.
inline uint64_t fromSystemTime(const uint64_t systemNanoseconds) noexcept
{
    const auto systemNow
        = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return now() - (static_cast<uint64_t>(systemNow) - systemNanoseconds);
}

/// Event types. Fields are Key, uint64_t or int64_t. A span records the struct at its begin and at its end, so fields that are only
/// known at the end are 0 in the begin record.

/// Source thread: waiting for the backpressure signal, for an empty buffer, and filling the buffer until it is handed to the engine.
struct SourceBackpressure
{
    static constexpr std::string_view name = "source backpressure";
    static constexpr Category category = Category::Wait;
};

struct SourceBufferWait
{
    static constexpr std::string_view name = "source buffer wait";
    static constexpr Category category = Category::Wait;
};

/// `firstData` is when the source got the first data of the buffer, or 0 if the source does not report it. `arrived` is when that
/// data reached the host before the source got it, as reported by a driver or the kernel, or 0.
struct SourceFill
{
    static constexpr std::string_view name = "source fill";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t pipeline = 0;
    uint64_t firstData = 0;
    uint64_t arrived = 0;
};

/// From emitWork entry until the task is in the task queue.
struct Enqueue
{
    static constexpr std::string_view name = "enqueue";
    static constexpr Category category = Category::Wait;
    Key key;
    uint64_t pipeline = 0;
};

/// A pipeline executing a task with the input buffer `key`.
struct Exec
{
    static constexpr std::string_view name = "exec";
    static constexpr Category category = Category::Work;
    Key key;
    uint64_t pipeline = 0;
    uint64_t tuples = 0;
};

/// A worker thread waiting for its next task. Belongs to no buffer or query, so it is recorded for every worker.
struct TaskQueueWait
{
    static constexpr std::string_view name = "task queue wait";
    static constexpr Category category = Category::Wait;
};

/// `pipeline` produced the buffer `output` while processing the buffer `input`.
struct Emit
{
    static constexpr std::string_view name = "emit";
    static constexpr Category category = Category::Other;
    Key output;
    Key input;
    uint64_t pipeline = 0;
};

/// A buffer handed to the network layer of the channel with the tag `channel`, see channelTag().
struct NetSend
{
    static constexpr std::string_view name = "net send";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t channel = 0;
};

/// A network source received the buffer `key` at `received` and hands it to `pipeline`.
struct NetRecv
{
    static constexpr std::string_view name = "net recv";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t pipeline = 0;
    uint64_t received = 0;
};

/// A pipe source received the buffer `upstream` from the pipe sink of another query at `received` and hands it to `pipeline` as
/// `key`. The pipe source assigns its own origin and sequence numbers, so the keys differ.
struct PipeRecv
{
    static constexpr std::string_view name = "pipe recv";
    static constexpr Category category = Category::Other;
    Key key;
    Key upstream;
    uint64_t pipeline = 0;
    uint64_t received = 0;
};

/// Steps of a buffer through the network layer of the channel `channel` between NetSend and NetRecv, recorded by the Rust network
/// crate for traced buffers. Sender: taken from the channel's software queue, serialized for the socket, acknowledged by the receiver.
/// Receiver: decoded from the socket, put into the queue the network source reads.
struct NetDequeue
{
    static constexpr std::string_view name = "net dequeue";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t channel = 0;
};

struct NetEncoded
{
    static constexpr std::string_view name = "net encoded";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t channel = 0;
};

struct NetAcked
{
    static constexpr std::string_view name = "net acked";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t channel = 0;
};

struct NetDecoded
{
    static constexpr std::string_view name = "net decoded";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t channel = 0;
};

struct NetQueued
{
    static constexpr std::string_view name = "net queued";
    static constexpr Category category = Category::Other;
    Key key;
    uint64_t channel = 0;
};

/// A sample of a CounterUpdater.
struct CounterSample
{
    static constexpr std::string_view name = "counter";
    static constexpr Category category = Category::Other;
    uint64_t counter = 0;
    int64_t value = 0;
};

/// The position of a type in this list is its id in the ring format.
using EventTypes = std::tuple<
    SourceBackpressure,
    SourceBufferWait,
    SourceFill,
    Enqueue,
    Exec,
    TaskQueueWait,
    Emit,
    NetSend,
    NetRecv,
    PipeRecv,
    CounterSample,
    NetDequeue,
    NetEncoded,
    NetAcked,
    NetDecoded,
    NetQueued>;

namespace detail
{
template <typename Event, typename... Events>
consteval uint16_t indexOf(std::tuple<Events...>*)
{
    uint16_t index = 0;
    (void)((std::is_same_v<Event, Events> ? false : (++index, true)) && ...);
    return index;
}
}

template <typename Event>
constexpr uint16_t typeId = detail::indexOf<Event>(static_cast<EventTypes*>(nullptr));

/// Empty event types record no payload.
template <typename Event>
constexpr uint32_t payloadSize = std::is_empty_v<Event> ? 0 : sizeof(Event);

/// Forwards an event to the backends. Use begin(), end() and instant().
void record(uint16_t type, Kind kind, const void* payload, uint32_t size) noexcept;

template <typename Event>
void emit(Kind kind, const Event& event) noexcept
{
    static_assert(std::is_trivially_copyable_v<Event>, "Event types are copied into the trace as bytes");
    static_assert(typeId<Event> < std::tuple_size_v<EventTypes>, "Event types must be listed in EventTypes");
    record(typeId<Event>, kind, &event, payloadSize<Event>);
}

/// A span starts or ends now on the calling thread. Spans of the same type on one thread do not overlap, so an end closes the last
/// begin of its type on that thread.
template <typename Event>
void begin(const Event& event) noexcept
{
    emit(Kind::Begin, event);
}

template <typename Event>
void end(const Event& event) noexcept
{
    emit(Kind::End, event);
}

template <typename Event>
void instant(const Event& event) noexcept
{
    emit(Kind::Instant, event);
}

/// A span over the lifetime of the scope. Fields changed through event() before the scope ends are recorded at its end.
template <typename Event>
class Scope
{
public:
    Scope(bool enabled, const Event& event) : enabled(enabled), recorded(event)
    {
        if (enabled)
        {
            begin(recorded);
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
            end(recorded);
        }
    }

    Event& event() { return recorded; }

private:
    bool enabled;
    Event recorded;
};

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
    uint64_t id;
};

/// Whether the query with this printed QueryId is traced.
bool traces(std::string_view queryDescription);
/// Pipeline graph of a started query, for the offline stitcher.
void writePlan(std::string_view plan);

/// Set by the worker thread for the task it executes, for code that runs inside a pipeline without knowing its query.
inline thread_local bool currentTaskTraced = false;

/// State of the source thread. The buffer's key is only known once the buffer is emitted, so the fill span ends with it when the
/// source emits the buffer.
struct SourceState
{
    /// Whether the query of the source was traced when it emitted its last buffer. Gates the begins and ends of the next buffer,
    /// so both see the same value.
    bool traced = false;
    uint64_t firstData = 0;
    uint64_t arrived = 0;
    bool networkSource = false;
    /// Set by a pipe source to the key the buffer had in the query that wrote it into the pipe.
    std::optional<Key> pipeUpstream;
};

inline thread_local SourceState sourceState;

/// Ends the fill span of a buffer that the source thread hands to the engine. Its pipeline is the first successor of the source.
/// A network source forwards buffers that keep their upstream key; their receive time is recorded for each successor. A pipe source
/// gives the buffer a new key, so its receive links the new key to the upstream one.
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
                instant(NetRecv{.key = key, .pipeline = successor->id.getRawValue(), .received = state.firstData});
            }
        }
        if (state.pipeUpstream)
        {
            for (const auto& successor : successors)
            {
                instant(PipeRecv{
                    .key = key, .upstream = *state.pipeUpstream, .pipeline = successor->id.getRawValue(), .received = state.firstData});
            }
        }
        end(SourceFill{
            .key = key, .pipeline = successors.front()->id.getRawValue(), .firstData = state.firstData, .arrived = state.arrived});
    }
    state.traced = false;
    for (const auto& successor : successors)
    {
        state.traced = state.traced || successor->latencyTraced;
    }
    state.firstData = 0;
    state.arrived = 0;
    state.pipeUpstream.reset();
}

}

    /// Wraps statements that only exist when event tracing is enabled.
    #define NES_EVENT(...) __VA_ARGS__
    /// Begins or ends a source span of the buffer the source thread is producing.
    #define NES_EVENT_SOURCE_BEGIN(Event) \
        (::NES::EventTrace::sourceState.traced ? ::NES::EventTrace::begin(::NES::EventTrace::Event{}) : (void)0)
    #define NES_EVENT_SOURCE_END(Event) \
        (::NES::EventTrace::sourceState.traced ? ::NES::EventTrace::end(::NES::EventTrace::Event{}) : (void)0)
    /// Sources that block waiting for input call this once data arrived, so the stitcher can start a buffer's latency there.
    #define NES_EVENT_FIRST_DATA() \
        (::NES::EventTrace::sourceState.firstData == 0 ? (void)(::NES::EventTrace::sourceState.firstData = ::NES::EventTrace::now()) \
                                                       : (void)0)
    /// Sources whose driver or kernel reports when data reached the host call this with that time (see fromSystemTime), together
    /// with NES_EVENT_FIRST_DATA when they get the data.
    #define NES_EVENT_DATA_ARRIVED(steadyTime) \
        (::NES::EventTrace::sourceState.arrived == 0 ? (void)(::NES::EventTrace::sourceState.arrived = (steadyTime)) : (void)0)
#else
    #define NES_EVENT(...)
    #define NES_EVENT_SOURCE_BEGIN(Event) ((void)0)
    #define NES_EVENT_SOURCE_END(Event) ((void)0)
    #define NES_EVENT_FIRST_DATA() ((void)0)
    #define NES_EVENT_DATA_ARRIVED(steadyTime) ((void)0)
#endif
