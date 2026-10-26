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

#ifdef NES_EVENT_TRACE_ITT

    #include <array>
    #include <cstddef>
    #include <cstdint>
    #include <string>
    #include <string_view>
    #include <unordered_map>
    #include <ittnotify.h>
    #include <Util/EventTrace.hpp>
    #include <EventTraceBackends.hpp>

namespace NES::EventTrace::Itt
{
namespace
{

constexpr size_t StageCount = 9;
constexpr std::array<const char*, StageCount> StageNames
    = {"SourceBackpressure", "SourceBufferWait", "SourceFill", "Enqueue", "Exec", "Emit", "NetSend", "NetRecv", "Counter"};

/// Only accessed with the sampler's lock held, see EventTraceBackends.hpp.
std::unordered_map<uint32_t, __itt_counter>& counters()
{
    static std::unordered_map<uint32_t, __itt_counter> instance;
    return instance;
}

struct Handles
{
    __itt_domain* domain = __itt_domain_create("NebulaStream");
    std::array<__itt_string_handle*, StageCount> stages{};
    __itt_string_handle* keyName = __itt_string_handle_create("key");
    __itt_string_handle* pipelineName = __itt_string_handle_create("pipeline");
    __itt_string_handle* extraName = __itt_string_handle_create("extra");

    Handles()
    {
        for (size_t i = 0; i < StageCount; ++i)
        {
            stages[i] = __itt_string_handle_create(StageNames[i]);
        }
    }
};

const Handles& handles() noexcept
{
    static const Handles instance;
    return instance;
}

__itt_string_handle* stageName(const Handles& h, Stage stage) noexcept
{
    const auto index = static_cast<size_t>(stage);
    return index < StageCount ? h.stages[index] : h.stages[static_cast<size_t>(Stage::Emit)];
}

/// Identifies the buffer independently of the stage and pipeline. Every task of a buffer has it as parent, so a relation
/// on it connects all tasks of that buffer.
__itt_id bufferId(const Key& key) noexcept
{
    return __itt_id{key.origin, key.seq, key.chunk};
}

/// Unique per (stage, key, pipeline) as long as pipeline ids fit into 24 bits.
__itt_id taskId(Stage stage, const Key& key, uint64_t pipeline) noexcept
{
    return __itt_id{key.origin, key.seq, key.chunk | (uint64_t{static_cast<uint16_t>(stage)} << 32) | (pipeline << 40)};
}

}

/// ITT records tasks, markers and relations at the time of the call, so the timestamps of mark() and link() are not used.
void begin(Stage stage, const Key& key, uint64_t pipeline) noexcept
{
    const auto& h = handles();
    const bool known = key.origin != 0 || key.seq != 0;
    __itt_task_begin(h.domain, known ? taskId(stage, key, pipeline) : __itt_null, known ? bufferId(key) : __itt_null, stageName(h, stage));
}

void end(Stage, const Key& key, uint64_t pipeline, uint64_t extra) noexcept
{
    const auto& h = handles();
    /// Metadata attaches to the current task, which is the one this call ends.
    std::array<uint64_t, 3> keyData{key.origin, key.seq, key.chunk};
    __itt_metadata_add(h.domain, __itt_null, h.keyName, __itt_metadata_u64, keyData.size(), keyData.data());
    __itt_metadata_add(h.domain, __itt_null, h.pipelineName, __itt_metadata_u64, 1, &pipeline);
    if (extra != 0)
    {
        __itt_metadata_add(h.domain, __itt_null, h.extraName, __itt_metadata_u64, 1, &extra);
    }
    __itt_task_end(h.domain);
}

void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t) noexcept
{
    const auto& h = handles();
    __itt_marker(h.domain, taskId(stage, key, pipeline), stageName(h, stage), __itt_marker_scope_thread);
}

void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t) noexcept
{
    const auto& h = handles();
    __itt_relation_add(h.domain, bufferId(output), __itt_relation_is_continuation_of, taskId(Stage::Exec, input, pipeline));
}

void counterCreated(uint32_t id, std::string_view name)
{
    counters()[id] = __itt_counter_create_typed(std::string(name).c_str(), "NebulaStream", __itt_metadata_s64);
}

void counterDestroyed(uint32_t id)
{
    if (const auto it = counters().find(id); it != counters().end())
    {
        __itt_counter_destroy(it->second);
        counters().erase(it);
    }
}

void counterValue(uint32_t id, int64_t value) noexcept
{
    if (const auto it = counters().find(id); it != counters().end())
    {
        __itt_counter_set_value(it->second, &value);
    }
}

}

#endif
