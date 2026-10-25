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
    #include <EventTraceBackends.hpp>
    #include <ittnotify.h>
    #include <Util/EventTrace.hpp>

namespace NES::EventTrace::Itt
{
namespace
{

constexpr size_t StageCount = 8;
constexpr std::array<const char*, StageCount> StageNames
    = {"SourceBackpressure", "SourceBufferWait", "SourceFill", "Enqueue", "Exec", "Emit", "NetSend", "NetRecv"};

void steadyClockInfo(__itt_clock_info* info, void*)
{
    info->clock_freq = 1'000'000'000;
    info->clock_base = now();
}

struct Handles
{
    __itt_domain* domain = __itt_domain_create("NebulaStream");
    __itt_clock_domain* clock = __itt_clock_domain_create(steadyClockInfo, nullptr);
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

void span(Stage stage, const Key& key, uint64_t pipeline, uint64_t t0, uint64_t t1, uint64_t extra) noexcept
{
    const auto& h = handles();
    __itt_task_begin_ex(h.domain, h.clock, t0, taskId(stage, key, pipeline), bufferId(key), stageName(h, stage));
    std::array<uint64_t, 3> keyData{key.origin, key.seq, key.chunk};
    __itt_metadata_add(h.domain, __itt_null, h.keyName, __itt_metadata_u64, keyData.size(), keyData.data());
    __itt_metadata_add(h.domain, __itt_null, h.pipelineName, __itt_metadata_u64, 1, &pipeline);
    if (extra != 0)
    {
        __itt_metadata_add(h.domain, __itt_null, h.extraName, __itt_metadata_u64, 1, &extra);
    }
    __itt_task_end_ex(h.domain, h.clock, t1);
}

void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept
{
    const auto& h = handles();
    __itt_marker_ex(h.domain, h.clock, t, taskId(stage, key, pipeline), stageName(h, stage), __itt_marker_scope_thread);
}

void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept
{
    const auto& h = handles();
    __itt_relation_add_ex(
        h.domain, h.clock, t, bufferId(output), __itt_relation_is_continuation_of, taskId(Stage::Exec, input, pipeline));
}

}

#endif
