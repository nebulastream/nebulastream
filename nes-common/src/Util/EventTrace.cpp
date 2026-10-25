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

#include <Util/EventTrace.hpp>

#ifdef NES_EVENT_TRACE_ENABLED

    #include <cstdint>
    #include <cstdlib>
    #include <string>
    #include <string_view>
    #include <EventTraceBackends.hpp>

namespace NES::EventTrace
{

void span(Stage stage, const Key& key, uint64_t pipeline, uint64_t t0, uint64_t t1, uint64_t extra) noexcept
{
    #ifdef NES_EVENT_TRACE_RING
    Ring::span(stage, key, pipeline, t0, t1, extra);
    #endif
    #ifdef NES_EVENT_TRACE_ITT
    Itt::span(stage, key, pipeline, t0, t1, extra);
    #endif
}

void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept
{
    #ifdef NES_EVENT_TRACE_RING
    Ring::mark(stage, key, pipeline, t);
    #endif
    #ifdef NES_EVENT_TRACE_ITT
    Itt::mark(stage, key, pipeline, t);
    #endif
}

void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept
{
    #ifdef NES_EVENT_TRACE_RING
    Ring::link(output, input, pipeline, t);
    #endif
    #ifdef NES_EVENT_TRACE_ITT
    Itt::link(output, input, pipeline, t);
    #endif
}

void writePlan([[maybe_unused]] std::string_view plan)
{
    #ifdef NES_EVENT_TRACE_RING
    Ring::writePlan(plan);
    #endif
}

bool traces(std::string_view queryDescription)
{
    static const std::string filter = []
    {
        const auto* env = std::getenv("NES_LATENCY_TRACE_QUERY"); /// NOLINT(concurrency-mt-unsafe) read once during initialization
        return env != nullptr ? std::string(env) : std::string();
    }();
    return queryDescription.contains(filter);
}

}

#endif
