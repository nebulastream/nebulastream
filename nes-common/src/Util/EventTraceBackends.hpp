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

#include <cstdint>
#include <string_view>
#include <Util/EventTrace.hpp>

/// The backends behind Util/EventTrace.hpp. EventTrace.cpp forwards every call to each backend that is compiled in.
/// The counter functions are called with the sampler's lock held, so they never run concurrently.
namespace NES::EventTrace
{

#ifdef NES_EVENT_TRACE_RING
namespace Ring
{
void begin(Stage stage, const Key& key, uint64_t pipeline) noexcept;
void end(Stage stage, const Key& key, uint64_t pipeline, uint64_t extra) noexcept;
void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept;
void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept;
void writePlan(std::string_view plan);
void counterCreated(uint32_t id, std::string_view name);
void counterValue(uint32_t id, int64_t value) noexcept;
}
#endif

#ifdef NES_EVENT_TRACE_ITT
namespace Itt
{
void begin(Stage stage, const Key& key, uint64_t pipeline) noexcept;
void end(Stage stage, const Key& key, uint64_t pipeline, uint64_t extra) noexcept;
void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept;
void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept;
void counterCreated(uint32_t id, std::string_view name);
void counterDestroyed(uint32_t id);
void counterValue(uint32_t id, int64_t value) noexcept;
}
#endif

}
