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
namespace NES::EventTrace
{

#ifdef NES_EVENT_TRACE_RING
namespace Ring
{
void span(Stage stage, const Key& key, uint64_t pipeline, uint64_t t0, uint64_t t1, uint64_t extra) noexcept;
void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept;
void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept;
void writePlan(std::string_view plan);
}
#endif

#ifdef NES_EVENT_TRACE_ITT
namespace Itt
{
void span(Stage stage, const Key& key, uint64_t pipeline, uint64_t t0, uint64_t t1, uint64_t extra) noexcept;
void mark(Stage stage, const Key& key, uint64_t pipeline, uint64_t t) noexcept;
void link(const Key& output, const Key& input, uint64_t pipeline, uint64_t t) noexcept;
}
#endif

}
