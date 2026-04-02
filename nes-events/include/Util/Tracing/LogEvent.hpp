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

#if __has_include(<sys/sdt.h>)
    #include <sys/sdt.h>
#else
    #define DTRACE_PROBE(...)
#endif

#include <Util/Tracing/EventStore.hpp>

/// LOG_EVENT emits both:
///   1. A USDT probe under the "nes" provider (for ad-hoc USDT tracing, zero-overhead NOP when unused)
///   2. An in-process event to the EventStore (lock-free, for system queries)
///
/// Usage:
///   LOG_EVENT(pipeline_compiled, pipeline_id, compile_time_ns)
///   LOG_EVENT(query_status, query_id, "started")
///
/// The name must be a valid EventType enum value.

/// clang-format off


#if __has_include(<sys/sdt.d>)
    #define NES_LOG_EVENT_ARG_COUNT(...) NES_LOG_EVENT_ARG_COUNT_IMPL(__VA_ARGS__, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    #define NES_LOG_EVENT_ARG_COUNT_IMPL(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, N, ...) N
    #define NES_LOG_EVENT_CONCAT(a, b) a##b
    #define NES_LOG_EVENT_DISPATCH(n) NES_LOG_EVENT_CONCAT(DTRACE_PROBE, n)

    #define LOG_EVENT(name, ...) \
        do \
        { \
            NES_LOG_EVENT_DISPATCH(NES_LOG_EVENT_ARG_COUNT(__VA_ARGS__))(nes, name, __VA_ARGS__); \
            NES_EVENT(name, __VA_ARGS__); \
        } while (0)

    #define LOG_EVENT0(name) \
        do \
        { \
            DTRACE_PROBE(nes, name); \
            NES_EVENT0(name); \
        } while (0)

#else
    #define LOG_EVENT(name, ...) \
        do \
        { \
            NES_EVENT(name, __VA_ARGS__); \
        } while (0)

    #define LOG_EVENT0(name) \
        do \
        { \
            NES_EVENT0(name); \
        } while (0)
#endif
/// clang-format on
