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

namespace NES
{

/// Scheduling strategy for task dispatch within the QueryEngine.
/// GLOBAL_QUEUE: All worker threads share a single internal task queue (default, existing behavior).
/// PER_THREAD_ROUND_ROBIN: Each worker thread has its own queue; tasks are assigned round-robin.
/// PER_THREAD_SMALLEST_QUEUE: Each worker thread has its own queue; tasks are assigned to the shortest queue.
enum class SchedulingStrategy : uint8_t
{
    GLOBAL_QUEUE,
    PER_THREAD_ROUND_ROBIN,
    PER_THREAD_SMALLEST_QUEUE
};

}
