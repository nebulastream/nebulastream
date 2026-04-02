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

/// Work stealing strategy for idle worker threads in per-thread queue modes.
/// Determines how a worker selects a victim queue to steal from when its own queue is empty.
///
/// NONE: No stealing. Worker falls through directly to the admission queue.
/// ROUND_ROBIN: Try victims in order starting from the next neighbor (i+1, i+2, ...).
/// RANDOM: Pick a single random victim queue.
/// CHOOSE_TWO: Sample two random victim queues, steal from the one with more tasks.
/// LARGEST_QUEUE: Steal from the queue with the most pending tasks (scan all queues).
enum class WorkStealingStrategy : uint8_t
{
    NONE,
    ROUND_ROBIN,
    RANDOM,
    CHOOSE_TWO,
    LARGEST_QUEUE
};

}
