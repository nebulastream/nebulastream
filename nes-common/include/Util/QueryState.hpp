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

#ifndef NES_COMMON_INCLUDE_UTIL_QUERYSTATE_HPP_
#define NES_COMMON_INCLUDE_UTIL_QUERYSTATE_HPP_

#include <cinttypes>
#include <string>
#include <unordered_map>

namespace NES {
/**
     * @brief Represents various states the user query goes through.
     *
     * Registered: Query is registered to be scheduled to the worker nodes (added to the queue).
     * Optimizing: Coordinator is optimizing the query.
     * Running: Query is now running successfully.
     * MarkedForHardStop: A request arrived into the system for stopping a query and system marks the query for stopping (added to the queue).
     * MarkedForSoftStop: marked for soft stop after a source completed
     * SoftStopTriggered: soft stop triggered after source completed
     * SoftStopCompleted: soft stop completed after all sources complete
     * Stopped: Query was explicitly stopped either by system or by user.
     * Failed: Query failed because of some reason.
     * Restarting: restarting the query
     * Migrating: migrating query
     */
enum class QueryState : uint8_t {
    REGISTERED = 0,
    OPTIMIZING,
    DEPLOYED,
    RUNNING,
    MARKED_FOR_HARD_STOP,
    MARKED_FOR_SOFT_STOP,
    SOFT_STOP_TRIGGERED,
    SOFT_STOP_COMPLETED,
    STOPPED,
    MARKED_FOR_FAILURE,
    FAILED,
    RESTARTING,
    MIGRATING,
    EXPLAINED,
    RECONFIGURE
};

}// namespace NES

#endif// NES_COMMON_INCLUDE_UTIL_QUERYSTATE_HPP_
