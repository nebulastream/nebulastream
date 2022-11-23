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

#ifndef NES_CORE_INCLUDE_UTIL_QUERYSTATUS_HPP_
#define NES_CORE_INCLUDE_UTIL_QUERYSTATUS_HPP_

#include <cinttypes>
#include <stdint.h>
#include <string>
#include <unordered_map>

namespace NES {

class QueryStatus {

  public:
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
    enum Value : uint8_t {
        Registered = 0,
        Optimizing,
        Deployed,
        Running,
        MarkedForHardStop,
        MarkedForSoftStop,
        SoftStopTriggered,
        SoftStopCompleted,
        Stopped,
        MarkedForFailure,
        Failed,
        Restarting,
        Migrating
    };

    /**
     * @brief Get query status from string
     * @param queryStatus : string representation of query status
     * @return enum representing query status
     */
    static Value getFromString(const std::string queryStatus);

    /**
     * @brief Get query status in string representation
     * @param queryStatus : enum value of the query status
     * @return string representation of query status
     */
    static std::string toString(const Value queryStatus);
};

}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_QUERYSTATUS_HPP_
