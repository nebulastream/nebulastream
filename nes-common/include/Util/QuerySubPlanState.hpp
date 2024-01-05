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

#ifndef NES_COMMON_INCLUDE_UTIL_QUERYSUBPLANSTATE_HPP_
#define NES_COMMON_INCLUDE_UTIL_QUERYSUBPLANSTATE_HPP_

#include <cinttypes>
#include <string>
#include <unordered_map>

namespace NES {
/**
 * @brief Represents various states the placed query sub plan goes through.
 * Deploy: deploy the query.
 * Running: Query is now running successfully.
 * Stop: mark for soft
 * Stopped: Query was explicitly stopped either by system or by user.
 * Restarting: restarting the query
 * Reconfiguring: reconfigure the placed query plan
 */
enum class QuerySubPlanState : uint8_t {
    DEPLOY = 0,
    RUNNING,
    STOP,
    STOPPED,
    RESTARTING,
    RECONFIGURING,
};

}// namespace NES

#endif// NES_COMMON_INCLUDE_UTIL_QUERYSUBPLANSTATE_HPP_
