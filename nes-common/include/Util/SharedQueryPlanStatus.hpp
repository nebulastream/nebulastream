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

#ifndef NES_COMMON_INCLUDE_UTIL_SHAREDQUERYPLANSTATUS_HPP_
#define NES_COMMON_INCLUDE_UTIL_SHAREDQUERYPLANSTATUS_HPP_

#include <cinttypes>
#include <stdint.h>
#include <string>
#include <unordered_map>

namespace NES {
/**
     * @brief Represents various states a Shared query Plan goes through.
     *
     * Created: Shared query plan was just created
     * Deployed: Shared Query Plan was deployed successfully.
     * Updated: A request arrived into the system that either added or removed operators from a Shared Query Plan.
     * Stopped: Shared Query Plan was stopped by user.
     * Failed: Shared Query Plan failed because of some reason.
     */
enum class SharedQueryPlanStatus : uint8_t { CREATED = 0, DEPLOYED, UPDATED, STOPPED, FAILED, MIGRATING };

}// namespace NES

#endif // NES_COMMON_INCLUDE_UTIL_SHAREDQUERYPLANSTATUS_HPP_
