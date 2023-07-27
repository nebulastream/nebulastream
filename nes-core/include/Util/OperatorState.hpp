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

#ifndef NES_CORE_INCLUDE_UTIL_OPERATORSTATE_HPP_
#define NES_CORE_INCLUDE_UTIL_OPERATORSTATE_HPP_

#include <cinttypes>
#include <string>
#include <unordered_map>

namespace NES {
/**
 * @brief Represents various states the operator goes through.
 *
 * TO_BE_PLACED: Operator is newly added and yet to be placed.
 * TO_BE_REMOVED: Operator is marked for removal.
 * TO_BE_REPLACED: Operator was placed and deployed somewhere but need to be unplaced, undeploy, placed, and deployed.
 * PLACED: Operator is placed and deployed.
 * REMOVED: operator is undeployed and is removed.
 */
enum class OperatorState : uint8_t { TO_BE_PLACED = 0, TO_BE_REMOVED, TO_BE_REPLACED, PLACED, REMOVED };

}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_OPERATORSTATE_HPP_
