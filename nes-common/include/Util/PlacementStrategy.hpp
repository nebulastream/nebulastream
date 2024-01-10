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

#ifndef NES_COMMON_INCLUDE_UTIL_PLACEMENTSTRATEGY_HPP_
#define NES_COMMON_INCLUDE_UTIL_PLACEMENTSTRATEGY_HPP_

#include <cinttypes>

namespace NES::Optimizer {
enum class PlacementStrategy : uint8_t {
    TopDown = 0,
    BottomUp = 1,
    IFCOP = 2,
    ILP = 3,
    MlHeuristic = 4,
    ELEGANT_PERFORMANCE = 5,
    ELEGANT_ENERGY = 6,
    ELEGANT_BALANCED = 7
};
}// namespace NES::Optimizer
#endif // NES_COMMON_INCLUDE_UTIL_PLACEMENTSTRATEGY_HPP_
