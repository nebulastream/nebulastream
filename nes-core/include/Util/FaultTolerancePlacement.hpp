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

#ifndef NES_CORE_INCLUDE_UTIL_FAULTTOLERANCEPLACEMENT_HPP_
#define NES_CORE_INCLUDE_UTIL_FAULTTOLERANCEPLACEMENT_HPP_
#include <cinttypes>
#include <stdint.h>
#include <string>
#include <unordered_map>

namespace NES {
class FaultTolerancePlacement {

  public:
    enum Value : uint8_t { NONE = 0, NAIVE = 1, MFTP = 2, MFTPH = 3 , FLINK = 4, FRONTIER = 5};

    /**
     * @brief Get fault tolerance placement strategy from string
     * @param ftPlacementStrategy : string representation of placement strategy
     * @return enum representing Placement Strategy
     */
    static Value getFromString(const std::string ftPlacementStrategy);

    /**
     * @brief Get fault tolerance placement strategy in string representation
     * @param ftPlacementStrategy : enum value of the Placement Strategy
     * @return string representation of Placement Strategy
     */
    static std::string toString(const Value ftPlacementStrategy);
};

}// namespace NES
#endif// NES_CORE_INCLUDE_UTIL_FAULTTOLERANCEPLACEMENT_HPP_
