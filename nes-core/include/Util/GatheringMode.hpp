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

#ifndef NES_INCLUDE_UTIL_GATHERINGMODE_HPP_
#define NES_INCLUDE_UTIL_GATHERINGMODE_HPP_

#include <cinttypes>
#include <stdint.h>
#include <string>

namespace NES {
class GatheringMode {

  public:
    enum Value : uint8_t { FREQUENCY_MODE = 0, INGESTION_RATE_MODE = 1, ADAPTIVE_MODE = 2 };

    /**
     * @brief Get Gathering Mode from string
     * @param gatheringMode : string representation of gathering mode
     * @return enum representing gathering mode
     */
    static Value getFromString(const std::string gatheringMode);

    /**
     * @brief Get Gathering mode in string representation
     * @param gatheringMode : enum value of the gathering mode
     * @return string representation of gathering mode
     */
    static std::string toString(const Value gatheringMode);
};
}// namespace NES

#endif  // NES_INCLUDE_UTIL_GATHERINGMODE_HPP_
