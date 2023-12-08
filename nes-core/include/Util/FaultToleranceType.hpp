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

#ifndef NES_CORE_INCLUDE_UTIL_FAULTTOLERANCETYPE_HPP_
#define NES_CORE_INCLUDE_UTIL_FAULTTOLERANCETYPE_HPP_
#include <stdint.h>
#include <string>
#include <unordered_map>
namespace NES {
class FaultToleranceType {
  public:
    enum Value : uint8_t {
        NONE = 0,         ///No fault-tolerance
        LOW = 1, ///At-most-once guarantee: some tuple buffers might be dropped
        HIGH = 2,///At-least-once guarantee: some tuple buffers might be processed twice
        MEDIUM = 3, ///Exactly-once guarantee: all tuple buffers are processed once
        INVALID = 4
    };

    /**
     * @brief Get fault tolerance type in string representation
     * @param faultToleranceMode : enum value of the fault tolerance type
     * @return string representation of fault tolerance type
     */
    static std::string toString(const Value faultToleranceMode);

    /**
     * @brief Get fault tolerance type from string
     * @param faultToleranceMode : string representation of fault tolerance type
     * @return enum representing fault tolerance type
     */
    static Value getFromString(const std::string faultToleranceMode);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_FAULTTOLERANCETYPE_HPP_
