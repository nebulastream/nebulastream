/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_FAULTTOLERANCETYPE_H
#define NES_FAULTTOLERANCETYPE_H
#include <unordered_map>
#include <cstdint>
#include <string>
namespace NES {
enum class FaultToleranceType : std::int8_t {
    NONE = 0,         ///No fault-tolerance
    AT_MOST_ONCE = 1, ///At-most-once guarantee: some tuple buffers might be dropped
    AT_LEAST_ONCE = 2,///At-least-once guarantee: some tuple buffers might be processed twice
    EXACTLY_ONCE = 3, ///Exactly-once guarantee: all tuple buffers are processed once
    INVALID = 4
};

FaultToleranceType stringToFaultToleranceTypeMap(const std::string faultToleranceMode);
}// namespace NES

#endif//NES_FAULTTOLERANCETYPE_H
