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
#ifndef NES_COMMON_INCLUDE_UTIL_LATENCY_SYNTHETICTYPE_HPP_
#define NES_COMMON_INCLUDE_UTIL_LATENCY_SYNTHETICTYPE_HPP_

#include <cstdint>
#include <string>

namespace NES::Synthetic::Experimental {

/**
* this enum defines different types workers can have regarding their synthetic coordinate information
*/
enum class SyntheticType : uint8_t {
    NC_DISABLED = 0,  //the worker does not have a known network coordinate
    NC_ENABLED = 1,
    INVALID = 2
};
}// namespace NES::Synthetic::Experimental

#endif// NES_COMMON_INCLUDE_UTIL_LATENCY_SYNTHETICTYPE_HPP_
