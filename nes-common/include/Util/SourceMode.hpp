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

#ifndef NES_COMMON_INCLUDE_UTIL_SOURCEMODE_HPP_
#define NES_COMMON_INCLUDE_UTIL_SOURCEMODE_HPP_

#include <cinttypes>
#include <stdint.h>
#include <string>

namespace NES {
enum class SourceMode : uint8_t {
    EMPTY_BUFFER = 0,
    WRAP_BUFFER = 1,
    CACHE_COPY = 2,
    COPY_BUFFER = 3,
    COPY_BUFFER_SIMD_RTE = 4,
    COPY_BUFFER_SIMD_APEX = 5
};
}// namespace NES

#endif  // NES_COMMON_INCLUDE_UTIL_SOURCEMODE_HPP_
