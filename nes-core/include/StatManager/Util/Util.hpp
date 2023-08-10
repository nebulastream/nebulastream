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

#ifndef NES_CORE_INCLUDE_STATMANAGER_UTIL_UTIL_HPP
#define NES_CORE_INCLUDE_STATMANAGER_UTIL_UTIL_HPP

#include <cstdint>
#include <cmath>

namespace NES::Experimental::Statistics {

  uint32_t roundUpToNextPowerOf2(uint32_t x);
  uint64_t roundUpToNextPowerOf2(uint64_t x);
  bool isPowerOfTwo(uint32_t n);
  double_t logBaseN(double_t x, double_t base);

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_UTIL_UTIL_HPP
