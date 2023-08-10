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

#include "StatManager/Util/Util.hpp"

namespace NES::Experimental::Statistics {

  uint32_t roundUpToNextPowerOf2(uint32_t x) {

    uint32_t i = 1;
    x--;
    while (i <= sizeof(x) * 8) {
      x |= x >> i;
      i <<= 1;
    }
    return ++x;
  }

  uint64_t roundUpToNextPowerOf2(uint64_t x) {

    uint32_t i = 1;
    x--;
    while (i <= sizeof(x) * 8) {
      x |= x >> i;
      i <<= 1;
    }
    return ++x;
  }

  bool isPowerOfTwo(uint32_t n) {
    return ((n & (n - 1)) == 0) && (n != 0);
  }

  double_t logBaseN(double_t x, double_t base) {
    return log(x) / log(base);
  }

} // NES::Experimental::Statistics

