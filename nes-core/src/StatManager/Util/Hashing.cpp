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

#include <random>
#include <StatManager/Util/Hashing.hpp>

namespace NES::Experimental::Statistics {

  uint32_t* H3::getQ() {
    return mQ.data();
  }

  H3::H3(uint32_t depth, uint32_t width) {

    uint8_t BytetoBits = 8;

    uint32_t numberBits = sizeof(width) * BytetoBits;

    std::vector<uint32_t> q;
    q.reserve(numberBits);

    std::mt19937 gen(0);

    // Fill buffers
    for (uint32_t i = 0; i < numberBits * depth; i++) {
      q.push_back(gen());
    }

    mQ = q;
  }

  bool isSetLSB(uint32_t x, uint32_t pos) {
    return (x >> pos) & 1;
  }

  bool parity(uint32_t x) {
    uint8_t shift = 1;

    while (sizeof(x) * 8 / 2 > shift) {
      x = x ^ (x >> shift);
      shift <<= 1;
    }

    return x & 1;
  }

  uint32_t H3::hashH3(uint32_t x, uint32_t* q) {
    uint32_t hash = 0;
    for(uint64_t i = 0; i < sizeof(uint32_t) * 8; i++){
      hash ^= isSetLSB(x,i) * q[i];
    }
    return hash;
  }

  int8_t EH3(uint32_t v, uint32_t seed, bool sbit){
    int8_t res = (uint32_t) (__builtin_parity(v & seed) ^ __builtin_parity(v | (v >> 1)) ^ sbit);
    return (res << 1) - 1;
  }

} // NES::Experimental::Statistics