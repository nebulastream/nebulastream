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
#ifndef NES_CORE_INCLUDE_UTIL_HASHING_HPP
#define NES_CORE_INCLUDE_UTIL_HASHING_HPP

#include <iostream>
#include <stdint.h>
#include <vector>

namespace NES {

  class H3 {
    public:
      uint32_t hashH3(uint32_t key, uint32_t* q);
      uint32_t* getQ();
      H3(uint32_t depth, uint32_t width);

    private:
      std::vector<uint32_t> mQ;
  };

} // NES

#endif //NES_CORE_INCLUDE_UTIL_HASHING_HPP
