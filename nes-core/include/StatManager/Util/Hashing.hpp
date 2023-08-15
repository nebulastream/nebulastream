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

namespace NES::Experimental::Statistics {

  /**
   * @brief a class which implements the family of H3 hashing functions
   */
  class H3 {
  public:
      /**
       * @brief produces a H3 hash value given a seed array q
       * @param key the key to be hashed
       * @param q a pointer to a seed array q
       * @return a hash value for the key
       */
      uint32_t hashH3(uint32_t key, uint32_t* q);

      /**
       * @brief returns a pointer to the seed array of the H3 object
       * @return mQ
       */
      uint32_t* getQ();

      /**
       * @brief the constructor to create a family of hash functions
       * @param depth the number of hash functions for the H3 object
       * @param width the hashing interval will be [0, width - 1]
       */
      H3(uint32_t depth, uint32_t width);

    private:
      std::vector<uint32_t> mQ;
  };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_UTIL_HASHING_HPP
