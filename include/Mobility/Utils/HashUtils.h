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

#ifndef NES_HASHUTILS_H
#define NES_HASHUTILS_H

#include <cstdint>

namespace NES {

class HashUtils {
  public:

    /**
     * dbj2 hash function based on: http://www.cse.yorku.ca/~oz/hash.html
     * @param buffer to build the hash
     * @param size number of bytes considered for the hash
     * @return hash value
     */
    static uint64_t djb2(unsigned char*  buffer, uint32_t size);
};

}

#endif//NES_HASHUTILS_H
