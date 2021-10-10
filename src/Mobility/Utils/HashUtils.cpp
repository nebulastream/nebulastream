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

#include <Mobility/Utils/HashUtils.h>

namespace NES {

uint64_t HashUtils::djb2(unsigned char* buffer, uint32_t size) {
    uint64_t hash = 5381;
    unsigned char c = *buffer;
    uint32_t count = 0;

    while (count < size) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        c = *buffer++;
        count++;
    }
    return hash;
}

}
