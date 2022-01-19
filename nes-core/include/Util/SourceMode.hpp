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

#ifndef NES_SOURCEMODE_HPP
#define NES_SOURCEMODE_HPP

#include <cinttypes>
#include <stdint.h>
#include <string>

namespace NES {
class SourceMode {

  public:
    enum Value : uint8_t {
        EMPTY_BUFFER = 0,
        WRAP_BUFFER = 1,
        CACHE_COPY = 2,
        COPY_BUFFER = 3,
        COPY_BUFFER_SIMD_RTE = 4,
        COPY_BUFFER_SIMD_APEX = 5
    };

    /**
     * @brief Get Source Mode from string
     * @param sourceMode : string representation of Source mode
     * @return enum representing Source mode
     */
    static Value getFromString(const std::string sourceMode);

    /**
     * @brief Get Source mode in string representation
     * @param sourceMode : enum value of the Source mode
     * @return string representation of Source mode
     */
    static std::string toString(const Value sourceMode);
};
}// namespace NES

#endif//NES_SOURCEMODE_HPP
