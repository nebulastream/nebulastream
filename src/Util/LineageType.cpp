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

#include <Util/LineageType.hpp>
#include <string>

namespace NES {
LineageType stringToLineageTypeMap(const std::string lineageMode) {
    if (lineageMode == "NONE") {
        return LineageType::NONE;
    } else if (lineageMode == "IN_MEMORY") {
        return LineageType::IN_MEMORY;
    } else if (lineageMode == "PERSISTENT") {
        return LineageType::PERSISTENT;
    } else if (lineageMode == "REMOTE") {
        return LineageType::REMOTE;
    } else {
        return LineageType::INVALID;
    }
}
}// namespace NES
