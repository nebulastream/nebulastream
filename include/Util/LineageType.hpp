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

#ifndef NES_LINEAGETYPE_H
#define NES_LINEAGETYPE_H
#include <unordered_map>

namespace NES {
enum class LineageType : std::int8_t { NONE = 0, IN_MEMORY = 1, PERSISTENT = 2, REMOTE = 3, INVALID = 4 };

static std::unordered_map<std::string, LineageType> const stringToLineageTypeMap = {{"NONE", LineageType::NONE},
                                                                                    {"IN_MEMORY", LineageType::IN_MEMORY},
                                                                                    {"PERSISTENT", LineageType::PERSISTENT},
                                                                                    {"REMOTE", LineageType::REMOTE}};
}// namespace NES
#endif//NES_LINEAGETYPE_H
