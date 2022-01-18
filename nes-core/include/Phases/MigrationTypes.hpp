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

#ifndef NES_MIGRATIONTYPES_HPP
#define NES_MIGRATIONTYPES_HPP
#include <vector>
#include <cstdint>
namespace NES {

/**
 * @brief these alias represent Migration Types
 */
using MigrationType = uint8_t;
static constexpr uint8_t RESTART = 1;
static constexpr uint8_t MIGRATION_WITH_BUFFERING = 2;
static constexpr uint8_t MIGRATION_WITHOUT_BUFFERING = 3;
inline static const std::vector<MigrationType> migrationTypes = {RESTART, MIGRATION_WITH_BUFFERING, MIGRATION_WITHOUT_BUFFERING};
}
#endif//NES_MIGRATIONTYPES_HPP
