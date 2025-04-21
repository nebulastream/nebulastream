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

#pragma once

#include <string>

namespace NES
{
/**
 * @brief return the equiJoinName contained in all joinFunctions
 * @param joinFunction : a set of potenitally nested binary functions
 * @param the keyFieldNames as pair
 */
std::pair<std::basic_string<char>, std::basic_string<char>> findEquiJoinKeyNames(std::shared_ptr<NES::NodeFunction> joinFunction);
}
