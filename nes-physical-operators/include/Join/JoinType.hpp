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

#include <cstdint>

namespace NES
{

/// The type of a window join. A CARTESIAN_PRODUCT joins every pair of rows (a join without a condition).
enum class JoinType : uint8_t
{
    INNER_JOIN,
    CARTESIAN_PRODUCT,
    OUTER_LEFT_JOIN,
    OUTER_RIGHT_JOIN,
    OUTER_FULL_JOIN
};

[[nodiscard]] inline bool isOuterJoin(const JoinType joinType) noexcept
{
    return joinType == JoinType::OUTER_LEFT_JOIN || joinType == JoinType::OUTER_RIGHT_JOIN || joinType == JoinType::OUTER_FULL_JOIN;
}

}
