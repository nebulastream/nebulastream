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

#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

/// Debug aid: NES_INFO-logs (ts, slot, anomaly_score) for every tuple, then returns the score
/// argument unchanged. See FtLogScoreLogicalFunction for why it must be wired into a WHERE
/// clause's predicate rather than an unused SELECT column.
class FtLogScorePhysicalFunction final
{
public:
    FtLogScorePhysicalFunction(PhysicalFunction ts, PhysicalFunction slot, PhysicalFunction score);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static PhysicalFunctionRegistryReturnType createFT_LOG_SCORE(PhysicalFunctionRegistryArguments arguments);

private:
    PhysicalFunction ts;
    PhysicalFunction slot;
    PhysicalFunction score;
};

static_assert(PhysicalFunctionConcept<FtLogScorePhysicalFunction>);

}
