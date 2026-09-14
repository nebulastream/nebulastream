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

#include <Functions/FtLogScorePhysicalFunction.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

FtLogScorePhysicalFunction::FtLogScorePhysicalFunction(PhysicalFunction ts, PhysicalFunction slot, PhysicalFunction score)
    : ts(std::move(ts)), slot(std::move(slot)), score(std::move(score))
{
}

namespace
{
/// Native function invoked via nautilus::invoke, one call per tuple. `slotPtr`/`slotSize` point at
/// the VariableSizedData's raw bytes (not null-terminated), hence the explicit-length string ctor.
void logFrameScore(uint64_t ts, int8_t* slotPtr, uint64_t slotSize, float score)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char for a log message
    const std::string slotStr(reinterpret_cast<const char*>(slotPtr), static_cast<size_t>(slotSize));
    NES_INFO("FT_LOG_SCORE ts={} slot={} anomaly_score={}", ts, slotStr, score);
}
}

VarVal FtLogScorePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto tsValue = ts.execute(record, arena).getRawValueAs<nautilus::val<uint64_t>>();
    const auto slotValue = slot.execute(record, arena).getRawValueAs<VariableSizedData>();
    const auto scoreValue = score.execute(record, arena);
    const auto scoreFloat = scoreValue.getRawValueAs<nautilus::val<float>>();

    nautilus::invoke(logFrameScore, tsValue, slotValue.getContent(), slotValue.getSize(), scoreFloat);

    return scoreValue;
}

/// NOLINTNEXTLINE(readability-identifier-naming)
PhysicalFunctionRegistryReturnType FtLogScorePhysicalFunction::createFT_LOG_SCORE(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3, "FT_LOG_SCORE must have exactly 3 child functions (ts, slot, score)");
    return FtLogScorePhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.childFunctions[2]);
}

}
