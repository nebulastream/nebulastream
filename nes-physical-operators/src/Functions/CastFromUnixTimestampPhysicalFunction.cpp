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

#include <Functions/CastFromUnixTimestampPhysicalFunction.hpp>

#include <chrono>
#include <cstdint>
#include <format>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <val_arith.hpp>

namespace NES
{

CastFromUnixTimestampPhysicalFunction::CastFromUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastFromUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);

    /// For non-nullable inputs this is a compile-time false, folded away by the trace.
    const nautilus::val<bool> valueIsNull = value.isNullable() ? value.isNull() : nautilus::val<bool>{false};

    /// ISO-8601 UTC format: YYYY-MM-DDTHH:MM:SS.mmmZ (24 chars). Skip the allocation for nulls.
    constexpr uint32_t iso8601OutputLen = 24;
    const auto allocSize = nautilus::select(valueIsNull, nautilus::val<uint32_t>{0}, nautilus::val<uint32_t>{iso8601OutputLen});
    auto timestampAsIso8601 = arena.allocateVariableSizedData(allocSize);

    if (not valueIsNull)
    {
        const auto milliSeconds = value.getRawValueAs<nautilus::val<uint64_t>>();
        nautilus::invoke(
            +[](const uint64_t milliSecondsSinceEpoch, char* outTimestampIso8601Utc)
            {
                const std::chrono::sys_time utcTimePoint{std::chrono::milliseconds{milliSecondsSinceEpoch}};
                std::format_to(outTimestampIso8601Utc, "{:%FT%T}Z", utcTimePoint);
            },
            milliSeconds,
            timestampAsIso8601.getContent());
    }

    return VarVal{timestampAsIso8601, value.isNullable(), valueIsNull};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "CastFromUnixTimestampPhysicalFunction must have exactly one child function");

    return CastFromUnixTimestampPhysicalFunction(args.childFunctions[0], args.outputType);
}

}
