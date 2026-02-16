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
#include <cstdio>
#include <utility>

#include <date/date.h>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{
namespace
{
static constexpr uint32_t OUT_LEN = 24;

inline void writeZuluTimestamp(char* out, uint64_t msSinceEpoch)
{
    using Ms = std::chrono::milliseconds;
    using Sec = std::chrono::seconds;

    const date::sys_time<Ms> tp{Ms{msSinceEpoch}};
    const auto tpSec = date::floor<Sec>(tp);
    const auto msPart = static_cast<int>(std::chrono::duration_cast<Ms>(tp - tpSec).count());

    const auto dayPoint = date::floor<date::days>(tpSec);
    const date::year_month_day ymd{dayPoint};
    const auto tod = date::make_time(tpSec - dayPoint);

    const int year = int(ymd.year());
    const unsigned month = unsigned(ymd.month());
    const unsigned day = unsigned(ymd.day());

    const int hh = int(tod.hours().count());
    const int mm = int(tod.minutes().count());
    const int ss = int(tod.seconds().count());

    std::snprintf(out, OUT_LEN + 1, "%04d-%02u-%02uT%02d:%02d:%02d.%03dZ", year, month, day, hh, mm, ss, msPart);
}
}

CastFromUnixTimestampPhysicalFunction::CastFromUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastFromUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);

    const auto msVal = value.cast<nautilus::val<uint64_t>>();

    auto outVar = arena.allocateVariableSizedData(nautilus::val<uint32_t>(OUT_LEN));

    nautilus::invoke(
        +[](uint64_t ms, int8_t* payload) { writeZuluTimestamp(reinterpret_cast<char*>(payload), ms); }, msVal, outVar.getContent());

    VarVal(nautilus::val<uint32_t>(OUT_LEN)).writeToMemory(outVar.getReference());

    return outVar;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCastFromUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "CastFromUnixTimestampPhysicalFunction must have exactly one child function");

    return CastFromUnixTimestampPhysicalFunction(args.childFunctions[0], args.outputType);
}

}
