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

#include <Functions/CastToUnixTimestampPhysicalFunction.hpp>

#include <cctype>
#include <chrono>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include <date/date.h>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
std::string_view trim(std::string_view s)
{
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front())))
    {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
    {
        s.remove_suffix(1);
    }
    return s;
}

uint64_t parseWebTimestampToUnixMs(std::string_view raw)
{
    using Ms = std::chrono::milliseconds;
    const auto s = trim(raw);

    if (s.empty())
    {
        throw UnknownOperation("CastToUnixTs: empty timestamp");
    }

    date::sys_time<Ms> tp;

    std::string abbrev;
    std::chrono::minutes offset{0};

    static constexpr const char* formats_with_tz[] = {
        "%FT%T%Ez",
        "%FT%T.%f%Ez",

        "%FT%TZ",
        "%FT%T.%fZ",

        "%a, %d %b %Y %T GMT",
    };

    static constexpr const char* formats_without_tz[] = {
        "%F %T",
        "%F %T.%f",

        "%FT%T",
        "%FT%T.%f",
    };

    for (const char* fmt : formats_with_tz)
    {
        std::istringstream iss(std::string{s});
        abbrev.clear();
        offset = std::chrono::minutes{0};

        date::from_stream(iss, fmt, tp, &abbrev, &offset);

        if (!iss.fail())
        {
            const auto ms = tp.time_since_epoch().count();
            if (ms < 0)
            {
                throw UnknownOperation("CastToUnixTs: pre-epoch timestamp is not supported");
            }
            return static_cast<uint64_t>(ms);
        }
    }


    for (const char* fmt : formats_without_tz)
    {
        std::istringstream iss(std::string{s});
        date::local_time<Ms> ltp;

        date::from_stream(iss, fmt, ltp);

        if (!iss.fail())
        {
            tp = date::sys_time<Ms>{ltp.time_since_epoch()};

            const auto ms = tp.time_since_epoch().count();
            if (ms < 0)
            {
                throw UnknownOperation("CastToUnixTs: pre-epoch timestamp is not supported");
            }
            return static_cast<uint64_t>(ms);
        }
    }

    throw UnknownOperation("CastToUnixTs: unsupported timestamp format: '{}'", std::string{s});
}

}

CastToUnixTimestampPhysicalFunction::CastToUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastToUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);

    const auto var = value.cast<VariableSizedData>();
    const auto size = var.getContentSize();
    const auto ptr = var.getContent();

    const auto tsMs = nautilus::invoke(
        +[](uint32_t sz, int8_t* p) -> uint64_t
        {
            std::string_view sv(reinterpret_cast<const char*>(p), sz);
            return parseWebTimestampToUnixMs(sv);
        },
        size,
        ptr);

    return VarVal(tsMs).castToType(outputType.type);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterCastToUnixTsPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1,
        "CastToUnixTimestampPhysicalFunction must have exactly one child function");

    return CastToUnixTimestampPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.outputType);
}

}
