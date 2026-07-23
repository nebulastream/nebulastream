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

#include <Functions/ExtractFromTimestampPhysicalFunction.hpp>

#include <chrono>
#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/ExtractFromTimestampLogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>

namespace NES
{

ExtractFromTimestampPhysicalFunction::ExtractFromTimestampPhysicalFunction(
    TimestampUnit unit, PhysicalFunction childFunction, DataType outputType)
    : unit(unit), outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal ExtractFromTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    if (value.isNullable())
    {
        if (value.isNull())
        {
            return VarVal{0, true, true}.castToType(outputType.type);
        }
    }

    const auto milliSeconds = value.getRawValueAs<nautilus::val<uint64_t>>();
    const auto extracted = [&]() -> nautilus::val<uint64_t>
    {
        switch (unit)
        {
            case TimestampUnit::Day:
                return nautilus::invoke(
                    +[](const uint64_t millis) -> uint64_t
                    {
                        const std::chrono::sys_time utcTimePoint{std::chrono::milliseconds{millis}};
                        const auto utcDays = std::chrono::floor<std::chrono::days>(utcTimePoint);
                        const std::chrono::year_month_day utcDate{utcDays};
                        return static_cast<uint64_t>(static_cast<unsigned>(utcDate.day()));
                    },
                    milliSeconds);
            case TimestampUnit::Month:
                return nautilus::invoke(
                    +[](const uint64_t millis) -> uint64_t
                    {
                        const std::chrono::sys_time utcTimePoint{std::chrono::milliseconds{millis}};
                        const auto utcDays = std::chrono::floor<std::chrono::days>(utcTimePoint);
                        const std::chrono::year_month_day utcDate{utcDays};
                        return static_cast<uint64_t>(static_cast<unsigned>(utcDate.month()));
                    },
                    milliSeconds);
            case TimestampUnit::Year:
                return nautilus::invoke(
                    +[](const uint64_t millis) -> uint64_t
                    {
                        const std::chrono::sys_time utcTimePoint{std::chrono::milliseconds{millis}};
                        const auto utcDays = std::chrono::floor<std::chrono::days>(utcTimePoint);
                        const std::chrono::year_month_day utcDate{utcDays};
                        return static_cast<uint64_t>(static_cast<int>(utcDate.year()));
                    },
                    milliSeconds);
        }
        INVARIANT(false, "Unknown TimestampUnit value: {}", static_cast<int>(unit));
        std::unreachable();
    }();

    return VarVal{extracted, value.isNullable(), false};
}

PhysicalFunctionRegistryReturnType ExtractFromTimestampPhysicalFunction::createDay_Of(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "ExtractFromTimestampPhysicalFunction (Day_Of) must have exactly one child function");
    return ExtractFromTimestampPhysicalFunction{TimestampUnit::Day, args.childFunctions[0], args.outputType};
}

PhysicalFunctionRegistryReturnType ExtractFromTimestampPhysicalFunction::createMonth_Of(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "ExtractFromTimestampPhysicalFunction (Month_Of) must have exactly one child function");
    return ExtractFromTimestampPhysicalFunction{TimestampUnit::Month, args.childFunctions[0], args.outputType};
}

PhysicalFunctionRegistryReturnType ExtractFromTimestampPhysicalFunction::createYear_Of(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "ExtractFromTimestampPhysicalFunction (Year_Of) must have exactly one child function");
    return ExtractFromTimestampPhysicalFunction{TimestampUnit::Year, args.childFunctions[0], args.outputType};
}

}
