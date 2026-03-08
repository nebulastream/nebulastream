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

#include <Functions/DayOfPhysicalFunction.hpp>

#include <chrono>
#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>

namespace NES
{

DayOfPhysicalFunction::DayOfPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal DayOfPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
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
    const auto day = nautilus::invoke(
        +[](const uint64_t millis) -> uint64_t
        {
            const std::chrono::sys_time utcTimePoint{std::chrono::milliseconds{millis}};
            const auto utcDays = std::chrono::floor<std::chrono::days>(utcTimePoint);
            const std::chrono::year_month_day utcDate{utcDays};
            return static_cast<uint64_t>(static_cast<unsigned>(utcDate.day()));
        },
        milliSeconds);

    return VarVal{day, value.isNullable(), false};
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterDayOfPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == 1, "DayOfPhysicalFunction must have exactly one child function");
    return DayOfPhysicalFunction(args.childFunctions[0], args.outputType);
}

}
