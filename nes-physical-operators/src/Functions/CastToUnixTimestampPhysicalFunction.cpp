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

#include <cstdint>
#include <exception>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nes-rust-timestamp-bindings/cast_to_unix_timestamp.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>

namespace NES
{

CastToUnixTimestampPhysicalFunction::CastToUnixTimestampPhysicalFunction(PhysicalFunction childFunction, DataType outputType)
    : outputType(std::move(outputType)), childFunction(std::move(childFunction))
{
}

VarVal CastToUnixTimestampPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childFunction.execute(record, arena);
    if (value.isNullable() && value.isNull())
    {
        return VarVal{0, true, true}.castToType(outputType.type);
    }

    const auto var = value.getRawValueAs<VariableSizedData>();
    const auto parsedMilliSeconds = nautilus::invoke(
        +[](const uint32_t size, const char* timestamp) -> uint64_t
        {
            try
            {
                return parse_timestamp_to_unix_milliseconds(rust::Str{timestamp, size});
            }
            catch (const std::exception& error)
            {
                throw FormattingError("CastToUnixTs: {}", error.what());
            }
        },
        var.getSize(),
        var.getContent());

    return VarVal{parsedMilliSeconds, value.isNullable(), false}.castToType(outputType.type);
}

PhysicalFunctionRegistryReturnType CastToUnixTimestampPhysicalFunction::createCastToUnixTs(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1, "CastToUnixTimestampPhysicalFunction must have exactly one child function");
    return CastToUnixTimestampPhysicalFunction(arguments.childFunctions[0], arguments.outputType);
}

}
