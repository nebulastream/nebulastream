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

#include <Functions/SqlPhysicalFunction.hpp>

#include <numbers>
#include <string>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ArithmeticalFunctions/PowPhysicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/NegatePhysicalFunction.hpp>
#include <Functions/BooleanFunctions/OrPhysicalFunction.hpp>
#include <Functions/CastFromUnixTimestampPhysicalFunction.hpp>
#include <Functions/CastToPhysicalFunction.hpp>
#include <Functions/CastToUnixTimestampPhysicalFunction.hpp>
#include <Functions/CastVarSizedToNumericPhysicalFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ExtractFromTimestampPhysicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNullCheckPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
void expectChildren([[maybe_unused]] const PhysicalFunctionRegistryArguments& arguments, [[maybe_unused]] const size_t count, [[maybe_unused]] const std::string_view name)
{
    PRECONDITION(arguments.childFunctions.size() == count, "{} must have {} child functions", name, count);
}

PhysicalFunction isNull(PhysicalFunction child)
{
    return IsNullCheckPhysicalFunction(std::move(child));
}
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createNot_Equal(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 2, "NOT_EQUAL");
    return NegatePhysicalFunction(EqualsPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]));
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createIs_Not_Null(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "IS_NOT_NULL");
    return NegatePhysicalFunction(isNull(arguments.childFunctions[0]));
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createIs_Not_True(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "IS_NOT_TRUE");
    return OrPhysicalFunction(isNull(arguments.childFunctions[0]), NegatePhysicalFunction(arguments.childFunctions[0]));
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createIs_Nan(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "IS_NAN");
    if (not arguments.inputTypes.at(0).isFloat())
    {
        return ConstantBooleanValueFunction(false);
    }
    return NegatePhysicalFunction(EqualsPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[0]));
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createExp(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "EXP");
    return PowPhysicalFunction(ConstantDoubleValueFunction(std::numbers::e), arguments.childFunctions[0], arguments.outputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createNegate(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "NEGATE");
    const auto zero = CastToPhysicalFunction(ConstantInt8ValueFunction(0), arguments.inputTypes.at(0));
    return castIntegerResult(SubPhysicalFunction(zero, arguments.childFunctions[0]), arguments.outputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createCast(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "CAST");
    const auto& inputType = arguments.inputTypes.at(0);
    if (inputType.type == DataType::Type::VARSIZED and arguments.outputType.isNumeric())
    {
        return CastVarSizedToNumericPhysicalFunction(arguments.childFunctions[0], arguments.outputType);
    }
    if (inputType.type == DataType::Type::VARSIZED and arguments.outputType.type == DataType::Type::VARSIZED)
    {
        return arguments.childFunctions[0];
    }
    return CastToPhysicalFunction(arguments.childFunctions[0], arguments.outputType, inputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createTimestamp_Millis(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "TIMESTAMP_MILLIS");
    return CastToPhysicalFunction(arguments.childFunctions[0], arguments.outputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createUnix_Millis(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "UNIX_MILLIS");
    return CastToPhysicalFunction(arguments.childFunctions[0], arguments.outputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createExtract(PhysicalFunctionRegistryArguments arguments)
{
    /// (unit, timestamp) or (unit, indexing, timestamp): Substrait gives units below a year an indexing, of which we have ONE.
    PRECONDITION(arguments.childFunctions.size() == 2 or arguments.childFunctions.size() == 3, "EXTRACT must have 2 or 3 child functions");
    if (arguments.childFunctions.size() == 3 and arguments.literals.at(1) != "ONE")
    {
        throw UnsupportedQuery("EXTRACT supports only the indexing ONE");
    }
    const auto& unit = arguments.literals.at(0);
    const auto timestampUnit = [&]
    {
        if (unit == "DAY")
        {
            return TimestampUnit::Day;
        }
        if (unit == "MONTH")
        {
            return TimestampUnit::Month;
        }
        if (unit == "YEAR")
        {
            return TimestampUnit::Year;
        }
        throw UnsupportedQuery("EXTRACT of {} is not supported, only of DAY, MONTH and YEAR", unit.value_or("a computed unit"));
    }();
    const auto milliseconds = DataTypeProvider::provideDataType(DataType::Type::UINT64, arguments.outputType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return ExtractFromTimestampPhysicalFunction(
        timestampUnit, CastToPhysicalFunction(arguments.childFunctions.back(), milliseconds), arguments.outputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createFormat_Datetime(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 2, "FORMAT_DATETIME");
    static constexpr std::string_view ISO_8601 = "%Y-%m-%dT%H:%M:%E3SZ";
    if (arguments.literals.at(0) != ISO_8601)
    {
        throw UnsupportedQuery("FORMAT_DATETIME supports only the format '{}'", ISO_8601);
    }
    return CastFromUnixTimestampPhysicalFunction(arguments.childFunctions[1], arguments.outputType);
}

PhysicalFunctionRegistryReturnType SqlPhysicalFunction::createTimestamp(PhysicalFunctionRegistryArguments arguments)
{
    expectChildren(arguments, 1, "TIMESTAMP");
    return CastToUnixTimestampPhysicalFunction(arguments.childFunctions[0], arguments.outputType);
}

}
