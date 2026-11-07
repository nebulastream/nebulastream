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

#include <DecimalAddPhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/val.hpp>
#include <std/cmath.h>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

DecimalAddPhysicalFunction::DecimalAddPhysicalFunction(PhysicalFunction leftFunction, PhysicalFunction rightFunction, DataType outputType)
    : leftFunction(std::move(leftFunction)), rightFunction(std::move(rightFunction)), outputType(std::move(outputType))
{
}

VarVal DecimalAddPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto left = leftFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto right = rightFunction.execute(record, arena).getRawValueAs<StructData>();

    auto leftUnscaled = left.at("unscaled").getRawValueAs<nautilus::val<int64_t>>();
    auto rightUnscaled = right.at("unscaled").getRawValueAs<nautilus::val<int64_t>>();
    auto leftScale = left.at("scale").getRawValueAs<nautilus::val<int32_t>>();
    auto rightScale = right.at("scale").getRawValueAs<nautilus::val<int32_t>>();

    /// Upscale the value of the lower scale
    nautilus::val<int32_t> newScale{0};
    if (leftScale < rightScale)
    {
        leftUnscaled = leftUnscaled * nautilus::pow(nautilus::val<double>(10), nautilus::val<double>(rightScale - leftScale));
        leftScale = rightScale;
    }
    else if (rightScale < leftScale)
    {
        rightUnscaled = rightUnscaled * nautilus::pow(nautilus::val<double>(10), nautilus::val<double>(leftScale - rightScale));
        rightScale = leftScale;
    }
    const nautilus::val<int64_t> resultUnscaled = leftUnscaled + rightUnscaled;
    /// Allocate space for the output struct
    const auto totalBytes = outputType.getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(totalBytes);
    const StructData out{outputBuffer, outputType.fields};
    out.writeAt(static_cast<size_t>(0), VarVal{resultUnscaled, false, false});
    out.writeAt(static_cast<size_t>(1), VarVal{leftScale, false, false});
    return VarVal{out, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType DecimalAddPhysicalFunction::createAdd_Decimal_Decimal(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "Add_Decimal_Decimal expects exactly two child functions");
    return DecimalAddPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1], arguments.outputType);
}

}
