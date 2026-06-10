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

#include <PointCircleContainsPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <numbers>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <std/cmath.h>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>

namespace NES
{

PointCircleContainsPhysicalFunction::PointCircleContainsPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal PointCircleContainsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto pointStruct = leftPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto xPoint = pointStruct.at("x").getRawValueAs<nautilus::val<double>>();
    const auto yPoint = pointStruct.at("y").getRawValueAs<nautilus::val<double>>();

    const auto circleStruct = rightPhysicalFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto radius = circleStruct.at("radius").getRawValueAs<nautilus::val<double>>();
    const auto center = circleStruct.at("center").getRawValueAs<StructData>();
    const auto xCenter = center.at("x").getRawValueAs<nautilus::val<double>>();
    const auto yCenter = center.at("y").getRawValueAs<nautilus::val<double>>();

    const auto dx = xPoint - xCenter;
    const auto dy = yPoint - yCenter;
    const auto centerDistance = nautilus::sqrt(dx * dx + dy * dy);
    return VarVal{nautilus::val<bool>{centerDistance <= radius}, false, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPOINT_CIRCLE_CONTAINSPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "PointCircleContains expects exactly two children functions");
    return PointCircleContainsPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}
