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

#include <CircleCircumferencePhysicalFunction.hpp>

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
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>

namespace NES
{

CircleCircumferencePhysicalFunction::CircleCircumferencePhysicalFunction(PhysicalFunction childFunction) : childFunction(std::move(childFunction))
{
}

VarVal CircleCircumferencePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto circleStruct = childFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto radius = circleStruct.at("radius").getRawValueAs<nautilus::val<double>>();
    const nautilus::val<double> area = 2 * radius * std::numbers::pi;
    return VarVal{area, false, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCIRCLE_CIRCUMFERENCEPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1, "CircleCircumference expects exactly one child function");
    return CircleCircumferencePhysicalFunction(arguments.childFunctions[0]);
}

}
