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

#include <IsFeverPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
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

IsFeverPhysicalFunction::IsFeverPhysicalFunction(PhysicalFunction childFunction) : childFunction(std::move(childFunction))
{
}

VarVal IsFeverPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    constexpr uint16_t feverThresholdCentiKelvin = 31115;
    const auto frameStruct = childFunction.execute(record, arena).getRawValueAs<StructData>();
    const auto pixelsView = frameStruct.at("pixels").getRawValueAs<FixedSizedData>();

    for (nautilus::val<size_t> i; i < pixelsView.getNumElements(); ++i)
    {
        if (pixelsView.at(i) >= nautilus::val<uint16_t>(feverThresholdCentiKelvin))
        {
            return VarVal{true, false, nautilus::val<bool>{false}};
        }
    }
    return VarVal{false, false, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterIS_FEVERPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1, "IsFever expects exactly one child function");
    return IsFeverPhysicalFunction(arguments.childFunctions[0]);
}

}
