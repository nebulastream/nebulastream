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

#include <Sample/SampleAggregationPhysicalFunction.hpp>

#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>

namespace NES
{

void SampleAggregationPhysicalFunction::storeNull(const nautilus::val<SampleAggregationState*>& state, const nautilus::val<bool>& isNull)
{
    const auto memAreaNull = static_cast<nautilus::val<int8_t*>>(state);
    VarVal{isNull}.writeToMemory(memAreaNull);
}

nautilus::val<bool> SampleAggregationPhysicalFunction::readNull(const nautilus::val<SampleAggregationState*>& state)
{
    const auto memAreaNull = static_cast<nautilus::val<int8_t*>>(state);
    return readValueFromMemRef<bool>(memAreaNull);
}

}
