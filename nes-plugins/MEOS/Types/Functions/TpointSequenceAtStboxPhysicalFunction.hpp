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

#pragma once

#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

/// Returns only the portion of the TemporalPointSequence that falls into the SpatioTemporalBox
class TpointSequenceAtStboxPhysicalFunction final
{
public:
    explicit TpointSequenceAtStboxPhysicalFunction(
        PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;
    static PhysicalFunctionRegistryReturnType
    createTGEO_AT_STBOX_TemporalPointSequence_SpatioTemporalBox(PhysicalFunctionRegistryArguments arguments);

private:
    PhysicalFunction leftPhysicalFunction;
    PhysicalFunction rightPhysicalFunction;
    DataType outputType;
};

static_assert(PhysicalFunctionConcept<TpointSequenceAtStboxPhysicalFunction>);

}
