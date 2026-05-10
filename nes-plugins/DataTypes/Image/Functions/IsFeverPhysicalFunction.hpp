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

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Per-record `is_fever(ThermalFrame) -> BOOLEAN`. Aggregate semantics: true
/// iff any pixel exceeds the fever threshold. Threshold is encoded in the
/// raw mono16 (centi-Kelvin) representation directly to avoid converting
/// every pixel.
class IsFeverPhysicalFunction final
{
public:
    explicit IsFeverPhysicalFunction(PhysicalFunction childFunction);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction childFunction;
};

static_assert(PhysicalFunctionConcept<IsFeverPhysicalFunction>);

}
