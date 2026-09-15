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

/// Physical function that decodes a JPEG byte buffer once and crops/resizes/packs all 9 fixed HBW
/// slot ROIs into the [1,9,3,64,64] float32 CHW tensor BUCKET_SLOT_CNN_9 expects. See
/// FtPreprocessAllSlotsLogicalFunction for the argument contract.
class FtPreprocessAllSlotsPhysicalFunction final
{
public:
    explicit FtPreprocessAllSlotsPhysicalFunction(PhysicalFunction image);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static PhysicalFunctionRegistryReturnType createFT_PREPROCESS_ALL_SLOTS(PhysicalFunctionRegistryArguments arguments);

private:
    PhysicalFunction image;
};

static_assert(PhysicalFunctionConcept<FtPreprocessAllSlotsPhysicalFunction>);

}
