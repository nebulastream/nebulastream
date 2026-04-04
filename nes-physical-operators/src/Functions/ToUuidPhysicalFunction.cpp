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

#include <Functions/ToUuidPhysicalFunction.hpp>

#include <array>
#include <bit>
#include <cstdint>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/UUID.hpp>
#include <uuid/uuid.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_bool.hpp>

namespace NES
{

ToUuidPhysicalFunction::ToUuidPhysicalFunction(PhysicalFunction hiFunction, PhysicalFunction loFunction)
    : hiFunction(std::move(hiFunction)), loFunction(std::move(loFunction))
{
}

VarVal ToUuidPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto hiValue = hiFunction.execute(record, arena);
    const auto loValue = loFunction.execute(record, arena);

    const auto hi = hiValue.getRawValueAs<nautilus::val<uint64_t>>();
    const auto lo = loValue.getRawValueAs<nautilus::val<uint64_t>>();

    const auto isNullable = hiValue.isNullable() or loValue.isNullable();
    const auto isNull = isNullable and (hiValue.isNull() or loValue.isNull());

    /// uuid_unparse writes 36 chars + null terminator, allocate 37 but report size as 36
    const nautilus::val<uint64_t> uuidAllocLen{UUID_STRING_LENGTH + 1};
    auto uuidVarData = arena.allocateVariableSizedData(uuidAllocLen);
    const auto outPtr = uuidVarData.getContent();

    nautilus::invoke(
        +[](uint64_t hiRaw, uint64_t loRaw, char* out)
        {
            std::array<uint64_t, 2> halves{hiRaw, loRaw};
            const auto uuid = std::bit_cast<UUID>(halves);
            uuid_unparse(uuid.data(), out);
        },
        hi,
        lo,
        outPtr);

    return VarVal{uuidVarData, isNullable, isNull};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterToUuidPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "ToUuid function must have exactly two child functions");
    return ToUuidPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}
}
