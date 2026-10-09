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

#include <Functions/ConstructArrayPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/FixedSizedData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/val.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <static.hpp>

namespace NES
{

ConstructArrayPhysicalFunction::ConstructArrayPhysicalFunction(std::vector<PhysicalFunction> childFunctions, DataType outputType)
    : childFunctions(std::move(childFunctions)), outputType(std::move(outputType))
{
}

VarVal ConstructArrayPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    PRECONDITION(outputType.type == DataType::Type::FIXEDSIZED, "ConstructArray expects ARRAY outputType, got {}", outputType);
    PRECONDITION(
        childFunctions.size() == outputType.count,
        "ConstructArray child count ({}) must match type count {}",
        childFunctions.size(),
        outputType.structName,
        outputType.fields.size());

    /// Layout is host-side data — `getSizeInBytesWithoutNull` folds to a constant
    /// at trace time, so the allocation is a single arena bump.
    const auto totalBytes = outputType.getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(totalBytes);
    const FixedSizedData out{outputBuffer, outputType.count, *outputType.elementType};

    /// Host-side unroll: `nautilus::static_val<size_t>` makes the index a trace-time
    /// constant so each iteration is emitted as its own ops sequence instead of
    /// becoming a runtime loop the tracer can't fold.
    for (nautilus::static_val<size_t> i = 0; i < childFunctions.size(); ++i)
    {
        out.writeAt(nautilus::val<uint64_t>{i}, childFunctions[i].execute(record, arena));
    }
    return VarVal{out, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType ConstructArrayPhysicalFunction::createConstructArray(PhysicalFunctionRegistryArguments arguments)
{
    return ConstructArrayPhysicalFunction(std::move(arguments.childFunctions), std::move(arguments.outputType));
}

}
