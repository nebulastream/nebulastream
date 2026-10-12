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

#include <Functions/ConstructVectorPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VectorData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

ConstructVectorPhysicalFunction::ConstructVectorPhysicalFunction(std::vector<PhysicalFunction> childFunctions, DataType outputType)
    : childFunctions(std::move(childFunctions)), outputType(std::move(outputType))
{
}

VarVal ConstructVectorPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    PRECONDITION(outputType.type == DataType::Type::VECTOR, "ConstructVector expects VECTOR outputType, got {}", outputType);

    /// Infer the space we need to allocate for the vector by multiplying element size with the number of child functions
    const auto totalBytes = outputType.elementType->getSizeInBytesWithoutNull() * childFunctions.size();
    const nautilus::val<int8_t*> outputBuffer = arena.allocateMemory(totalBytes);
    const VectorData out{outputBuffer, *outputType.elementType, totalBytes};

    /// Host-side unroll: `nautilus::static_val<size_t>` makes the index a trace-time
    /// constant so each iteration is emitted as its own ops sequence instead of
    /// becoming a runtime loop the tracer can't fold.
    for (nautilus::static_val<size_t> i = 0; i < childFunctions.size(); ++i)
    {
        out.writeAt(nautilus::val<uint64_t>{i}, childFunctions.at(i).execute(record, arena));
    }
    return VarVal{out, outputType.nullable, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType ConstructVectorPhysicalFunction::createConstructVector(PhysicalFunctionRegistryArguments arguments)
{
    return ConstructVectorPhysicalFunction(std::move(arguments.childFunctions), std::move(arguments.outputType));
}

}
