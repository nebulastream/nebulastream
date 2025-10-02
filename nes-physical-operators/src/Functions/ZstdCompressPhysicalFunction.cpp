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

#include <Functions/ZstdCompressPhysicalFunction.hpp>

#include <utility>
#include <vector>
#include <zstd.h>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

VarVal ZstdCompressPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childPhysicalFunction.execute(record, arena);

    // get the size and the memory location of the value
    auto valueMem = nautilus::val<int8_t*>(0);
    auto valueSize = nautilus::val<uint32_t>(0);
    if (type.type == DataType::Type::VARSIZED)
    {
        auto varSizedValueInput = value.cast<VariableSizedData>();
        valueMem = varSizedValueInput.getContent();
        valueSize = varSizedValueInput.getContentSize();
    }
    else
    {
        auto typedValue = value.castToType(type.type);
        valueSize = nautilus::val<uint32_t>(type.getSizeInBytes());
        //TODO can this be done zero copy?
        valueMem = arena.allocateMemory(valueSize);
        typedValue.writeToMemory(valueMem);
    }

    //compress the data
    auto maxCompressedSize = nautilus::invoke(+[](size_t inputSize) { return ZSTD_compressBound(inputSize); }, valueSize);
    auto tempBufferCompressed = arena.allocateMemory(maxCompressedSize);
    auto compressedSize = nautilus::invoke(
        +[](size_t inputSize, int8_t* inputData, size_t compressedMaxSize, int8_t* compressedData)
        {
            //TODO: make compression level configurable
            auto compressionLevel = 3;
            return ZSTD_compress(compressedData, compressedMaxSize, inputData, inputSize, compressionLevel);
        },
        valueSize,
        valueMem,
        maxCompressedSize,
        tempBufferCompressed);

    //TODO: error handling can be inserted here for non positive compressed sizes

    //copy the compressed data to a buffer fitting it's actual size
    auto compressedVarSizedTotalSize = compressedSize + nautilus::val<size_t>(sizeof(uint32_t));
    //this allocation can be optimized away if we use the original buffer. But this requires some additional metadata to handle the
    //case when the compressed data is not actually smaller than the original
    auto memCompressedVarSized = arena.allocateMemory(compressedVarSizedTotalSize);
    nautilus::invoke(
        +[](int8_t* destination, int8_t* source, size_t compressedSize)
        {
            //set the size field of the varsized value
            *std::bit_cast<uint32_t*>(destination) = static_cast<uint32_t>(compressedSize);
            //copy data
            std::memcpy((destination + sizeof(uint32_t)), source, compressedSize);
        },
        memCompressedVarSized,
        tempBufferCompressed,
        compressedSize);

    VariableSizedData compressedDataVarsized(memCompressedVarSized);
    return compressedDataVarsized;
}

ZstdCompressPhysicalFunction::ZstdCompressPhysicalFunction(PhysicalFunction childPhysicalFunction, DataType type)
    : childPhysicalFunction(childPhysicalFunction), type(type)
{
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterZstdCompressPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "Zstd compression function must have exactly one  sub-function");
    auto function = physicalFunctionRegistryArguments.childFunctions[0];
    return ZstdCompressPhysicalFunction(function, physicalFunctionRegistryArguments.dataType);
}

}
