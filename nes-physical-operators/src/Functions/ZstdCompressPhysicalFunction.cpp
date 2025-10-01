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
    if (type.type == DataType::Type::VARSIZED)
    {
        auto varSizedValueInput = value.cast<VariableSizedData>();

        auto maxCompressedSize
            = nautilus::invoke(+[](size_t inputSize) { return ZSTD_compressBound(inputSize); }, varSizedValueInput.getContentSize());

        //TODO can we also call this outside of nautilus to avoid calling invoke twice?
        auto tempBufferCompressed = arena.allocateMemory(maxCompressedSize);

        auto compressedSize = nautilus::invoke(
            +[](size_t inputSize, int8_t* inputData, size_t compressedMaxSize, int8_t* compressedData)
            {
                //TODO: make compression level configurable
                auto compressionLevel = 3;
                return ZSTD_compress(compressedData, compressedMaxSize, inputData, inputSize, compressionLevel);
                //TODO: error handling?
            },
            varSizedValueInput.getContentSize(),
            varSizedValueInput.getContent(),
            maxCompressedSize,
            tempBufferCompressed);

        /*
         * TODO: is this copying really necessary or can we just use the original buffer and supply a shorter size value?
         * The answer depends on:
         * - Does the system at any point assume that a varsized value really uses all the mamory that was allocated for the ref (probably not?)?
         * - Can we return the big allocation to the arena to be reused? Then copying to the smaller buffer allows freeing the much l
        */
        auto compressedVarSizedTotalSize = compressedSize + nautilus::val<size_t>(sizeof(uint32_t));
        auto memCompressedVarSized = arena.allocateMemory(compressedVarSizedTotalSize);

        nautilus::invoke(
            +[](int8_t* destination, int8_t* source, size_t compressedSize)
            {
                *std::bit_cast<uint32_t*>(destination) = static_cast<uint32_t>(compressedSize);
                std::memcpy((destination + sizeof(uint32_t)), source, compressedSize);
            },
            memCompressedVarSized,
            tempBufferCompressed,
            compressedSize);

        VariableSizedData compressedDataVarsized(memCompressedVarSized);
        return compressedDataVarsized;
    }
    else
    {
        auto typedValue = value.castToType(type.type);
        auto valueSize = nautilus::val<uint32_t>(type.getSizeInBytes());
        auto maxCompressedSize = nautilus::invoke(+[](size_t inputSize) { return ZSTD_compressBound(inputSize); }, valueSize);
        auto tempBufferCompressed = arena.allocateMemory(maxCompressedSize);

        //TODO can this be done zero copy?
        auto tempBufferUncompressed = arena.allocateMemory(valueSize);
        typedValue.writeToMemory(tempBufferUncompressed);

        auto compressedSize = nautilus::invoke(
            +[](size_t inputSize, int8_t* inputData, size_t compressedMaxSize, int8_t* compressedData)
            {
                //TODO: make compression level configurable
                auto compressionLevel = 3;
                return ZSTD_compress(compressedData, compressedMaxSize, inputData, inputSize, compressionLevel);
                //TODO: error handling?
            },
            valueSize,
            tempBufferUncompressed,
            maxCompressedSize,
            tempBufferCompressed);
        auto compressedVarSizedTotalSize = compressedSize + nautilus::val<size_t>(sizeof(uint32_t));
        auto memCompressedVarSized = arena.allocateMemory(compressedVarSizedTotalSize);

        nautilus::invoke(
            +[](int8_t* destination, int8_t* source, size_t compressedSize)
            {
                *std::bit_cast<uint32_t*>(destination) = static_cast<uint32_t>(compressedSize);
                std::memcpy((destination + sizeof(uint32_t)), source, compressedSize);
            },
            memCompressedVarSized,
            tempBufferCompressed,
            compressedSize);

        VariableSizedData compressedDataVarsized(memCompressedVarSized);
        return compressedDataVarsized;
    }
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
    return ZstdCompressPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctionTypes[0]);
}

}
