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

#include <Functions/ZstdDecompressPhysicalFunction.hpp>

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

VarVal ZstdDecompressPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childPhysicalFunction.execute(record, arena);
    auto varSizedValueCompressed = value.cast<VariableSizedData>();

    auto decompressedSize = nautilus::invoke(
        +[](uint8_t* compressed, size_t inputSize) { return static_cast<uint64_t>(ZSTD_getFrameContentSize(compressed, inputSize)); },
        varSizedValueCompressed.getContent(),
        varSizedValueCompressed.getContentSize());

    // //TODO can we also call this outside of nautilus to avoid calling invoke twice?
    auto decompressedVarSizedTotalSize = decompressedSize + nautilus::val<size_t>(sizeof(uint32_t));
    auto memDecompressed = arena.allocateMemory(decompressedVarSizedTotalSize);

    nautilus::invoke(
        +[](size_t inputSize, int8_t* inputData, size_t decompressedSize, int8_t* decompressedData)
        {
            //currently we ignore the returned compressed size, checking the actual returned size for error handlin might be desirable
            *std::bit_cast<uint32_t*>(decompressedData) = static_cast<uint32_t>(decompressedSize);
            size_t compressedSize = ZSTD_decompress(decompressedData + sizeof(uint32_t), decompressedSize, inputData, inputSize);
            //TODO: error handling?
        },
        varSizedValueCompressed.getContentSize(),
        varSizedValueCompressed.getContent(),
        decompressedSize,
        memDecompressed);

    VariableSizedData decompressedVarSized(memDecompressed);
    return decompressedVarSized;
}

ZstdDecompressPhysicalFunction::ZstdDecompressPhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(childPhysicalFunction)
{
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterZstdDecompressPhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1, "Zstd compression function must have exactly one sub-function");
    return ZstdDecompressPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

}
