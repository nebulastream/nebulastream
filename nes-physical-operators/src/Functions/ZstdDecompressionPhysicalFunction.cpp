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

#include <Functions/ZstdDecompressionPhysicalFunction.hpp>

#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <zstd.h>
#include <nautilus/function.hpp>

namespace NES
{

VarVal ZstdDecompressionPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childPhysicalFunction.execute(record, arena);
    auto varSizedValue = value.cast<VariableSizedData>();

    auto decompressedSize = nautilus::invoke(
        +[](uint8_t* compressed, size_t inputSize)
        {
            return  ZSTD_getFrameContentSize(compressed, inputSize);
        },
        varSizedValue.getContent(),
        varSizedValue.getContentSize()
        );

    // //TODO can we also call this outside of nautilus to avoid calling invoke twice?
    auto decompressedVarValSize = decompressedSize + nautilus::val<size_t>(sizeof(uint32_t));
    auto memDecompressed = arena.allocateMemory(decompressedVarValSize);
    VariableSizedData decompressedData(memDecompressed);

   nautilus::invoke(
        +[](size_t inputSize, int8_t* inputData, size_t decompressedSize, int8_t* decompressedData)
        {
            size_t compressedSize = ZSTD_decompress(decompressedData, decompressedSize,
                                          inputData, inputSize);
            //TODO: error handling?
        },
        varSizedValue.getContentSize(),
        varSizedValue.getContent(),
        decompressedData.getContentSize(),
        decompressedData.getContent()
        );

        return decompressedData;
}

ZstdDecompressionPhysicalFunction::ZstdDecompressionPhysicalFunction(PhysicalFunction childPhysicalFunction) : childPhysicalFunction(childPhysicalFunction)
{
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterZstdDecompressionPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "Zstd compression function must have exactly onet  sub-function");
    return ZstdDecompressionPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

}
