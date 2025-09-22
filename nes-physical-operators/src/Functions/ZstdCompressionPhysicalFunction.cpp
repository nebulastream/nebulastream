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

// #include <Functions/ZstdCompressionPhysicalFunction.hpp>
#include <Functions/ZstdCompressionPhysicalFunction.hpp>

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

VarVal ZstdCompressionPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto value = childPhysicalFunction.execute(record, arena);
    auto varSizedValue = value.cast<VariableSizedData>();

    auto maxCompressedSize = nautilus::invoke(
        +[](size_t inputSize)
        {
            return  ZSTD_compressBound(inputSize);
        },
        varSizedValue.getContentSize()
        );

    // //TODO can we also call this outside of nautilus to avoid calling invoke twice?
    auto compressedVarValSize = maxCompressedSize + nautilus::val<size_t>(sizeof(uint32_t));
    auto memCompressed = arena.allocateMemory(compressedVarValSize);
    VariableSizedData compressedData(memCompressed);

   nautilus::invoke(
        +[](size_t inputSize, int8_t* inputData, size_t compressedMaxSize, int8_t* compressedData)
        {
            size_t compressedSize = ZSTD_compress(compressedData, compressedMaxSize,
                                          inputData, inputSize, 3);

        },
        varSizedValue.getContentSize(),
        varSizedValue.getContent(),
        compressedData.getContentSize(),
        compressedData.getContent()
        );


        return compressedData;
}

ZstdCompressionPhysicalFunction::ZstdCompressionPhysicalFunction(PhysicalFunction childPhysicalFunction) : childPhysicalFunction(childPhysicalFunction)
{
}

// PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterCompressionPhysicalFunction(
//     PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
// {
//     return CompressionPhysicalFunction();
// }

}
