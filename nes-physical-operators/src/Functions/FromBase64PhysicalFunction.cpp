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

#include <Functions/FromBase64PhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <openssl/evp.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{

FromBase64PhysicalFunction::FromBase64PhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

namespace
{
/// Native function called via nautilus::invoke to decode base64 using OpenSSL.
/// Returns the actual decoded size written to outputPtr.
uint64_t decodeBase64(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    const auto decodedLen = EVP_DecodeBlock(
        reinterpret_cast<unsigned char*>(outputPtr), reinterpret_cast<const unsigned char*>(inputPtr), static_cast<int>(inputSize));

    if (decodedLen < 0)
    {
        return 0;
    }

    /// EVP_DecodeBlock doesn't account for padding — subtract padding bytes
    auto actualLen = static_cast<uint64_t>(decodedLen);
    if (inputSize >= 1 && inputPtr[inputSize - 1] == '=')
        --actualLen;
    if (inputSize >= 2 && inputPtr[inputSize - 2] == '=')
        --actualLen;
    return actualLen;
}
}

VarVal FromBase64PhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena).getRawValueAs<VariableSizedData>();
    const auto inputSize = inputValue.getSize();

    /// Base64 output is at most 3/4 of input size
    const auto maxOutputSize = inputSize / nautilus::val<uint64_t>(4) * nautilus::val<uint64_t>(3);
    auto output = arena.allocateVariableSizedData(maxOutputSize);

    /// Decode and get actual size
    auto actualSize = nautilus::invoke(decodeBase64, inputValue.getContent(), inputSize, output.getContent());

    /// Re-allocate with the exact size (the arena output already has the data, just wrap with correct size)
    return VariableSizedData(output.getContent(), actualSize);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterFROM_BASE64PhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "FROM_BASE64 function must have exactly one child function");
    return FromBase64PhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}
