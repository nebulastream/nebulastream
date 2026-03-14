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

#include <Functions/ToBase64PhysicalFunction.hpp>

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

To_Base64PhysicalFunction::To_Base64PhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

namespace
{
/// Native function called via nautilus::invoke to encode bytes to base64 using OpenSSL.
/// Returns the actual encoded size written to outputPtr.
uint64_t encodeBase64(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    const auto encodedLen = EVP_EncodeBlock(
        reinterpret_cast<unsigned char*>(outputPtr), reinterpret_cast<const unsigned char*>(inputPtr), static_cast<int>(inputSize));
    return static_cast<uint64_t>(encodedLen);
}
}

VarVal To_Base64PhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena).getRawValueAs<VariableSizedData>();
    const auto inputSize = inputValue.getSize();

    /// Base64 output is at most 4/3 of input size + padding, rounded up to multiple of 4
    const auto maxOutputSize = (inputSize + nautilus::val<uint64_t>(2)) / nautilus::val<uint64_t>(3) * nautilus::val<uint64_t>(4);
    auto output = arena.allocateVariableSizedData(maxOutputSize);

    auto actualSize = nautilus::invoke(encodeBase64, inputValue.getContent(), inputSize, output.getContent());

    return VariableSizedData(output.getContent(), actualSize);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTo_Base64PhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "TO_BASE64 function must have exactly one child function");
    return To_Base64PhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}
