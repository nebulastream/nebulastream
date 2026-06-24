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

#include "PseudonymizePhysicalFunction.hpp"

#include <utility>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_arith.hpp>

#include <openssl/hmac.h>
#include <openssl/evp.h>

namespace NES
{

namespace {
    thread_local const std::string* tl_secretKey = nullptr;
}

PseudonymizePhysicalFunction::PseudonymizePhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
    const char* key = std::getenv("PSEUDONYM_SECRET_KEY");
    if (key == nullptr)
    {
        throw std::runtime_error(
            "[PseudonymizePhysicalFunction] PSEUDONYM_SECRET_KEY is not set. "
            "Run: export PSEUDONYM_SECRET_KEY=\"your-secret-key\""
        );
    }
    secretKey = std::string(key);
}

namespace {

template <typename T>
T generateGenericHMAC(T inputId)
{
    const std::string* keyPtr = tl_secretKey;
    unsigned char hmacResult[EVP_MAX_MD_SIZE];
    unsigned int  hmacLen = 0;
    HMAC(
        EVP_sha256(),
        keyPtr->data(),
        static_cast<int>(keyPtr->size()),
        reinterpret_cast<const unsigned char*>(&inputId),
        sizeof(inputId),
        hmacResult,
        &hmacLen
    );
    T result = 0;
    std::memcpy(&result, hmacResult, sizeof(result));
    return result > 0 ? result : -result;
}

uint64_t generateHMACString(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    const std::string* keyPtr = tl_secretKey;
    unsigned char hmacResult[EVP_MAX_MD_SIZE];
    unsigned int  hmacLen = 0;
    HMAC(
        EVP_sha256(),
        keyPtr->data(),
        static_cast<int>(keyPtr->size()),
        reinterpret_cast<const unsigned char*>(inputPtr),
        static_cast<int>(inputSize),
        hmacResult,
        &hmacLen
    );
    static const char hexChars[] = "0123456789abcdef";
    for (unsigned int i = 0; i < hmacLen; ++i)
    {
        outputPtr[i * 2]     = static_cast<int8_t>(hexChars[(hmacResult[i] >> 4) & 0xF]);
        outputPtr[i * 2 + 1] = static_cast<int8_t>(hexChars[hmacResult[i] & 0xF]);
    }
    return static_cast<uint64_t>(hmacLen * 2);
}

} // anonymous namespace

VarVal PseudonymizePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    tl_secretKey = &secretKey;
    const auto inputValue = childPhysicalFunction.execute(record, arena);

    return inputValue.customVisit([&](auto&& arg) -> VarVal
    {
        using ArgType = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<ArgType, nautilus::val<int8_t>>)
        {
            auto pseudoId = nautilus::invoke(generateGenericHMAC<int8_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int16_t>>)
        {
            auto pseudoId = nautilus::invoke(generateGenericHMAC<int16_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int32_t>>)
        {
            auto pseudoId = nautilus::invoke(generateGenericHMAC<int32_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int64_t>>)
        {
            auto pseudoId = nautilus::invoke(generateGenericHMAC<int64_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, VariableSizedData>)
        {
            const auto inputSize = arg.getSize();
            const auto outputSize = nautilus::val<uint64_t>(64);
            auto output = arena.allocateVariableSizedData(outputSize);
            auto actualSize = nautilus::invoke(
                generateHMACString,
                arg.getContent(),
                inputSize,
                output.getContent()
            );
            return VariableSizedData(output.getContent(), actualSize);
        }
        else
        {
            throw std::runtime_error(
                "[PseudonymizePhysicalFunction] Unsupported input data type. "
                "Supported types: INT8, INT16, INT32, INT64, VARSIZED"
            );
        }
    });
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterPseudonymizePhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1,
        "Pseudonymize function must have exactly one child function");
    return PseudonymizePhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

} // namespace NES