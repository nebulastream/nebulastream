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
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

#include <openssl/hmac.h>
#include <openssl/evp.h>

namespace NES
{

// -----------------------------------------------------------------------------
// Thread-local pointer to the key string.
//
// Why thread-local?
//   nautilus::invoke() can only receive types it natively understands
//   (integers, nautilus::val<> wrappers). It cannot accept const char*,
//   int, struct pointers, or any custom type — the Nautilus StateResolver
//   will fail to compile for anything unknown.
//
//   The solution: don't pass the key through invoke() at all.
//   Instead, store a pointer to the key in a thread-local variable that
//   generateHMAC reads directly from memory. Thread-local means each
//   worker thread in NES has its own copy, so there are no data races.
// -----------------------------------------------------------------------------
namespace {
    thread_local const std::string* tl_secretKey = nullptr;
}

// Constructor — loads the key once from the environment at startup
// Throws immediately if the variable is not set
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

// TEMPLATE: A single, generic logic block for all numeric types.
// 'sizeof(inputId)' automatically adapts to the input type (e.g., 4 bytes for INT32, 8 for INT64).
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

} // anonymous namespace

VarVal PseudonymizePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    // Provide the key to the current execution thread right before entering the JIT block
    tl_secretKey = &secretKey;
    const auto inputValue = childPhysicalFunction.execute(record, arena);

    // VISITOR PATTERN: 'customVisit' dynamically unpacks the VarVal box from the inside.
    // 'arg' automatically assumes the exact underlying type (e.g., nautilus::val<int32_t>).
    return inputValue.customVisit([&](auto&& arg) -> VarVal
    {
        using ArgType = std::decay_t<decltype(arg)>;

        // CRITICAL: 'if constexpr' forces the C++ compiler to drop irrelevant branches at compile-time.
        // This prevents Nautilus from crashing due to type mismatches and creates clean,
        // isolated execution paths perfect for MC/DC test coverage.
        if constexpr (std::is_same_v<ArgType, nautilus::val<int32_t>>)
        {
            auto pseudoId = nautilus::invoke(generateGenericHMAC<int32_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int64_t>>)
        {
            auto pseudoId = nautilus::invoke(generateGenericHMAC<int64_t>, arg);
            return VarVal(pseudoId);
        }
        else
        {
            throw std::runtime_error(
                "[PseudonymizePhysicalFunction] Unsupported input data type"
                "Currently only INT32 and INT64 are supported"
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