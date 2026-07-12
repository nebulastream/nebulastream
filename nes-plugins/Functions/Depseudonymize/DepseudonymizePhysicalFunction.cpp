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

#include "DepseudonymizePhysicalFunction.hpp"
#include "../Pseudonymize/FeistelCipher.hpp"

#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <utility>

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


DepseudonymizePhysicalFunction::DepseudonymizePhysicalFunction(PhysicalFunction childPhysicalFunction)
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

VarVal DepseudonymizePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    tl_secretKey = &secretKey;
    const auto inputValue = childPhysicalFunction.execute(record, arena);
    if (inputValue.isNull()) return inputValue;

    return inputValue.customVisit([&](auto&& arg) -> VarVal
    {
        using ArgType = std::decay_t<decltype(arg)>;

        if constexpr (std::is_same_v<ArgType, nautilus::val<int8_t>>)
        {
            auto pseudoId = nautilus::invoke(feistelDepseudonymization<int8_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int16_t>>)
        {
            auto pseudoId = nautilus::invoke(feistelDepseudonymization<int16_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int32_t>>)
        {
            auto pseudoId = nautilus::invoke(feistelDepseudonymization<int32_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, nautilus::val<int64_t>>)
        {
            auto pseudoId = nautilus::invoke(feistelDepseudonymization<int64_t>, arg);
            return VarVal(pseudoId);
        }
        else if constexpr (std::is_same_v<ArgType, VariableSizedData>)
        {
            const auto inputSize = arg.getSize();
            const auto outputSize = nautilus::val<uint64_t>(64);
            auto output = arena.allocateVariableSizedData(outputSize);
            auto actualSize = nautilus::invoke(
                depseudonymizeString,
                arg.getContent(),
                inputSize,
                output.getContent()
            );
            return VariableSizedData(output.getContent(), actualSize);
        }
        else
        {
            throw std::runtime_error(
                "[DepseudonymizePhysicalFunction] Unsupported input data type. "
                "Supported types: INT8, INT16, INT32, INT64, VARSIZED"
            );
        }
    });
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterDepseudonymizePhysicalFunction(
    PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(
        physicalFunctionRegistryArguments.childFunctions.size() == 1,
        "Depseudonymize function must have exactly one child function");
    return DepseudonymizePhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

} // namespace NES