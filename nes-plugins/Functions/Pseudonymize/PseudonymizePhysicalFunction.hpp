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

#pragma once

#include <string>  // added: needed for std::string secretKey

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Physical function that pseudonymizes an integer value.
/// The secret key is loaded once from the environment variable
/// PSEUDONYM_SECRET_KEY at construction time and reused for every record.
class PseudonymizePhysicalFunction final
{
public:
    explicit PseudonymizePhysicalFunction(PhysicalFunction childPhysicalFunction);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction childPhysicalFunction;
    std::string secretKey;  // added: holds the HMAC key for the lifetime of this object
};

static_assert(PhysicalFunctionConcept<PseudonymizePhysicalFunction>);

}