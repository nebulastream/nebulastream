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

#include <memory>
#include <string>

#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using HashFunctionRegistryReturnType = std::unique_ptr<HashFunction>;

struct HashFunctionRegistryArguments
{
};

class HashFunctionRegistry
    : public BaseRegistry<HashFunctionRegistry, std::string, HashFunctionRegistryReturnType, HashFunctionRegistryArguments>
{
};

}

#define INCLUDED_FROM_HASH_FUNCTION_REGISTRY
#include <HashFunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_HASH_FUNCTION_REGISTRY
