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

#include <string>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <Util/Registry.hpp>
#include <Store.hpp>

namespace NES
{

using StoreTypeRegistryReturnType = StoreManager::Store;

struct StoreTypeRegistryArguments
{
    Schema schema;
    std::unordered_map<std::string, std::string> config;
};

class StoreTypeRegistry : public BaseRegistry<StoreTypeRegistry, std::string, StoreTypeRegistryReturnType, StoreTypeRegistryArguments>
{
};

}

#define INCLUDED_FROM_STORE_TYPE_REGISTRY
#include <StoreTypeGeneratedRegistrar.inc>
#undef INCLUDED_FROM_STORE_TYPE_REGISTRY
