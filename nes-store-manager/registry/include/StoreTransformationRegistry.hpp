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

#include <optional>
#include <string>
#include <string_view>

#include <Util/Registry.hpp>
#include <StoreTransformation.hpp>

namespace NES
{

using StoreTransformationRegistryReturnType = StoreManager::StoreTransformation;

struct StoreTransformationRegistryArguments
{
};

class StoreTransformationRegistry : public BaseRegistry<
                                        StoreTransformationRegistry,
                                        std::string,
                                        StoreTransformationRegistryReturnType,
                                        StoreTransformationRegistryArguments>
{
public:
    /// Look up a transformation by source and destination store type names.
    /// Constructs the key as "sourceTypeName_to_destTypeName".
    [[nodiscard]] std::optional<StoreTransformationRegistryReturnType>
    findTransformation(const std::string_view sourceTypeName, const std::string_view destTypeName) const
    {
        const auto key = std::string(sourceTypeName) + "_to_" + std::string(destTypeName);
        return create(key, StoreTransformationRegistryArguments{});
    }
};

}

#define INCLUDED_FROM_STORE_TRANSFORMATION_REGISTRY
#include <StoreTransformationGeneratedRegistrar.inc>
#undef INCLUDED_FROM_STORE_TRANSFORMATION_REGISTRY
