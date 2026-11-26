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

#include <cstddef>
#include <functional>
#include <memory>
#include <string>

#include <DataTypes/DataType.hpp>
#include <Internal/InternalCodec.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using InternalCodecRegistryReturnType = std::unique_ptr<InternalCodec>;

struct InternalCodecRegistryArguments
{
    /// DataType of the field that the Codec is applied to.
    DataType fieldType;
};

using InternalCodecFactoryFn = std::function<InternalCodecRegistryReturnType(InternalCodecRegistryArguments)>;

class InternalCodecRegistry : public RuntimeRegistry<InternalCodecRegistry, std::string, InternalCodecFactoryFn, false>
{
public:
    static InternalCodecRegistry& instance();
};

}
