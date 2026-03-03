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
#include <cstdint>
#include <memory>
#include <string>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <Util/Registry.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
class DefaultLazyValueRepresentation;

using LazyValueRepresentationRegistryReturnType = std::shared_ptr<LazyValueRepresentation>;

struct LazyValueRepresentationRegistryArguments
{
    nautilus::val<int8_t*> valueAddress;
    nautilus::val<uint64_t> size;
    DataType::Type type;
};

class LazyValueRepresentationRegistry : public BaseRegistry<
                                            LazyValueRepresentationRegistry,
                                            std::string,
                                            LazyValueRepresentationRegistryReturnType,
                                            LazyValueRepresentationRegistryArguments>
{
};
}

#define INCLUDED_FROM_LAZYVALUEREPRESENTATION_REGISTRY
#include <LazyValueRepresentationGeneratedRegistrar.inc>
#undef INCLUDED_FROM_LAZYVALUEREPRESENTATION_REGISTRY
