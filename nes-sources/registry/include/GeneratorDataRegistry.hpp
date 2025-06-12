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

#include <SystestSources/SourceTypes.hpp>
#include <Util/Registry.hpp>

namespace NES::Sources
{

using GeneratorDataRegistryReturnType = SystestSourceYAMLBinder::PhysicalSource;
struct GeneratorDataRegistryArguments
{
    SystestSourceYAMLBinder::PhysicalSource physicalSourceConfig;
    SystestAttachSource attachSource;
};

class GeneratorDataRegistry
    : public BaseRegistry<GeneratorDataRegistry, std::string, GeneratorDataRegistryReturnType, GeneratorDataRegistryArguments>
{
};

}

#define INCLUDED_FROM_SOURCES_GENERATOR_DATA_REGISTRY
#include <GeneratorDataGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCES_GENERATOR_DATA_REGISTRY
