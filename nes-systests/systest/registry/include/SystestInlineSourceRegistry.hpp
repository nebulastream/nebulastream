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
#include <NebuLI.hpp>
#include <SystestParser.hpp>

#include <API/Schema.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using SystestInlineSourceRegistryReturnType = CLI::PhysicalSource;
struct SystestInlineSourceRegistryArguments
{
    CLI::PhysicalSource physicalSourceConfig;
    Systest::SystestParser::AttachSource attachSource;
    std::filesystem::path testFilePath;

};

class SystestInlineSourceRegistry
    : public BaseRegistry<SystestInlineSourceRegistry, std::string, SystestInlineSourceRegistryReturnType, SystestInlineSourceRegistryArguments>
{
};

}

#define INCLUDED_FROM_SYSTEST_INLINE_SOURCE_REGISTRY
#include <SystestInlineSourceGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SYSTEST_INLINE_SOURCE_REGISTRY
