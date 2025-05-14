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

using InlineDataSourceRegistryReturnType = CLI::PhysicalSource;
struct InlineDataSourceRegistryArguments
{
    CLI::PhysicalSource physicalSourceConfig;
    Systest::SystestParser::AttachSource attachSource;
    std::filesystem::path testFilePath;

};

class InlineDataSourceRegistry
    : public BaseRegistry<InlineDataSourceRegistry, std::string, InlineDataSourceRegistryReturnType, InlineDataSourceRegistryArguments>
{
};

}

#define INCLUDED_FROM_SYSTEST_INLINE_DATA_SOURCE_REGISTRY
#include <InlineDataSourceGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SYSTEST_INLINE_DATA_SOURCE_REGISTRY
