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

using SystestAdaptorRegistryReturnType = std::unique_ptr<SystestAdaptor>;
struct SystestAdaptorRegistryArguments
{
    CLI::PhysicalSource physicalSourceConfig;
    Systest::SystestParser::AttachSource attachSource;
    std::optional<std::filesystem::path> testFilePath;
    std::optional<std::filesystem::path> testDataDir;

};

class SystestAdaptorRegistry
    : public BaseRegistry<SystestAdaptorRegistry, std::string, SystestAdaptorRegistryReturnType, SystestAdaptorRegistryArguments>
{
};

}

#define INCLUDED_FROM_SYSTEST_ADAPTOR_REGISTRY
#include <SystestAdaptorGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SYSTEST_ADAPTOR_REGISTRY
