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

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <Sources/SourceDataProvider.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using InlineDataRegistryReturnType = PhysicalSourceConfig;

struct InlineDataRegistryArguments
{
    PhysicalSourceConfig physicalSourceConfig;
    std::vector<std::string> tuples;
    std::shared_ptr<std::vector<std::jthread>> serverThreads;
    std::filesystem::path testFilePath;
};

class InlineDataRegistry : public BaseRegistry<InlineDataRegistry, std::string, PhysicalSourceConfig, InlineDataRegistryArguments>
{
};

}

#define INCLUDED_FROM_SOURCES_INLINE_DATA_REGISTRY
#include <InlineDataGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCES_INLINE_DATA_REGISTRY
