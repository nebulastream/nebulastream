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

#include <Sources/SourceDataProvider.hpp>

#include <filesystem>

#include <utility>
#include <SystestSources/SourceTypes.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <GeneratorDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES::Sources
{
SystestSourceYAMLBinder::PhysicalSource SourceDataProvider::provideFileDataSource(
    SystestSourceYAMLBinder::PhysicalSource initialPhysicalSourceConfig,
    SystestAttachSource attachSource,
    std::filesystem::path testDataDir)
{
    const auto fileDataArgs = FileDataRegistryArguments{
        .physicalSourceConfig = std::move(initialPhysicalSourceConfig),
        .attachSource = attachSource,
        .testDataDir = std::move(testDataDir)};
    if (auto physicalSourceConfig = FileDataRegistry::instance().create(attachSource.sourceType, fileDataArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", attachSource.sourceType);
}

SystestSourceYAMLBinder::PhysicalSource SourceDataProvider::provideInlineDataSource(
    SystestSourceYAMLBinder::PhysicalSource initialPhysicalSourceConfig,
    SystestAttachSource attachSource,
    std::filesystem::path testFilePath)
{
    const auto inlineDataArgs = InlineDataRegistryArguments{
        .physicalSourceConfig = std::move(initialPhysicalSourceConfig),
        .attachSource = attachSource,
        .testFilePath = std::move(testFilePath)};
    if (auto physicalSourceConfig = InlineDataRegistry::instance().create(attachSource.sourceType, inlineDataArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", attachSource.sourceType);
}

SystestSourceYAMLBinder::PhysicalSource SourceDataProvider::provideGeneratorDataSource(
    SystestSourceYAMLBinder::PhysicalSource initialPhysicalSourceConfig, SystestAttachSource attachSource)
{
    const auto generatorDataArgs
        = GeneratorDataRegistryArguments{.physicalSourceConfig = std::move(initialPhysicalSourceConfig), .attachSource = attachSource};
    if (auto physicalSourceConfig = GeneratorDataRegistry::instance().create(attachSource.sourceType, generatorDataArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", attachSource.sourceType);
}
}
