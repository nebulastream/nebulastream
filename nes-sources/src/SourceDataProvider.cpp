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

#include <filesystem>


#include <Sources/SourceDataProvider.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES::Sources
{
SystestPhysicalSource SourceDataProvider::provideFileDataSource(
    SystestPhysicalSource initialPhysicalSourceConfig, SystestAttachSource attachSource, std::filesystem::path testDataDir)
{
    const auto theArgs = FileDataRegistryArguments{initialPhysicalSourceConfig, attachSource, testDataDir};
    if (auto physicalSourceConfig = FileDataRegistry::instance().create(attachSource.sourceType, theArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", attachSource.sourceType);
}

SystestPhysicalSource SourceDataProvider::provideInlineDataSource(
    SystestPhysicalSource initialPhysicalSourceConfig, SystestAttachSource attachSource, std::filesystem::path testFilePath)
{
    const auto theArgs = InlineDataRegistryArguments{initialPhysicalSourceConfig, attachSource, testFilePath};
    if (auto physicalSourceConfig = InlineDataRegistry::instance().create(attachSource.sourceType, theArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", attachSource.sourceType);
}
}
