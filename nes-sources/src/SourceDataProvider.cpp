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

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES
{
PhysicalSourceConfig SourceDataProvider::provideFileDataSource(
    PhysicalSourceConfig initialPhysicalSourceConfig,
    std::shared_ptr<std::vector<std::jthread>> serverThreads,
    std::filesystem::path testFilePath)
{
    const auto fileDataArgs = FileDataRegistryArguments{
        .physicalSourceConfig = std::move(initialPhysicalSourceConfig),
        .serverThreads = std::move(serverThreads),
        .testFilePath = std::move(testFilePath)};
    if (auto physicalSourceConfig = FileDataRegistry::instance().create(fileDataArgs.physicalSourceConfig.type.asCanonicalString(), fileDataArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", fileDataArgs.physicalSourceConfig.type);
}

PhysicalSourceConfig SourceDataProvider::provideInlineDataSource(
    PhysicalSourceConfig initialPhysicalSourceConfig,
    std::vector<std::string> tuples,
    std::shared_ptr<std::vector<std::jthread>> serverThreads,
    std::filesystem::path testFilePath)
{
    const auto inlineDataArgs = InlineDataRegistryArguments{
        .physicalSourceConfig = std::move(initialPhysicalSourceConfig),
        .tuples = std::move(tuples),
        .serverThreads = std::move(serverThreads),
        .testFilePath = std::move(testFilePath)};
    if (auto physicalSourceConfig = InlineDataRegistry::instance().create(inlineDataArgs.physicalSourceConfig.type.asCanonicalString(), inlineDataArgs))
    {
        return physicalSourceConfig.value();
    }
    throw UnknownSourceType("Source type {} not found.", inlineDataArgs.physicalSourceConfig.type);
}
}
