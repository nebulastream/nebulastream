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

#include <Runtime/NodeEngineBuilder.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Allocator/ComposeFixedMemoryResource.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Allocator/VmCacheMemoryResource.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceProvider.hpp>
#include <ErrorHandling.hpp>
#include <QueryEngine.hpp>

namespace NES
{


NodeEngineBuilder::NodeEngineBuilder(const WorkerConfiguration& workerConfiguration, std::shared_ptr<StatisticListener> statisticsListener)
    : workerConfiguration(workerConfiguration), statisticsListener(std::move(statisticsListener))
{
}

std::optional<SizeClassConfig> NodeEngineBuilder::makeSizeClassConfig(const WorkerConfiguration& workerConfiguration)
{
    if (!workerConfiguration.enableBufferSizeClasses.getValue())
    {
        return std::nullopt;
    }
    const auto minClassSize = workerConfiguration.bufferSizeClassMinBytes.getValue();
    const auto maxClassSize = workerConfiguration.bufferSizeClassMaxBytes.getValue();
    if (minClassSize > maxClassSize)
    {
        throw InvalidConfigParameter(
            "buffer_size_class_min_bytes ({}) must be <= buffer_size_class_max_bytes ({})", minClassSize, maxClassSize);
    }
    SizeClassConfig config{.minClassSize = minClassSize, .maxClassSize = maxClassSize};
    config.policy = workerConfiguration.bufferSizeClassProvisioning.getValue();
    config.totalBudgetBytes = workerConfiguration.bufferSizeClassBudgetBytes.getValue();
    config.buffersPerClass = workerConfiguration.bufferSizeClassBuffersPerClass.getValue();
    /// For LazyElastic, buffer_size_class_buffers_per_class (when set) also raises the per-class *growth
    /// ceiling*, so a large-state query can grow its hot classes on demand instead of hitting the default
    /// 4096-buffer cap. Lazy, so a high ceiling only faults in what is actually used.
    if (config.policy == BufferProvisioningPolicy::LazyElastic && config.buffersPerClass > 0)
    {
        config.maxBuffersPerClass = config.buffersPerClass;
    }
    return config;
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(const Host& host)
{
    /// Select the allocator for variable-sized requests (the A1/A2/A3 design alternatives). ComposeFixed and
    /// VmCache back the variable-sized path with a large mmap arena (MAP_NORESERVE, so resident tracks touched
    /// pages) and force the size-class path off; the arena size is itself the budget for those modes.
    static constexpr size_t VARIABLE_ARENA_BYTES = std::size_t{64} << 30;
    std::shared_ptr<std::pmr::memory_resource> variableAllocator;
    std::optional<SizeClassConfig> sizeClassConfig;
    switch (workerConfiguration.variableSizeAllocator.getValue())
    {
        case VariableSizeAllocator::ComposeFixed:
            variableAllocator = std::make_shared<ComposeFixedMemoryResource>(
                VARIABLE_ARENA_BYTES, workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue());
            sizeClassConfig = std::nullopt;
            break;
        case VariableSizeAllocator::VmCache:
            variableAllocator = std::make_shared<VmCacheMemoryResource>(VARIABLE_ARENA_BYTES);
            sizeClassConfig = std::nullopt;
            break;
        case VariableSizeAllocator::Default:
            variableAllocator = std::make_shared<NesDefaultMemoryAllocator>();
            sizeClassConfig = makeSizeClassConfig(workerConfiguration);
            break;
    }
    auto bufferManager = BufferManager::create(
        workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue(),
        workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
        variableAllocator,
        sizeClassConfig);
    auto queryLog = std::make_shared<QueryLog>();

    auto queryEngine = std::make_unique<QueryEngine>(workerConfiguration.queryEngine, statisticsListener, queryLog, bufferManager, host);

    auto sourceProvider = std::make_unique<SourceProvider>(workerConfiguration.defaultMaxInflightBuffers.getValue(), bufferManager);

    return std::make_unique<NodeEngine>(
        std::move(bufferManager), statisticsListener, std::move(queryLog), std::move(queryEngine), std::move(sourceProvider));
}

}
