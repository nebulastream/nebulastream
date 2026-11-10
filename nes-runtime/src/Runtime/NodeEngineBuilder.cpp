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

#include <cstdint>
#include <exception>
#include <memory>
#include <memory_resource>
#include <thread>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Allocator/SharedMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceProvider.hpp>
#include <CpuPinning.hpp>
#include <QueryEngine.hpp>

namespace NES
{


NodeEngineBuilder::NodeEngineBuilder(const WorkerConfiguration& workerConfiguration, std::shared_ptr<StatisticListener> statisticsListener)
    : workerConfiguration(workerConfiguration), statisticsListener(std::move(statisticsListener))
{
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(const Host& host)
{
    /// Processes of out-of-process UDFs read and write the buffers of the pool in place, without copying them, if it is shared memory.
    const bool sharedPool = workerConfiguration.pythonUdfProcessProtocol == "SHARED_MEMORY_ZERO_COPY";
    const auto createBufferManager = [&]
    {
        return BufferManager::create(
            workerConfiguration.totalMemoryInBytes,
            workerConfiguration.unpooledMemoryFraction,
            NES::BufferAlignment{static_cast<uint32_t>(workerConfiguration.bufferAlignmentInBytes)},
            static_cast<uint32_t>(workerConfiguration.defaultQueryExecution.operatorBufferSize),
            sharedPool ? std::shared_ptr<std::pmr::memory_resource>{std::make_shared<SharedMemoryAllocator>()}
                       : std::shared_ptr<std::pmr::memory_resource>{std::make_shared<NesDefaultMemoryAllocator>()});
    };
    std::shared_ptr<BufferManager> bufferManager;
    if (workerConfiguration.queryEngine.cpuPinning)
    {
        /// Linux places a page on the NUMA node of the thread that touches it first, and creating the pool touches a page of every
        /// buffer. So a thread on a performance CPU creates it, or the pool ends up wherever the thread that starts the worker runs.
        CpuPinning::enable(workerConfiguration.queryEngine.performanceCpus, workerConfiguration.queryEngine.miscCpus);
        std::exception_ptr failure;
        std::thread(
            [&]
            {
                CpuPinning::pinThisThread(CpuPinning::Role::Performance);
                try
                {
                    bufferManager = createBufferManager();
                }
                catch (...)
                {
                    failure = std::current_exception();
                }
            })
            .join();
        if (failure)
        {
            std::rethrow_exception(failure);
        }
    }
    else
    {
        bufferManager = createBufferManager();
    }
    auto queryLog = std::make_shared<QueryLog>();

    auto queryEngine = std::make_unique<QueryEngine>(workerConfiguration.queryEngine, statisticsListener, queryLog, bufferManager, host);

    auto sourceProvider = std::make_unique<SourceProvider>(workerConfiguration.defaultMaxInflightBuffers, bufferManager);

    return std::make_unique<NodeEngine>(
        std::move(bufferManager), statisticsListener, std::move(queryLog), std::move(queryEngine), std::move(sourceProvider));
}

}
