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

#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSource.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
#    if defined(__linux__)
#        include <numa.h>
#    endif
#endif
namespace NES
{

DataSourcePtr ConvertLogicalToPhysicalSource::createDataSource(
    OperatorId operatorId,
    OriginId originId,
    const SourceDescriptorPtr& sourceDescriptor,
    const Runtime::NodeEnginePtr& nodeEngine,
    size_t numSourceLocalBuffers,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
{
    NES_ASSERT(nodeEngine, "invalid engine");
    auto numaNodeIndex = 0u;
#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
    if (sourceDescriptor->instanceOf<BenchmarkSourceDescriptor>())
    {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating memory source");
        auto benchmarkSourceDescriptor = sourceDescriptor->as<BenchmarkSourceDescriptor>();
        auto sourceAffinity = benchmarkSourceDescriptor->getSourceAffinity();
        if (sourceAffinity != std::numeric_limits<uint64_t>::max())
        {
            auto nodeOfCpu = numa_node_of_cpu(sourceAffinity);
            numaNodeIndex = nodeOfCpu;
            NES_ASSERT2_FMT(
                0 <= numaNodeIndex && numaNodeIndex < nodeEngine->getHardwareManager()->getNumberOfNumaRegions(), "invalid numa settings");
        }
    }
#endif
    auto bufferManager = nodeEngine->getBufferManager();
    auto queryManager = nodeEngine->getQueryManager();
    if (sourceDescriptor->instanceOf<CsvSourceDescriptor>())
    {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
        const CsvSourceDescriptorPtr csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        return createCSVFileSource(
            csvSourceDescriptor->getSchema(),
            bufferManager,
            queryManager,
            csvSourceDescriptor->getSourceConfig(),
            operatorId,
            originId,
            numSourceLocalBuffers,
            sourceDescriptor->getPhysicalSourceName(),
            successors);
    }
    else if (sourceDescriptor->instanceOf<TCPSourceDescriptor>())
    {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating TCP source");
        auto tcpSourceDescriptor = sourceDescriptor->as<TCPSourceDescriptor>();
        return createTCPSource(
            tcpSourceDescriptor->getSchema(),
            bufferManager,
            queryManager,
            tcpSourceDescriptor->getSourceConfig(),
            operatorId,
            originId,

            numSourceLocalBuffers,
            sourceDescriptor->getPhysicalSourceName(),
            successors);
    }
    else
    {
        NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type {}", sourceDescriptor->getSchema()->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}

} /// namespace NES
