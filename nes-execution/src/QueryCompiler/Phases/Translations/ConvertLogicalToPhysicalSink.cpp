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

#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(
    OperatorId operatorId,
    const SinkDescriptorPtr& sinkDescriptor,
    const SchemaPtr& schema,
    const Runtime::NodeEnginePtr& nodeEngine,
    const QueryCompilation::PipelineQueryPlanPtr& pipelineQueryPlan,
    size_t numOfProducers)
{
    NES_DEBUG("Convert sink  {}", operatorId);
    NES_ASSERT(nodeEngine, "Invalid node engine");
    NES_ASSERT(pipelineQueryPlan, "Invalid query sub-plan");
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>())
    {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating print sink {}", schema->toString());
        const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
        return createCsvPrintSink(
            schema,
            pipelineQueryPlan->getQueryId(),
            pipelineQueryPlan->getQuerySubPlanId(),
            nodeEngine,
            numOfProducers,
            std::cout,
            printSinkDescriptor->getNumberOfOrigins());
    }
    if (sinkDescriptor->instanceOf<FileSinkDescriptor>())
    {
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        NES_INFO("ConvertLogicalToPhysicalSink: Creating file sink for format={}", fileSinkDescriptor->getSinkFormatAsString());
        if (fileSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT")
        {
            return createCSVFileSink(
                schema,
                pipelineQueryPlan->getQueryId(),
                pipelineQueryPlan->getQuerySubPlanId(),
                nodeEngine,
                numOfProducers,
                fileSinkDescriptor->getFileName(),
                fileSinkDescriptor->getAppend(),
                fileSinkDescriptor->getAddTimestamp(),
                fileSinkDescriptor->getNumberOfOrigins());
        }
        else if (fileSinkDescriptor->getSinkFormatAsString() == "NES_FORMAT")
        {
            return createBinaryNESFileSink(
                schema,
                pipelineQueryPlan->getQueryId(),
                pipelineQueryPlan->getQuerySubPlanId(),
                nodeEngine,
                numOfProducers,
                fileSinkDescriptor->getFileName(),
                fileSinkDescriptor->getAppend(),
                fileSinkDescriptor->getNumberOfOrigins());
        }
        else
        {
            NES_ERROR("createDataSink: unsupported format");
            throw std::invalid_argument("Unknown File format");
        }
    }
    else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>())
    {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating network sink");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        return createNetworkSink(
            schema,
            networkSinkDescriptor->getUniqueId(),
            pipelineQueryPlan->getQueryId(),
            pipelineQueryPlan->getQuerySubPlanId(),
            networkSinkDescriptor->getNodeLocation(),
            networkSinkDescriptor->getNesPartition(),
            nodeEngine,
            numOfProducers,
            networkSinkDescriptor->getWaitTime(),
            networkSinkDescriptor->getVersion(),
            networkSinkDescriptor->getNumberOfOrigins(),
            networkSinkDescriptor->getRetryTimes());
    }
    NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
    throw std::invalid_argument("Unknown Sink Descriptor Type");
}
} // namespace NES
