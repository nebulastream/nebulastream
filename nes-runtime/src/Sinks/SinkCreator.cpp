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
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/RawBufferSink.hpp>
#include <Sinks/SinkCreator.hpp>

namespace NES
{
DataSinkPtr createCSVFileSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    const std::string& filePath,
    bool append,
    bool addTimestamp,
    uint64_t numberOfOrigins)
{
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager(), addTimestamp);
    return std::make_shared<FileSink>(
        format, nodeEngine, activeProducers, filePath, append, sharedQueryId, decomposedQueryPlanId, numberOfOrigins);
}

DataSinkPtr createBinaryNESFileSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    const std::string& filePath,
    bool append,
    uint64_t numberOfOrigins)
{
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(
        format, nodeEngine, activeProducers, filePath, append, sharedQueryId, decomposedQueryPlanId, numberOfOrigins);
}

DataSinkPtr createJSONFileSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    const std::string& filePath,
    bool append,
    uint64_t numberOfOrigins)
{
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(
        format, nodeEngine, activeProducers, filePath, append, sharedQueryId, decomposedQueryPlanId, numberOfOrigins);
}

DataSinkPtr createMigrateFileSink(
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    Runtime::NodeEnginePtr nodeEngine,
    uint32_t numOfProducers,
    const std::string& filePath,
    bool append,
    uint64_t numberOfOrigins)
{
    return std::make_shared<RawBufferSink>(
        nodeEngine, numOfProducers, filePath, append, sharedQueryId, decomposedQueryPlanId, numberOfOrigins);
}

DataSinkPtr createCsvPrintSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    std::ostream& out,
    uint64_t numberOfOrigins)
{
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, nodeEngine, activeProducers, sharedQueryId, decomposedQueryPlanId, out, numberOfOrigins);
}

DataSinkPtr createCSVPrintSink(
    const SchemaPtr& schema,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    const Runtime::NodeEnginePtr& nodeEngine,
    uint32_t activeProducers,
    std::ostream& out,
    uint64_t numberOfOrigins)
{
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, nodeEngine, activeProducers, sharedQueryId, decomposedQueryPlanId, out, numberOfOrigins);
}

DataSinkPtr createNetworkSink(
    const SchemaPtr& schema,
    OperatorId uniqueNetworkSinkDescriptorId,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    Network::NodeLocation const& nodeLocation,
    Network::NesPartition nesPartition,
    Runtime::NodeEnginePtr const& nodeEngine,
    size_t numOfProducers,
    std::chrono::milliseconds waitTime,
    DecomposedQueryPlanVersion version,
    uint64_t numberOfOrigins,
    uint8_t retryTimes)
{
    return std::make_shared<Network::NetworkSink>(
        schema,
        uniqueNetworkSinkDescriptorId,
        sharedQueryId,
        decomposedQueryPlanId,
        nodeLocation,
        nesPartition,
        nodeEngine,
        numOfProducers,
        waitTime,
        retryTimes,
        numberOfOrigins,
        version);
}
} // namespace NES
