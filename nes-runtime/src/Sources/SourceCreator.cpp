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

#include <chrono>
#include <Runtime/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Sources/TCPSource.hpp>

namespace NES
{

DataSourcePtr createCSVFileSource(
    const SchemaPtr& schema,
    const std::shared_ptr<Runtime::AbstractPoolProvider>& bufferManager,
    const Runtime::QueryManagerPtr& queryManager,
    const CSVSourceTypePtr& csvSourceType,
    OperatorId operatorId,
    OriginId originId,
    size_t numSourceLocalBuffers,
    const std::string& physicalSourceName,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
{
    return std::make_shared<CSVSource>(
        schema,
        bufferManager,
        queryManager,
        csvSourceType,
        operatorId,
        originId,
        numSourceLocalBuffers,
        GatheringMode::INTERVAL_MODE,
        physicalSourceName,
        successors);
}

DataSourcePtr createTCPSource(
    const SchemaPtr& schema,
    const std::shared_ptr<Runtime::AbstractPoolProvider>& bufferManager,
    const Runtime::QueryManagerPtr& queryManager,
    const TCPSourceTypePtr& tcpSourceType,
    OperatorId operatorId,
    OriginId originId,
    size_t numSourceLocalBuffers,
    const std::string& physicalSourceName,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
{
    return std::make_shared<TCPSource>(
        schema,
        bufferManager,
        queryManager,
        tcpSourceType,
        operatorId,
        originId,
        numSourceLocalBuffers,
        GatheringMode::INTERVAL_MODE,
        physicalSourceName,
        successors);
}
} /// namespace NES