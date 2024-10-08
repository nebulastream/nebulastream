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

#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>


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
    PRECONDITION(nodeEngine, "Invalid node engine");
    PRECONDITION(pipelineQueryPlan, "Invalid query sub-plan");

    NES_DEBUG("Convert sink  {}", operatorId);
    if (NES::Util::instanceOf<PrintSinkDescriptor>(sinkDescriptor))
    {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating print sink {}", schema->toString());
        const PrintSinkDescriptorPtr printSinkDescriptor = NES::Util::as<PrintSinkDescriptor>(sinkDescriptor);
        return createCsvPrintSink(
            schema, pipelineQueryPlan->getQueryId(), nodeEngine, numOfProducers, std::cout, printSinkDescriptor->getNumberOfOrigins());
    }
    if (NES::Util::instanceOf<FileSinkDescriptor>(sinkDescriptor))
    {
        auto fileSinkDescriptor = NES::Util::as<FileSinkDescriptor>(sinkDescriptor);
        NES_INFO("ConvertLogicalToPhysicalSink: Creating file sink for format={}", fileSinkDescriptor->getSinkFormatAsString());
        if (fileSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT")
        {
            return createCSVFileSink(
                schema,
                pipelineQueryPlan->getQueryId(),
                nodeEngine,
                numOfProducers,
                fileSinkDescriptor->getFileName(),
                fileSinkDescriptor->getAppend(),
                fileSinkDescriptor->getAddTimestamp(),
                fileSinkDescriptor->getNumberOfOrigins());
        }
        else
        {
            throw UnknownSinkType();
        }
    }
    else
    {
        throw UnknownSinkType();
    }
}
} /// namespace NES
