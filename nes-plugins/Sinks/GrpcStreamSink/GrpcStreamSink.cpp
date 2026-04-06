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

#include <GrpcStreamSink.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <SystemStatsBroadcaster.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{

GrpcStreamSink::GrpcStreamSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , statType(sinkDescriptor.getFromConfig(ConfigParametersGrpcStream::STAT_TYPE))
{
}

std::ostream& GrpcStreamSink::toString(std::ostream& os) const
{
    os << "GrpcStreamSink(statType: " << statType << ")";
    return os;
}

void GrpcStreamSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up gRPC stream sink: {}", *this);
}

void GrpcStreamSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Stopping gRPC stream sink: {}", *this);
}

namespace
{
/// Parse a CSV line into a SystemStatEvent based on the stat type.
/// Returns true on success.
bool parseLineToEvent(const std::string& statType, const std::string& line, SystemStatEvent& event)
{
    std::istringstream iss(line);

    auto getField = [&](std::string& out) -> bool { return static_cast<bool>(std::getline(iss, out, ',')); };

    if (statType == "pipeline_compiled")
    {
        // CSV: timestamp_ms,query_id,pipeline_id,compile_time_ns
        std::string tsStr, queryId, pipelineIdStr, compileTimeStr;
        if (!getField(tsStr) || !getField(queryId) || !getField(pipelineIdStr) || !getField(compileTimeStr))
        {
            return false;
        }
        auto* stat = event.mutable_pipeline_compiled();
        stat->set_timestamp_ms(std::stoull(tsStr));
        stat->set_query_id(queryId);
        stat->set_pipeline_id(std::stoull(pipelineIdStr));
        stat->set_compile_time_ns(std::stoull(compileTimeStr));
        return true;
    }

    if (statType == "query_status")
    {
        // CSV: timestamp_ms,query_id,status
        std::string tsStr, queryId, status;
        if (!getField(tsStr) || !getField(queryId) || !getField(status))
        {
            return false;
        }
        auto* stat = event.mutable_query_status();
        stat->set_timestamp_ms(std::stoull(tsStr));
        stat->set_query_id(queryId);
        stat->set_status(status);
        return true;
    }

    if (statType == "unpooled_buffer_alloc")
    {
        // CSV: start,end,alloc_count,avg_alloc_size
        std::string startStr, endStr, countStr, avgStr;
        if (!getField(startStr) || !getField(endStr) || !getField(countStr) || !getField(avgStr))
        {
            return false;
        }
        auto* stat = event.mutable_unpooled_buffer_alloc();
        stat->set_window_start_ms(std::stoull(startStr));
        stat->set_window_end_ms(std::stoull(endStr));
        stat->set_alloc_count(std::stoull(countStr));
        stat->set_avg_alloc_size(std::stod(avgStr));
        return true;
    }

    if (statType == "buffer_ingestion")
    {
        // CSV: query_id,start,end,total_tuples,ingestion_count
        std::string queryId, startStr, endStr, totalStr, countStr;
        if (!getField(queryId) || !getField(startStr) || !getField(endStr) || !getField(totalStr) || !getField(countStr))
        {
            return false;
        }
        auto* stat = event.mutable_buffer_ingestion();
        stat->set_query_id(queryId);
        stat->set_window_start_ms(std::stoull(startStr));
        stat->set_window_end_ms(std::stoull(endStr));
        stat->set_total_tuples(std::stoull(totalStr));
        stat->set_ingestion_count(std::stoull(countStr));
        return true;
    }

    return false;
}
}

void GrpcStreamSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in GrpcStreamSink.");

    auto* broadcaster = SystemStatsBroadcaster::tryInstance();
    if (!broadcaster)
    {
        return; // No broadcaster — silently drop
    }

    BufferIterator iterator{inputTupleBuffer};
    std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();

    // Accumulate raw bytes, then parse lines
    std::string buffer;
    while (element.has_value())
    {
        buffer.append(element.value().buffer.getAvailableMemoryArea<char>().data(),
                      element.value().contentLength);
        element = iterator.getNextElement();
    }

    // Process complete lines (skip the schema header if present)
    bool headerSkipped = false;
    size_t pos;
    size_t start = 0;
    while ((pos = buffer.find('\n', start)) != std::string::npos)
    {
        auto line = buffer.substr(start, pos - start);
        start = pos + 1;

        if (!line.empty() && line.back() == '\r')
        {
            line.pop_back();
        }
        if (line.empty())
        {
            continue;
        }

        // The first line in the first buffer is the CSV schema header — skip it
        if (!headerSkipped && line.find(':') != std::string::npos)
        {
            headerSkipped = true;
            continue;
        }
        headerSkipped = true;

        SystemStatEvent event;
        if (parseLineToEvent(statType, line, event))
        {
            broadcaster->broadcast(event);
        }
    }
}

DescriptorConfig::Config GrpcStreamSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGrpcStream>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterGrpcStreamSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return GrpcStreamSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterGrpcStreamSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<GrpcStreamSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
