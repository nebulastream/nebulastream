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

#include <GrpcSink.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/Statistic/StatisticFieldNames.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <Util/Variant.hpp>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <StatisticService.grpc.pb.h>
#include <StatisticService.pb.h>

namespace NES
{

GrpcSink::GrpcSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , grpcHost(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::HOST))
    , grpcPort(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::PORT))
    , probeId(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::PROBE_ID))
{
    /// Same schema FileSink formats its header from, so this is the order the CSV rows arrive in.
    const auto schema = NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema());
    for (const auto& name : getOrderedFieldNames(*schema))
    {
        /// QualifiedIdentifierBase<1> converts to the single Identifier it wraps.
        const auto lastName = static_cast<const Identifier&>(name).asCanonicalString();
        if (lastName == StatisticFieldNames::STATISTIC_ID)
        {
            statisticIdColumn = columnCount;
        }
        else if (lastName == StatisticFieldNames::START_TS)
        {
            startTsColumn = columnCount;
        }
        else if (lastName == StatisticFieldNames::END_TS)
        {
            endTsColumn = columnCount;
        }
        else if (lastName == StatisticFieldNames::VALUE)
        {
            valueColumn = columnCount;
        }
        ++columnCount;
    }
    if (statisticIdColumn == MissingColumn or startTsColumn == MissingColumn or endTsColumn == MissingColumn
        or valueColumn == MissingColumn)
    {
        throw CannotOpenSink(
            "GrpcSink: the sink schema must carry {}, {}, {} and {}",
            StatisticFieldNames::STATISTIC_ID,
            StatisticFieldNames::START_TS,
            StatisticFieldNames::END_TS,
            StatisticFieldNames::VALUE);
    }
}

void GrpcSink::start(PipelineExecutionContext&)
{
    const auto address = grpcHost + ":" + std::to_string(grpcPort);
    NES_DEBUG("GrpcSink::start: connecting to {}", address);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = StatisticReportService::NewStub(channel);
    if (not stub)
    {
        throw CannotOpenSink("GrpcSink: failed to create a gRPC stub for {}", address);
    }
    NES_INFO("GrpcSink: connected to {}", address);
}

bool GrpcSink::appendRow(const std::string_view line, StatisticReportBatch& batch) const
{
    if (line.empty())
    {
        return false;
    }

    const auto values = splitWithStringDelimiter<std::string_view>(line, ",");
    if (values.size() != columnCount)
    {
        NES_WARNING("GrpcSink: row has {} columns but the schema declared {}, dropping it", values.size(), columnCount);
        return false;
    }

    const auto statisticId = from_chars<uint64_t>(values[statisticIdColumn]);
    const auto startTs = from_chars<uint64_t>(values[startTsColumn]);
    const auto endTs = from_chars<uint64_t>(values[endTsColumn]);
    const auto value = from_chars<double>(values[valueColumn]);
    if (not statisticId or not startTs or not endTs or not value)
    {
        NES_WARNING("GrpcSink: dropping an unparsable row");
        return false;
    }

    auto* const report = batch.add_reports();
    report->set_statistic_id(*statisticId);
    report->set_start_ts(*startTs);
    report->set_end_ts(*endTs);
    report->set_value(*value);
    report->set_probe_id(probeId);
    return true;
}

void GrpcSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in GrpcSink.");
    PRECONDITION(stub != nullptr, "GrpcSink stub not initialized. Was start() called?");

    StatisticReportBatch batch;
    BufferIterator iterator{inputTupleBuffer};
    for (auto element = iterator.getNextElement(); element.has_value(); element = iterator.getNextElement())
    {
        const std::string_view content{element->buffer.getAvailableMemoryArea<const char>().data(), element->contentLength};
        for (const auto line : splitOnMultipleDelimiters(content, {'\n'}))
        {
            appendRow(line, batch);
        }
    }

    if (batch.reports().empty())
    {
        return;
    }

    grpc::ClientContext context;
    google::protobuf::Empty response;
    if (const auto status = stub->ReportStatistics(&context, batch, &response); not status.ok())
    {
        NES_WARNING("GrpcSink: ReportStatistics failed: {} (code {})", status.error_message(), static_cast<int>(status.error_code()));
    }
}

void GrpcSink::stop(PipelineExecutionContext&)
{
    NES_INFO("GrpcSink: stopped.");
    stub.reset();
}

DescriptorConfig::Config GrpcSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersGrpcSink>(std::move(config), std::string{NAME});
}

}
