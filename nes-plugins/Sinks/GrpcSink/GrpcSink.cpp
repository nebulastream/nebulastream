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

#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Runtime/TupleBuffer.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Variant.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <Util/Logger/Logger.hpp>
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

namespace
{

std::vector<std::string_view> splitColumns(const std::string_view line)
{
    std::vector<std::string_view> parts;
    size_t start = 0;
    while (start <= line.size())
    {
        const auto comma = line.find(',', start);
        if (comma == std::string_view::npos)
        {
            parts.emplace_back(line.substr(start));
            break;
        }
        parts.emplace_back(line.substr(start, comma - start));
        start = comma + 1;
    }
    return parts;
}

std::optional<uint64_t> parseUnsigned(const std::string_view text)
{
    uint64_t value{};
    const auto* const first = text.data();
    const auto* const last = text.data() + text.size();
    if (const auto [ptr, ec] = std::from_chars(first, last, value); ec == std::errc{} and ptr == last)
    {
        return value;
    }
    return std::nullopt;
}

/// libc++ still leaves the floating-point overload of from_chars deleted, so this goes through strtod. The
/// string_view is not null-terminated, hence the copy.
std::optional<double> parseDouble(const std::string_view text)
{
    const std::string owned{text};
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(owned.c_str(), &end);
    if (end == owned.c_str() or *end != '\0' or errno == ERANGE)
    {
        return std::nullopt;
    }
    return value;
}

}

GrpcSink::GrpcSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , grpcHost(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::HOST))
    , grpcPort(sinkDescriptor.getFromConfig(ConfigParametersGrpcSink::PORT))
{
    /// Same schema FileSink formats its header from, so this is the order the CSV rows arrive in.
    const auto schema = NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema());
    size_t index = 0;
    for (const auto& name : getOrderedFieldNames(*schema))
    {
        /// QualifiedIdentifierBase<1> converts to the single Identifier it wraps.
        const auto lastName = static_cast<const Identifier&>(name).asCanonicalString();
        if (lastName == StatisticFieldNames::STATISTIC_ID)
        {
            statisticIdColumn = index;
        }
        else if (lastName == StatisticFieldNames::START_TS)
        {
            startTsColumn = index;
        }
        else if (lastName == StatisticFieldNames::END_TS)
        {
            endTsColumn = index;
        }
        else if (lastName == StatisticFieldNames::VALUE)
        {
            valueColumn = index;
        }
        ++index;
    }
    columnCount = index;
}

void GrpcSink::start(PipelineExecutionContext&)
{
    const auto address = grpcHost + ":" + std::to_string(grpcPort);
    NES_DEBUG("GrpcSink::start: connecting to {}", address);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = StatisticInterfaceService::NewStub(channel);
    if (not stub)
    {
        throw CannotOpenSink("GrpcSink: failed to create a gRPC stub for {}", address);
    }
    NES_INFO("GrpcSink: connected to {}", address);
}

void GrpcSink::reportRow(const std::string_view line) const
{
    if (line.empty())
    {
        return;
    }

    const auto values = splitColumns(line);
    if (values.size() != columnCount)
    {
        NES_WARNING("GrpcSink: row has {} columns but the schema declared {}, dropping it", values.size(), columnCount);
        return;
    }

    StatisticReport report;
    if (statisticIdColumn.has_value())
    {
        if (const auto parsed = parseUnsigned(values[*statisticIdColumn]))
        {
            report.set_statistic_id(*parsed);
        }
    }
    if (startTsColumn.has_value())
    {
        if (const auto parsed = parseUnsigned(values[*startTsColumn]))
        {
            report.set_start_ts(*parsed);
        }
    }
    if (endTsColumn.has_value())
    {
        if (const auto parsed = parseUnsigned(values[*endTsColumn]))
        {
            report.set_end_ts(*parsed);
        }
    }
    if (valueColumn.has_value())
    {
        /// The branch this is ported from never populated the report's value, so getStatistics could only ever
        /// return zero. Carrying it makes the probe's result observable end to end.
        if (const auto parsed = parseDouble(values[*valueColumn]))
        {
            report.set_value(*parsed);
        }
    }

    grpc::ClientContext context;
    google::protobuf::Empty response;
    if (const auto status = stub->ReportStatistic(&context, report, &response); not status.ok())
    {
        NES_WARNING("GrpcSink: ReportStatistic failed: {} (code {})", status.error_message(), static_cast<int>(status.error_code()));
    }
}

void GrpcSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in GrpcSink.");
    PRECONDITION(stub != nullptr, "GrpcSink stub not initialized. Was start() called?");

    BufferIterator iterator{inputTupleBuffer};
    for (auto element = iterator.getNextElement(); element.has_value(); element = iterator.getNextElement())
    {
        const std::string_view content{element->buffer.getAvailableMemoryArea<const char>().data(), element->contentLength};
        size_t start = 0;
        while (start < content.size())
        {
            const auto newline = content.find('\n', start);
            const auto line = content.substr(start, newline == std::string_view::npos ? std::string_view::npos : newline - start);
            reportRow(line);
            if (newline == std::string_view::npos)
            {
                break;
            }
            start = newline + 1;
        }
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
