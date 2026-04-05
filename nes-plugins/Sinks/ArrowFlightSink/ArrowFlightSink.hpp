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

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

#include <arrow/api.h>
#include <arrow/flight/api.h>

namespace NES
{

/// Maps a NES DataType to the corresponding Arrow data type.
std::shared_ptr<arrow::DataType> nesTypeToArrowType(DataType::Type type);

/// Builds an arrow::Schema from a NES Schema.
std::shared_ptr<arrow::Schema> nesSchemaToArrowSchema(const Schema& schema);

/// A sink that sends Arrow RecordBatches via Arrow Flight DoPut to an external Flight server.
/// Expects input buffers written in Arrow columnar layout (via ArrowBufferRef).
///
/// The receiving server can be any Flight-compatible endpoint (Python pyarrow.flight, DuckDB, etc.).
class ArrowFlightSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "ArrowFlight";
    explicit ArrowFlightSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "ArrowFlightSink(" << endpoint << ")"; }

private:
    /// Wraps a TupleBuffer's Arrow-layout memory as an arrow::RecordBatch (zero-copy for fixed-width types).
    std::shared_ptr<arrow::RecordBatch> wrapBufferAsRecordBatch(const TupleBuffer& buffer) const;

    std::string endpoint;
    std::string streamName;
    std::shared_ptr<const Schema> nesSchema;

    std::unique_ptr<arrow::flight::FlightClient> client;
    std::shared_ptr<arrow::Schema> arrowSchema;
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> metadataReader;
    std::mutex writerMutex; /// gRPC stream writer is not thread-safe
};

struct ConfigParametersArrowFlight
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> ENDPOINT{
        "endpoint",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ENDPOINT, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> STREAM_NAME{
        "stream_name",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STREAM_NAME, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format",
        "Arrow",
        [](const std::unordered_map<std::string, std::string>&) { return std::optional<std::string>("Arrow"); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(ENDPOINT, STREAM_NAME, OUTPUT_FORMAT);
};

}

FMT_OSTREAM(NES::ArrowFlightSink);
