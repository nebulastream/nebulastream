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
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace NES
{

/// A sink that writes Arrow RecordBatches to a file in Arrow IPC streaming format.
/// Expects input buffers written in Arrow columnar layout (via ArrowBufferRef).
///
/// The output file can be read by any Arrow-compatible tool (PyArrow, DuckDB, Polars, Spark, etc.).
class ArrowFileSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "ArrowFile";
    explicit ArrowFileSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "ArrowFileSink(" << filePath << ")"; }

private:
    std::shared_ptr<arrow::RecordBatch> wrapBufferAsRecordBatch(const TupleBuffer& buffer) const;

    std::string filePath;
    std::shared_ptr<const Schema> nesSchema;

    std::shared_ptr<arrow::Schema> arrowSchema;
    std::shared_ptr<arrow::io::FileOutputStream> file;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    std::mutex writerMutex;
};

struct ConfigParametersArrowFile
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format",
        "Arrow",
        [](const std::unordered_map<std::string, std::string>&) { return std::optional<std::string>("Arrow"); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH, OUTPUT_FORMAT);
};

}

FMT_OSTREAM(NES::ArrowFileSink);
