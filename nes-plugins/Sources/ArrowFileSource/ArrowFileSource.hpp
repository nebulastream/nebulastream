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
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace NES
{

class ArrowBufferRef;

/// A source that reads Arrow IPC streaming format files (.arrows) and produces
/// TupleBuffers in ArrowBufferRef layout (columnar Arrow-compatible memory layout).
/// This avoids columnar→row→columnar conversion when used with ArrowBufferRef scans.
class ArrowFileSource final : public Source
{
public:
    static constexpr std::string_view NAME = "ArrowFile";

    explicit ArrowFileSource(const SourceDescriptor& sourceDescriptor);
    ~ArrowFileSource() override = default;

    ArrowFileSource(const ArrowFileSource&) = delete;
    ArrowFileSource& operator=(const ArrowFileSource&) = delete;
    ArrowFileSource(ArrowFileSource&&) = delete;
    ArrowFileSource& operator=(ArrowFileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

private:
    std::string filePath;
    std::shared_ptr<const Schema> nesSchema;

    /// Arrow reader state
    std::shared_ptr<arrow::io::ReadableFile> file;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader;
    std::shared_ptr<arrow::RecordBatch> currentBatch;
    int64_t currentRowInBatch = 0;
    bool exhausted = false;

    /// Buffer layout — obtained from LowerSchemaProvider in open(), shared with the scan operator
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::shared_ptr<const ArrowBufferRef> arrowBufRef;
};

struct ConfigParametersArrowFileSource
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILE_PATH);
};

}

FMT_OSTREAM(NES::ArrowFileSource);
