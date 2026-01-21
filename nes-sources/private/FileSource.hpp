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

#include <atomic>
#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <zstd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "file_path";
static constexpr std::string_view FILE_COMPRESSION_PARAMETER = "compression";

enum class CompressionType
{
    NONE,
    ZSTD
};

class FileSource final : public Source
{
public:
    static constexpr std::string_view NAME = "File";

    explicit FileSource(const SourceDescriptor& sourceDescriptor);
    ~FileSource() override = default;

    FileSource(const FileSource&) = delete;
    FileSource& operator=(const FileSource&) = delete;
    FileSource(FileSource&&) = delete;
    FileSource& operator=(FileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open file socket.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close file socket.
    void close() override;

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::ifstream inputFile;
    std::string filePath;
    CompressionType compressionType;
    std::atomic<size_t> totalNumBytesRead;

    /// Zstd decompression state
    ZSTD_DCtx* zstdContext = nullptr;
    std::vector<char> compressedBuffer;
    size_t compressedBufferPos = 0;
    size_t compressedBufferSize = 0;
    bool reachedEof = false;

    /// Helper to fill the compressed buffer from file
    void refillCompressedBuffer();
};

struct ConfigParametersCSV
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> COMPRESSION{
        std::string(FILE_COMPRESSION_PARAMETER),
        std::string("none"),
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(COMPRESSION, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH, COMPRESSION);
};

}
