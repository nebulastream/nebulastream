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
#include <cstdint>
#include <fstream>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

class SystestTupleFileSource final : public Source
{
public:
    static constexpr std::string_view NAME = "SystestTupleFile";
    static constexpr std::string_view FILE_PATH_PARAMETER = "file_path";

    explicit SystestTupleFileSource(const SourceDescriptor& sourceDescriptor);
    ~SystestTupleFileSource() override = default;

    SystestTupleFileSource(const SystestTupleFileSource&) = delete;
    SystestTupleFileSource& operator=(const SystestTupleFileSource&) = delete;
    SystestTupleFileSource(SystestTupleFileSource&&) = delete;
    SystestTupleFileSource& operator=(SystestTupleFileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;
    void setCheckpointRecoveryByteOffset(uint64_t byteOffset) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    size_t fillBufferWithSingleTuple(TupleBuffer& tupleBuffer);

    std::ifstream inputFile;
    std::string filePath;
    std::string tupleDelimiter;
    std::atomic<size_t> totalNumBytesRead{0};
    uint64_t recoveryByteOffset = 0;
    bool crashBeforeNextRead = false;
};

struct ConfigParametersSystestTupleFile
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SystestTupleFileSource::FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
