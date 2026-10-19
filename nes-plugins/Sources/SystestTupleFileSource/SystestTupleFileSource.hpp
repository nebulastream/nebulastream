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
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES
{

/// Reads a newline-delimited file one tuple per buffer. If the environment variable named by CrashEveryNTuplesVariable is set, the
/// process exits abruptly after the source read that many tuples, which lets the systest restart the worker between any two tuples.
class SystestTupleFileSource final : public Source
{
public:
    static constexpr std::string_view NAME = "SystestTupleFile";
    static constexpr std::string_view CrashEveryNTuplesVariable = "NES_SYSTEST_CRASH_EVERY_N_TUPLES";
    static constexpr int CrashExitCode = 86;

    explicit SystestTupleFileSource(const SourceDescriptor& sourceDescriptor);
    ~SystestTupleFileSource() override = default;

    SystestTupleFileSource(const SystestTupleFileSource&) = delete;
    SystestTupleFileSource& operator=(const SystestTupleFileSource&) = delete;
    SystestTupleFileSource(SystestTupleFileSource&&) = delete;
    SystestTupleFileSource& operator=(SystestTupleFileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;
    [[nodiscard]] bool resumeAtByteOffset(uint64_t byteOffset) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    static InlineDataRegistryReturnType provideInlineData(InlineDataRegistryArguments systestAdaptorArguments);
    static FileDataRegistryReturnType provideFileData(FileDataRegistryArguments systestAdaptorArguments);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    std::ifstream inputFile;
    uint64_t resumeByteOffset = 0;
    uint64_t numberOfReadTuples = 0;
    std::optional<uint64_t> crashEveryNTuples;
};

struct ConfigParametersSystestTupleFile
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "FILE_PATH",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
