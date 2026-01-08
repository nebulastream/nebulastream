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
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/Format.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Setsum.hpp>
#include <PipelineExecutionContext.hpp>

#include "SinkRegistry.hpp"
#include "SinkValidationRegistry.hpp"

namespace NES
{

/// A sink that counts the number of tuples and accumulates into 8 checksum columns
/// Example output of the sink:
/// S$Count:UINT64,S$Col0:UINT32,S$Col1:UINT32,S$Col2:UINT32,S$Col3:UINT32,S$Col4:UINT32,S$Col5:UINT32,S$Col6:UINT32,S$Col7:UINT32
/// 1042,12345678,23456789,34567890,45678901,56789012,67890123,78901234,89012345
class SetsumSink : public Sink
{
public:
    static constexpr std::string_view NAME = "Setsum";
    explicit SetsumSink(const SinkDescriptor& sinkDescriptor);

    /// Opens file and writes schema to file, if the file is empty.
    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    static SinkValidationRegistryReturnType RegisterSetsumSinkValidation(SinkValidationRegistryArguments sinkConfig);
    static SinkRegistryReturnType RegisterSetsumSink(SinkRegistryArguments sinkRegistryArguments);
    const Setsum& getCurrentSetsum() const { return setsum; }

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "SetsumSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    Setsum setsum;
    std::atomic<uint64_t> tupleCount{0};
    std::unique_ptr<Format> formatter;
};

struct ConfigParametersSetsum
{
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::FILE_PATH);
};

}

FMT_OSTREAM(NES::SetsumSink);
