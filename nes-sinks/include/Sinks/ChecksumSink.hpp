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

#include <memory>
#include <string>
#include <unordered_map>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>

namespace NES::Sinks
{

/// A sink that writes formatted TupleBuffers to arbitrary files.
class ChecksumSink : public Sink
{
public:
    static constexpr std::string_view NAME = "Checksum";
    explicit ChecksumSink(const SinkDescriptor& sinkDescriptor);

    ChecksumSink(const ChecksumSink&) = delete;
    ChecksumSink& operator=(const ChecksumSink&) = delete;
    ChecksumSink(ChecksumSink&&) = delete;
    ChecksumSink& operator=(ChecksumSink&&) = delete;

    /// Opens file and writes schema to file, if the file is empty.
    void start(Runtime::Execution::PipelineExecutionContext&) override;
    void stop(Runtime::Execution::PipelineExecutionContext&) override;
    void execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&) override;
    static std::unique_ptr<Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "ChecksumSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    std::atomic_size_t numberOfTuples;
    std::atomic_size_t checksum;
    std::atomic_size_t formattedChecksum;
    size_t schemaSizeInBytes;
    std::unique_ptr<CSVFormat> formatter;
};

struct ConfigParametersChecksum
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(FILEPATH, config);
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(FILEPATH);
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::ChecksumSink> : ostream_formatter
{
};
}
