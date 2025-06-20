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

#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Checksum.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

/// A sink that counts the number of tuples and accumulates a checksum, which is written to file once the query is stopped.
/// Example output of the sink:
/// S$Count:UINT64,S$Checksum:UINT64
/// 1042, 12390478290
class ChecksumSink : public Sink
{
public:
    static constexpr std::string_view NAME = "Checksum";
    explicit ChecksumSink(const SinkDescriptor& sinkDescriptor);

    /// Opens file and writes schema to file, if the file is empty.
    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&) override;
    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    static inline const Schema checksumSchema = []
    {
        auto checksumSinkSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
        checksumSinkSchema.addField("S$Count", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        checksumSinkSchema.addField("S$Checksum", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return checksumSinkSchema;
    }();

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "ChecksumSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    Checksum checksum;
    std::unique_ptr<CSVFormat> formatter;
};

struct ConfigParametersChecksum
{
    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::FILE_PATH);
};

}

FMT_OSTREAM(NES::Sinks::ChecksumSink);
