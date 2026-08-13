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

#include <filesystem>
#include <fstream>
#include <ostream>
#include <string>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <Checksum.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Sink-defined config struct: instantiated from the generic config by the SinkConfig registry
/// entry, carried through the SinkDescriptor as std::any, and serialized via reflection of
/// exactly this struct (all members are reflectable).
struct ChecksumSinkConfig
{
    std::filesystem::path filePath;

    static std::expected<ChecksumSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// A sink that counts the number of tuples and accumulates a checksum, which is written to file once the query is stopped.
/// Example output of the sink:
/// S$Count:UINT64:NOT_NULLABLE,S$Checksum:UINT64:NOT_NULLABLE
/// 1042, 12390478290
class ChecksumSink : public Sink
{
public:
    static constexpr std::string_view NAME = "Checksum";
    explicit ChecksumSink(BackpressureController backpressureController, const ChecksumSinkConfig& config);

    /// Opens file and writes schema to file, if the file is empty.
    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&) override;
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "ChecksumSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    Checksum checksum;
};

}

FMT_OSTREAM(NES::ChecksumSink);
