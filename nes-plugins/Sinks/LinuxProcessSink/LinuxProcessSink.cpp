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

#include <LinuxProcessSink.hpp>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

LinuxProcessSink::LinuxProcessSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , commandToRun(sinkDescriptor.getFromConfig(ConfigParametersLinuxProcessSink::COMMAND))
{
    // Create the formatter the same way FileSink does: based on the sink's INPUT_FORMAT.
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(SinkDescriptor::INPUT_FORMAT))
    {
        case InputFormat::CSV:
            formatter = std::make_unique<CSVFormat>(*sinkDescriptor.getSchema());
            break;
        case InputFormat::JSON:
            formatter = std::make_unique<JSONFormat>(*sinkDescriptor.getSchema());
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}

void LinuxProcessSink::start(PipelineExecutionContext&)
{
    NES_INFO("Ppening a process with: {}", commandToRun);

    pipe = popen(commandToRun.c_str(), "w");
    if (!pipe)
    {
        throw CannotOpenSink("Could not start process: {} ", commandToRun);
    }
    NES_INFO("Pipe connected to write in stdin of process: {}", commandToRun);
}

void LinuxProcessSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in LinuxProcessSink.");
    PRECONDITION(pipe != nullptr, "LinuxProcessSink was not opened (start() not called or popen failed).");
    PRECONDITION(formatter != nullptr, "LinuxProcessSink formatter was not initialized.");

    NES_INFO("Starting to fill stdin of process: {}", commandToRun);

    const auto out = formatter->getFormattedBuffer(inputTupleBuffer);

    if (!out.empty())
    {
        const size_t written = std::fwrite(out.c_str(), 1, out.size(), pipe);
        NES_DEBUG("Written {}", written);
        if (written != out.size())
        {
            NES_ERROR("Could not write to output file: from {} written: {}", out.size(), written);
        }

        // For interactive consumers, flushing per buffer.
        std::fflush(pipe);
    }
    NES_INFO("LinuxProcessSink finished writing");
}

void LinuxProcessSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Stopping LinuxProcessSink");

    if (pipe)
    {
        std::fflush(pipe);
        const int rc = pclose(pipe);
        pipe = nullptr;

        if (rc == -1)
        {
            NES_ERROR("LinuxProcessSink: pclose failed: {}", std::strerror(errno));
        }
        else
        {
            NES_TRACE("LinuxProcessSink closed");
        }
    }
}

DescriptorConfig::Config LinuxProcessSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    // Uses your ConfigParametersLinuxProcess::parameterMap (extends SinkDescriptor::parameterMap).
    return DescriptorConfig::validateAndFormat<ConfigParametersLinuxProcessSink>(std::move(config), NAME);
}

// ---- Registry hooks (like VoidSink / FileSink) ----

SinkValidationRegistryReturnType RegisterLinuxProcessSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return LinuxProcessSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterLinuxProcessSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<LinuxProcessSink>(std::move(sinkRegistryArguments.backpressureController),
                                              sinkRegistryArguments.sinkDescriptor);
}
}