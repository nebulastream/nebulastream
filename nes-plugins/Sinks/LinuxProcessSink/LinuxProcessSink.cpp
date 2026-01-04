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
    NES_DEBUG("Setting up LinuxProcessSink");
    NES_DEBUG("LinuxProcessSink: starting consumer process with command='{}'", commandToRun);

    // Open a pipe to the process' stdin.
    // "w" means: we write to the pipe -> becomes stdin of the spawned process.
    pipe = popen(commandToRun.c_str(), "w");
    if (!pipe)
    {
        throw InvalidConfigParameter("Could not start process '{}' ", commandToRun);
    }

    // Optional: unbuffered to deliver output immediately.
    // setvbuf(pipe, nullptr, _IONBF, 0);
}

void LinuxProcessSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in LinuxProcessSink.");
    PRECONDITION(pipe != nullptr, "LinuxProcessSink was not opened (start() not called or popen failed).");
    PRECONDITION(formatter != nullptr, "LinuxProcessSink formatter was not initialized.");

    // Format the tuple buffer into the configured output format (CSV/JSON/...)
    const auto out = formatter->getFormattedBuffer(inputTupleBuffer);

    if (!out.empty())
    {
        const size_t written = std::fwrite(out.c_str(), 1, out.size(), pipe);
        if (written != out.size())
        {
            throw CannotOpenSink(fmt::format("LinuxProcessSink: Failed to write to process (wrote {} of {} bytes).",
                                              written,
                                              out.size()));
        }

        // For interactive consumers, flushing per buffer is often desired.
        std::fflush(pipe);
    }
}

void LinuxProcessSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Stopping LinuxProcessSink");

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
            NES_TRACE("LinuxProcessSink: process closed (exit status raw={}).", rc);
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