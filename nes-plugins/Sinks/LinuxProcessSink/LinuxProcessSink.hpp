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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/Format.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class LinuxProcessSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "LinuxProcess";
    explicit LinuxProcessSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~LinuxProcessSink() override = default;

    LinuxProcessSink(const LinuxProcessSink&) = delete;
    LinuxProcessSink& operator=(const LinuxProcessSink&) = delete;
    LinuxProcessSink(LinuxProcessSink&&) = delete;
    LinuxProcessSink& operator=(LinuxProcessSink&&) = delete;

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "LinuxProcessSink"; }

private:
    std::string commandToRun;
    FILE* pipe = nullptr;
    std::unique_ptr<Format> formatter;
};

struct ConfigParametersLinuxProcessSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> COMMAND{
        "command",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) {
            return DescriptorConfig::tryGet(COMMAND, config);
        }
    };

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap,
            COMMAND
        );
};
}