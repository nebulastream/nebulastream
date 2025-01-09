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
#include <ostream>
#include <string>
#include <unordered_map>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <folly/Synchronized.h>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

/// A sink that writes formatted TupleBuffers to arbitrary files.
class TCPSink : public Sink
{
public:
    static inline std::string NAME = "TCP";
    explicit TCPSink(const SinkDescriptor& sinkDescriptor);
    ~TCPSink() override = default;

    TCPSink(const TCPSink&) = delete;
    TCPSink& operator=(const TCPSink&) = delete;
    TCPSink(TCPSink&&) = delete;
    TCPSink& operator=(TCPSink&&) = delete;

    void start(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

    static std::unique_ptr<Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string host;
    uint16_t port;

    std::optional<int> socket;
};

struct ConfigParametersTCP
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> HOST{
        "host", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(HOST, config);
        }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "port",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            /// Mandatory (no default value)
            const auto portNumber = Configurations::DescriptorConfig::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("TCPSource specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(HOST, PORT);
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::TCPSink> : ostream_formatter
{
};
}
