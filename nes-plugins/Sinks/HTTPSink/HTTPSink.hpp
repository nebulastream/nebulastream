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
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <curl/curl.h>

#include <folly/Synchronized.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/Format.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

/// Sink that sends off newline delimited json tuples to an HTTP endpoint at a certain port of an ip-address via POST.
/// Currently, does not wait for any response, just sends and continues.
/// Should probably use HTTPS at some point
class HTTPSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "HTTP";
    explicit HTTPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~HTTPSink() override = default;

    HTTPSink(const HTTPSink&) = delete;
    HTTPSink& operator=(const HTTPSink&) = delete;
    HTTPSink(HTTPSink&&) = delete;
    HTTPSink& operator=(HTTPSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string url;
    CURL* curl;
    bool isOpen = false;
    std::unique_ptr<Format> formatter;
};

struct ConfigParametersMatrix
{
    static inline const DescriptorConfig::ConfigParameter<std::string> IPADDRESS{
        "ip_address",
        "localhost",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(IPADDRESS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> PORT{
        "port", "3000", [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> ENDPOINT{
        "endpoint",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ENDPOINT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, IPADDRESS, PORT, ENDPOINT);
};

}

FMT_OSTREAM(NES::HTTPSink);
