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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <curl/curl.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Sink that POSTs newline-delimited JSON tuples to the LLM Operator's HTTP API.
///
/// Protocol (three endpoints under the configured ENDPOINT prefix):
///   start()   -> POST {url}/initialize  with the QUERY config blob verbatim
///   execute() -> POST {url}/process     with one JSON object per line
///   stop()    -> POST {url}/stop        with {"query_id": ...}
///
/// Since the output-formatting rework, formatting happens in the *emit* phase
/// (see nes-plugins/OutputFormatters/), so the TupleBuffer arriving in
/// execute() already holds formatted bytes -- this sink only has to ship them.
/// Configure `SINK.OUTPUT_FORMAT = 'JSON'` so those bytes are NDJSON, which is
/// what /process parses (it splits the body on '\n').
///
/// Unlike FileSink this deliberately does NOT emit a schema header line: the
/// operator expects data rows only.
class LLMSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "LLM";
    explicit LLMSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~LLMSink() override = default;

    LLMSink(const LLMSink&) = delete;
    LLMSink& operator=(const LLMSink&) = delete;
    LLMSink(LLMSink&&) = delete;
    LLMSink& operator=(LLMSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    /// POSTs `body` to `{url}/{path}` as application/x-ndjson. Throws on a curl
    /// error or a non-2xx status: the original version dropped every
    /// curl_easy_perform return code, so an operator that was down looked
    /// exactly like a query that was succeeding.
    void post(std::string_view path, const std::string& body) const;

    std::string url;
    std::string query;
    CURL* curl;
    bool isOpen = false;
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all LLM sink config parameters.
/// Keys are UPPERCASE to match the convention of the other sinks on main (cf. ConfigParametersMQTTSink).
struct ConfigParametersLLM
{
    ///NOLINTBEGIN(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> IP_ADDRESS{
        "IP_ADDRESS",
        "localhost",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(IP_ADDRESS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> PORT{
        "PORT", "3000", [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> ENDPOINT{
        "ENDPOINT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ENDPOINT, config); }};

    /// The registration blob forwarded verbatim to /initialize. Must be a JSON
    /// object carrying at least query_id, type, config and endpoint_id.
    static inline const DescriptorConfig::ConfigParameter<std::string> QUERY{
        "QUERY",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(QUERY, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, IP_ADDRESS, PORT, ENDPOINT, QUERY);
    ///NOLINTEND(cert-err58-cpp)
};

}

FMT_OSTREAM(NES::LLMSink);
