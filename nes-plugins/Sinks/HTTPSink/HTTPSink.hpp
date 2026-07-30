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
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <curl/curl.h>

#include <folly/Synchronized.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
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
    std::string logFilePath;
    CURL* curl;
    bool isOpen = false;
    std::ofstream logFile;

    /// Serialises execute() across all HTTPSink instances. Two reasons it
    /// has to be process-wide rather than per-instance:
    ///  1. Multiple sinks (different queries) may share the same log file —
    ///     concurrent writes would interleave and corrupt lines.
    ///  2. A libcurl easy handle is not safe to use from multiple threads
    ///     concurrently. Even though each HTTPSink instance owns its own
    ///     handle, the query engine can dispatch buffers to a single sink
    ///     instance from any worker thread. Without the lock, one thread's
    ///     curl_slist_free_all or stack-resident payload string could be
    ///     torn down while another thread's curl_easy_perform is still
    ///     reading it (segfault in Curl_checkheaders / Curl_http).
    static std::mutex executeMutex;
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

    static inline const DescriptorConfig::ConfigParameter<std::string> LOG_FILE_PATH{
        "log_file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LOG_FILE_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, IPADDRESS, PORT, ENDPOINT, LOG_FILE_PATH);
};

}

FMT_OSTREAM(NES::HTTPSink);