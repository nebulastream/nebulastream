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

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/// Defines the names, defaults, validation & config functions, for all HTTP config parameters.
struct ConfigParametersHTTP
{
    /// Mandatory endpoint, e.g. "http://localhost:8000/"
    static inline const DescriptorConfig::ConfigParameter<std::string> ENDPOINT{
        "endpoint",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ENDPOINT, config); }};

    /// Poll interval in milliseconds.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> INTERVAL_MS{
        "interval_ms",
        1000,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto v = DescriptorConfig::tryGet(INTERVAL_MS, config);
            return v;
        }};

    /// TCP connect timeout for the HTTP request (seconds)
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT{
        "connect_timeout_seconds",
        10,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT, config); }};

    /// Optional: append a newline after body (useful if downstream expects delimiter separated messages)
    static inline const DescriptorConfig::ConfigParameter<bool> APPEND_NEWLINE{
        "append_newline",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(APPEND_NEWLINE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, ENDPOINT, INTERVAL_MS, CONNECT_TIMEOUT, APPEND_NEWLINE);
};

class RestApiSource final : public Source
{
    constexpr static size_t ERROR_MESSAGE_BUFFER_SIZE = 256;

public:
    static const std::string& name()
    {
        static const std::string Instance = "RestApi";
        return Instance;
    }

    explicit RestApiSource(const SourceDescriptor& sourceDescriptor);
    ~RestApiSource() override = default;

    RestApiSource(const RestApiSource&) = delete;
    RestApiSource& operator=(const RestApiSource&) = delete;
    RestApiSource(RestApiSource&&) = delete;
    RestApiSource& operator=(RestApiSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct ParsedUrl
    {
        std::string host;
        std::string port;
        std::string path;
    };

private:
    static ParsedUrl parseHttpUrlOrThrow(const std::string& url);
    bool shouldPoll() const;
    size_t fetchOnceAndWriteBody(TupleBuffer& tupleBuffer);

    /// Performs HTTP GET and returns response body (no headers). Basic HTTP/1.1, no TLS.
    std::string httpGetBody(const ParsedUrl& u);

private:
    std::array<char, ERROR_MESSAGE_BUFFER_SIZE> errBuffer{};

    std::string endpoint;
    ParsedUrl parsed{};

    std::chrono::milliseconds interval;
    std::chrono::steady_clock::time_point lastPoll;

    uint32_t connectTimeoutSeconds;
    bool appendNewline;

    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};

}
