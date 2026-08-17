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

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/RawSource.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <sys/types.h>

namespace NES
{

/// Repeatedly executes a Bash command and exposes its standard output as a raw byte stream.
/// A process is never executed concurrently with its preceding invocation. If an invocation takes
/// longer than refreshInterval, the next invocation starts as soon as the previous one finishes.
class ProcessSource final : public RawSource
{
public:
    static constexpr std::string_view NAME = "Process";

    explicit ProcessSource(const SourceDescriptor& sourceDescriptor);
    ~ProcessSource() override;

    ProcessSource(const ProcessSource&) = delete;
    ProcessSource& operator=(const ProcessSource&) = delete;
    ProcessSource(ProcessSource&&) = delete;
    ProcessSource& operator=(ProcessSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    [[nodiscard]] std::string_view getType() const override { return NAME; }

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    FillTupleBufferResult fillRaw(std::span<std::byte> out, const std::stop_token& stopToken) override;

private:
    void startProcess();
    void reapProcess();
    void terminateProcess() noexcept;
    size_t readAvailable(std::span<std::byte> out);
    [[nodiscard]] bool processFinished() const noexcept;

    std::string command;
    uint64_t sourceId;
    uint64_t nextInvocation{1};
    std::chrono::milliseconds refreshInterval;
    std::chrono::milliseconds flushInterval;
    std::chrono::steady_clock::time_point nextRefresh;
    pid_t childPid{-1};
    pid_t processGroupId{-1};
    int standardOutputFd{-1};
    bool isOpen{false};
};

struct ConfigParametersProcessSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> COMMAND{
        "COMMAND",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(COMMAND, config);
            return value && !value->empty() ? value : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint64_t> REFRESH_INTERVAL_MS{
        "REFRESH_INTERVAL_MS",
        1000,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(REFRESH_INTERVAL_MS, config);
            return value && *value > 0 && *value <= static_cast<uint64_t>(std::chrono::milliseconds::max().count()) ? value : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        100,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config);
            return value && *value > 0 && *value <= static_cast<uint64_t>(std::chrono::milliseconds::max().count()) ? value : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, COMMAND, REFRESH_INTERVAL_MS, FLUSH_INTERVAL_MS);
};

}
