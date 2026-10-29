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
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <time.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Schema/Schema.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Configurations/ConfigField.hpp>

namespace NES
{

struct ProcessSourceConfig
{
    std::string command;
    uint64_t refreshIntervalMs;
    uint64_t flushIntervalMs;

    static std::expected<ProcessSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Repeatedly executes a Bash command and exposes its standard output as a raw byte stream.
/// A process is never executed concurrently with its preceding invocation. If an invocation takes
/// longer than refreshInterval, the next invocation starts as soon as the previous one finishes.
class ProcessSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Process";

    ProcessSource(const ProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~ProcessSource() override;

    ProcessSource(const ProcessSource&) = delete;
    ProcessSource& operator=(const ProcessSource&) = delete;
    ProcessSource(ProcessSource&&) = delete;
    ProcessSource& operator=(ProcessSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    [[nodiscard]] std::string_view getType() const override { return NAME; }

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

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

}
