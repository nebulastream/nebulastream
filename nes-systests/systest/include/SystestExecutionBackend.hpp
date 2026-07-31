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
#include <span>
#include <stop_token>
#include <string>
#include <variant>

#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestPreparation.hpp>
#include <SystestQueryModel.hpp>

namespace NES
{
class DistributedException;
class Exception;
}

namespace NES::Systest
{

enum class ExecutionRole : uint8_t
{
    Primary,
    Differential
};

struct ExecutionRequest
{
    TestCaseId testCase;
    uint64_t sequenceNumber = 0;
    ExecutionRole role = ExecutionRole::Primary;
    std::string sql;
    OutputTarget output;
    bool collectMetrics = false;
    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max();
    std::chrono::milliseconds cancellationGracePeriod{0};
};

struct BackendCapabilities
{
    bool supportsConfigurationOverrides = false;
    bool supportsRemoteFixtures = false;
    bool supportsExplain = false;
    size_t maximumConcurrency = 1;
};

struct ExecutionHandle
{
    uint64_t value = 0;

    auto operator<=>(const ExecutionHandle&) const = default;
};

struct StatementFailure
{
    ExecutionStage stage = ExecutionStage::Running;
    ExecutionError error;
    ArtifactSet artifacts;
};

ExecutionError runtimeExecutionError(const Exception& exception);
ExecutionError runtimeExecutionError(const DistributedException& exception);

enum class BackendFaultKind : uint8_t
{
    Failure,
    DeadlineReached,
    Cancelled,
    TeardownFailed
};

struct BackendFault
{
    BackendFaultKind kind = BackendFaultKind::Failure;
    ErrorCode code = ErrorCode::TestException;
    std::string message;
};

struct StatementCompletion
{
    ExecutionHandle handle;
    std::variant<StatementOutput, StatementFailure> result;
    ExecutionMetrics metrics;
    ArtifactSet artifacts;
};

class ExecutionSession
{
public:
    virtual ~ExecutionSession() = default;

    virtual std::expected<std::variant<ExecutionHandle, StatementFailure>, BackendFault> start(const ExecutionRequest&, std::stop_token)
        = 0;

    virtual std::expected<StatementCompletion, BackendFault>
    waitAny(std::span<const ExecutionHandle> active, std::chrono::steady_clock::time_point deadline, std::stop_token stopToken) = 0;

    virtual std::expected<void, BackendFault> cancel(ExecutionHandle, std::chrono::steady_clock::time_point deadline) = 0;
    virtual std::expected<void, BackendFault> close(std::chrono::steady_clock::time_point deadline) = 0;
};

class ExecutionBackend
{
public:
    virtual ~ExecutionBackend() = default;

    virtual BackendCapabilities capabilities() const = 0;
    virtual std::expected<std::unique_ptr<ExecutionSession>, BackendFault> open(const EnvironmentSpec&) = 0;
};

class EmbeddedExecutionBackend final : public ExecutionBackend
{
public:
    EmbeddedExecutionBackend(
        std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions, SingleNodeWorkerConfiguration baseConfiguration);

    BackendCapabilities capabilities() const override;
    std::expected<std::unique_ptr<ExecutionSession>, BackendFault> open(const EnvironmentSpec&) override;

private:
    std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions;
    SingleNodeWorkerConfiguration baseConfiguration;
};

class RemoteExecutionBackend final : public ExecutionBackend
{
public:
    explicit RemoteExecutionBackend(std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions);

    BackendCapabilities capabilities() const override;
    std::expected<std::unique_ptr<ExecutionSession>, BackendFault> open(const EnvironmentSpec&) override;

private:
    std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions;
};

}
