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
#include <expected>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SystestQueryModel.hpp>
#include <SystestResolver.hpp>

namespace NES
{
class ModelCatalog;
class SinkCatalog;
class SourceCatalog;
}

namespace NES::Systest
{

struct PreparedSourceMetric
{
    std::filesystem::path file;
    uint64_t occurrences = 0;
};

struct PreparedStatement
{
    std::string sql;
    DistributedLogicalPlan plan;
    ResultSchema outputSchema;
    OutputTarget output;
    std::vector<PreparedSourceMetric> sourceMetrics;
};

struct PreparedQuery
{
    PreparedStatement statement;
};

struct PreparedDifferential
{
    PreparedStatement primary;
    PreparedStatement differential;
};

struct PreparedExplain
{
    std::string sql;
    std::string output;
};

using PreparedAction = std::variant<PreparedQuery, PreparedDifferential, PreparedExplain>;

struct PlanningFailure
{
    ErrorCode code = ErrorCode::UnknownException;
    std::string message;
};

struct PreparedExecution
{
    EnvironmentId environment;
    std::variant<std::shared_ptr<const PreparedAction>, PlanningFailure> prepared;
};

class PreparedExecutionCatalog
{
public:
    explicit PreparedExecutionCatalog(std::map<TestCaseId, PreparedExecution> executions);

    [[nodiscard]] const PreparedExecution& at(const TestCaseId& id) const;
    [[nodiscard]] const PreparedExecution* find(const TestCaseId& id) const;
    [[nodiscard]] std::vector<TestCaseId> ids() const;

private:
    const std::map<TestCaseId, PreparedExecution> executions;
};

class PreparedEnvironmentContext
{
public:
    ~PreparedEnvironmentContext();

    [[nodiscard]] std::shared_ptr<const SourceCatalog> sourceCatalog() const;
    [[nodiscard]] std::shared_ptr<const SinkCatalog> sinkCatalog() const;
    [[nodiscard]] std::shared_ptr<const ModelCatalog> modelCatalog() const;
    [[nodiscard]] const std::vector<std::filesystem::path>& generatedAttachments() const;
    [[nodiscard]] size_t sourceThreadCount() const;

private:
    struct Impl;

    explicit PreparedEnvironmentContext(std::shared_ptr<Impl> impl);

    std::shared_ptr<Impl> impl;

    friend class EnvironmentPreparer;
    friend class ExecutionPreparer;
};

struct PreparedEnvironment
{
    EnvironmentId id;
    std::shared_ptr<PreparedEnvironmentContext> context;
};

class PreparedEnvironmentCatalog
{
public:
    explicit PreparedEnvironmentCatalog(std::map<EnvironmentId, PreparedEnvironment> environments);

    [[nodiscard]] const PreparedEnvironment& at(EnvironmentId id) const;
    [[nodiscard]] const PreparedEnvironment* find(EnvironmentId id) const;
    [[nodiscard]] std::vector<EnvironmentId> ids() const;

private:
    const std::map<EnvironmentId, PreparedEnvironment> environments;
};

class EnvironmentPreparer
{
public:
    [[nodiscard]] std::expected<std::shared_ptr<PreparedEnvironmentCatalog>, Exception> prepare(const ResolvedRun& run) const;
};

class ExecutionPreparer
{
public:
    ExecutionPreparer(
        std::filesystem::path workingDirectory,
        std::filesystem::path testDataDirectory,
        QueryOptimizerConfiguration queryOptimizerConfiguration);

    [[nodiscard]] std::expected<std::shared_ptr<const PreparedExecutionCatalog>, Exception>
    prepare(const ResolvedRun& run, PreparedEnvironmentCatalog& environments) const;

private:
    std::filesystem::path workingDirectory;
    std::filesystem::path testDataDirectory;
    QueryOptimizerConfiguration queryOptimizerConfiguration;
};

struct PreparedRun
{
    ResolvedRun resolved;
    std::shared_ptr<const PreparedEnvironmentCatalog> environments;
    std::shared_ptr<const PreparedExecutionCatalog> executions;
};

[[nodiscard]] std::expected<PreparedRun, Exception> prepareSystestRun(
    ResolvedRun run,
    const std::filesystem::path& workingDirectory,
    const std::filesystem::path& testDataDirectory,
    const QueryOptimizerConfiguration& queryOptimizerConfiguration);

}
