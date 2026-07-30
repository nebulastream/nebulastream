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

#include <SystestExecutor.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <rfl/json/write.hpp>
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <SystestConfiguration.hpp>
#include <SystestCoordinator.hpp>
#include <SystestExecutionBackend.hpp>
#include <SystestProgressTracker.hpp>
#include <SystestReporter.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>
#include <SystestValidation.hpp>
#include <WorkerCatalog.hpp>

/// Rust FFI function that enables in-memory communication channels for embedded multi-worker mode.
/// Configures the network layer to use shared memory instead of real network sockets
/// when workers run in the same process. Prevents actual port allocation.
extern void enable_memcom();

using namespace std::literals;

namespace NES
{
namespace
{
using OverrideQueriesMap = std::unordered_map<Systest::ConfigurationOverride, std::vector<Systest::SystestQuery>>;

void exitOnFailureIfNeeded(const std::vector<Systest::RunningQuery>& failedQueries, const size_t totalQueries)
{
    if (failedQueries.empty())
    {
        return;
    }

    std::stringstream outputMessage;
    outputMessage << fmt::format(
        "The following queries ({} of {}) failed:\n[Name, Command]\n- {}",
        failedQueries.size(),
        totalQueries,
        fmt::join(failedQueries, "\n- "));
    NES_ERROR("{}", outputMessage.str());
    std::cout << '\n' << outputMessage.str() << '\n';
    std::exit(1); ///NOLINT(concurrency-mt-unsafe)
}

[[noreturn]] void runEndlessRemote(
    const OverrideQueriesMap& queriesByOverride,
    std::mt19937& rng,
    const uint64_t numberConcurrentQueries,
    const SystestClusterConfiguration& clusterConfig,
    Systest::SystestProgressTracker& progressTracker)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>(clusterConfig.workers);

    Systest::QuerySubmitter querySubmitter(std::make_unique<QueryManager>(std::move(workerCatalog), createGRPCBackend()));

    while (true)
    {
        progressTracker.reset();
        const size_t totalRemote = std::accumulate(
            queriesByOverride.begin(),
            queriesByOverride.end(),
            static_cast<size_t>(0),
            [](size_t acc, const auto& entry) { return acc + entry.second.size(); });
        progressTracker.setTotalQueries(totalRemote);
        for (const auto& entry : queriesByOverride)
        {
            auto shuffledQueries = entry.second;
            std::ranges::shuffle(shuffledQueries, rng);
            const auto failedQueries = Systest::runQueries(
                shuffledQueries, numberConcurrentQueries, querySubmitter, progressTracker, Systest::discardPerformanceMessage);
            exitOnFailureIfNeeded(failedQueries, shuffledQueries.size());
        }
    }
}

[[noreturn]] void runEndlessLocal(
    const OverrideQueriesMap& queriesByOverride,
    std::mt19937& rng,
    const uint64_t numberConcurrentQueries,
    const SystestClusterConfiguration& clusterConfig,
    const SingleNodeWorkerConfiguration& baseConfiguration,
    Systest::SystestProgressTracker& progressTracker)
{
    while (true)
    {
        progressTracker.reset();
        const size_t totalLocal = std::accumulate(
            queriesByOverride.begin(),
            queriesByOverride.end(),
            static_cast<size_t>(0),
            [](size_t acc, const auto& entry) { return acc + entry.second.size(); });
        progressTracker.setTotalQueries(totalLocal);
        for (const auto& [overrideConfig, queriesForConfig] : queriesByOverride)
        {
            auto configCopy = baseConfiguration;
            for (const auto& [key, value] : overrideConfig.overrideParameters)
            {
                configCopy.overwriteConfigWithCommandLineInput({{key, value}});
            }

            auto workerCatalog = std::make_shared<WorkerCatalog>(clusterConfig.workers);

            Systest::QuerySubmitter querySubmitter(
                std::make_unique<QueryManager>(std::move(workerCatalog), createEmbeddedBackend(configCopy)));

            auto shuffledQueries = queriesForConfig;
            std::ranges::shuffle(shuffledQueries, rng);
            const auto failedQueries = Systest::runQueries(
                shuffledQueries, numberConcurrentQueries, querySubmitter, progressTracker, Systest::discardPerformanceMessage);
            exitOnFailureIfNeeded(failedQueries, shuffledQueries.size());
        }
    }
}
}

SystestExecutor::SystestExecutor(SystestConfiguration config) : config(std::move(config))
{
}

void SystestExecutor::runEndlessMode(const std::vector<Systest::SystestQuery>& queries)
{
    std::cout << std::format("Running endlessly over a total of {} queries (across all configuration overrides).", queries.size()) << '\n';

    const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
    auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{});
    if (not config.workerConfig.getValue().empty())
    {
        singleNodeWorkerConfiguration.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
    }
    else if (config.singleNodeWorkerConfig.has_value())
    {
        singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value();
    }

    OverrideQueriesMap queriesByOverride;
    for (const auto& query : queries)
    {
        queriesByOverride[query.configurationOverride].push_back(query);
    }

    std::mt19937 rng(std::random_device{}());

    if (config.remoteWorker.getValue())
    {
        runEndlessRemote(queriesByOverride, rng, numberConcurrentQueries, config.clusterConfig, progressTracker);
    }
    else
    {
        runEndlessLocal(
            queriesByOverride, rng, numberConcurrentQueries, config.clusterConfig, singleNodeWorkerConfiguration, progressTracker);
    }
}

void createSymlink(const std::filesystem::path& absoluteLogPath, const std::filesystem::path& symlinkPath)
{
    std::error_code errorCode;
    const auto relativeLogPath = relative(absoluteLogPath, symlinkPath.parent_path(), errorCode);
    if (errorCode)
    {
        std::cerr << "Error calculating relative path during logger setup: " << errorCode.message() << "\n";
        return;
    }

    if (exists(symlinkPath, errorCode) || is_symlink(symlinkPath, errorCode))
    {
        std::filesystem::remove(symlinkPath, errorCode);
        if (errorCode)
        {
            std::cerr << "Error removing existing symlink during logger setup:  " << errorCode.message() << "\n";
        }
    }

    try
    {
        create_symlink(relativeLogPath, symlinkPath);
    }
    catch (const std::filesystem::filesystem_error& e)
    {
        std::cerr << "Error creating symlink during logger setup: " << e.what() << '\n';
    }
}

void setupLogging(const SystestConfiguration& config)
{
    std::filesystem::path absoluteLogPath;
    const std::filesystem::path logDir = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests";

    if (config.logFilePath.getValue().empty())
    {
        std::error_code errorCode;
        create_directories(logDir, errorCode);
        if (errorCode)
        {
            std::cerr << "Error creating log directory during logger setup: " << errorCode.message() << "\n";
            return;
        }

        const auto now = std::chrono::system_clock::now();
        const auto pid = ::getpid();
        const std::string logFileName = fmt::format("SystemTest_{:%Y-%m-%d_%H-%M-%S}_{:d}.log", now, pid);

        absoluteLogPath = logDir / logFileName;
    }
    else
    {
        absoluteLogPath = config.logFilePath.getValue();
        const std::filesystem::path parentDir = absoluteLogPath.parent_path();
        if (not exists(parentDir) or not is_directory(parentDir))
        {
            fmt::println(std::cerr, "Error creating log file during logger setup: directory does not exist: file://{}", parentDir.string());
            std::exit(1); /// NOLINT(concurrency-mt-unsafe)
        }
    }

    fmt::println(std::cout, "Find the log at: file://{}", absoluteLogPath.string());
    Logger::setupLogging(absoluteLogPath.string(), LogLevel::LOG_DEBUG, false);

    const auto symlinkPath = logDir / "latest.log";
    createSymlink(absoluteLogPath, symlinkPath);
}

SystestExecutorResult SystestExecutor::executeSystests()
{
    setupLogging(config);

    CPPTRACE_TRY
    {
        /// Read the configuration
        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());

        auto discoveredTestFiles = Systest::loadTestFileMap(config);
        Systest::SystestBinder binder{
            config.workingDir.getValue(),
            config.testDataDir.getValue(),
            config.configDir.getValue(),
            config.queryOptimizerConfig.value_or(QueryOptimizerConfiguration{}),
            config.clusterConfig};
        auto [queries, loadedFiles] = binder.loadOptimizeQueries(discoveredTestFiles);
        if (loadedFiles != discoveredTestFiles.size())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "Could not load all test files. Terminating.",
                .errorCode = ErrorCode::TestException};
        }

        if (queries.empty())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "No queries were run.",
                .errorCode = ErrorCode::TestException};
        }

        auto resolved = Systest::resolveSystestQueries(std::move(queries), config.testsDiscoverDir.getValue(), config.clusterConfig);
        if (!resolved)
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = resolved.error().what(),
                .errorCode = resolved.error().code()};
        }

        if (!config.remoteWorker.getValue())
        {
            enable_memcom();
        }

        auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{});
        if (not config.workerConfig.getValue().empty())
        {
            singleNodeWorkerConfiguration.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
        }

        std::unique_ptr<Systest::ExecutionBackend> backend;
        if (config.remoteWorker.getValue())
        {
            backend = std::make_unique<Systest::RemoteExecutionBackend>(resolved->preparedCases);
        }
        else
        {
            backend = std::make_unique<Systest::EmbeddedExecutionBackend>(resolved->preparedCases, singleNodeWorkerConfiguration);
        }

        if (config.benchmark.getValue() && config.endlessMode.getValue())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "Benchmark and endless execution modes cannot be combined.",
                .errorCode = ErrorCode::InvalidConfigParameter};
        }

        Systest::RunSetup runSetup{
            .selection = Systest::TestSelection{.includeAll = false, .cases = resolved->preparedCases->ids()},
            .ordering = Systest::
                OrderingPolicy{.kind = config.randomQueryOrder.getValue() ? Systest::OrderingKind::Shuffled : Systest::OrderingKind::SourceOrder, .seed = std::nullopt},
            .concurrency = Systest::ConcurrencyPolicy{.maximumActiveCases = static_cast<size_t>(config.numberConcurrentQueries.getValue())},
            .repetition = config.endlessMode.getValue() ? Systest::RepetitionPolicy{Systest::UntilCancelled{}}
                                                        : Systest::RepetitionPolicy{Systest::Once{}},
            .failurePolicy
            = config.endlessMode.getValue() ? Systest::IndependentFailurePolicy::FailFast : Systest::IndependentFailurePolicy::Continue,
            .deadlines = {},
            .validation = {},
            .metrics = Systest::MetricsPolicy{
                .collect = config.benchmark.getValue() || config.showQueryPerformance.getValue(),
                .report = config.benchmark.getValue() || config.showQueryPerformance.getValue()}};

        if (config.benchmark.getValue())
        {
            runSetup.concurrency.maximumActiveCases = 1;
            runSetup.selection.cases.clear();
            for (const auto& id : resolved->preparedCases->ids())
            {
                const auto& preparedCase = resolved->preparedCases->at(id);
                const auto& query = preparedCase.query;
                if (const auto* action = std::get_if<Systest::QueryAction>(&preparedCase.definition->action);
                    action && action->kind == Systest::QueryKind::Explain)
                {
                    std::cout << "Skipping EXPLAIN query for benchmarking: " << query.testName << ":" << query.queryIdInFile.toString()
                              << "\n";
                    continue;
                }
                if (query.differentialQueryPlan)
                {
                    std::cout << "Skipping differential query for benchmarking: " << query.testName << ":" << query.queryIdInFile.toString()
                              << "\n";
                    continue;
                }
                if (std::holds_alternative<Systest::ExpectedError>(query.expectedResultsOrExpectedError))
                {
                    std::cout << "Skipping query expecting error for benchmarking: " << query.testName << ":"
                              << query.queryIdInFile.toString() << "\n";
                    continue;
                }
                runSetup.selection.cases.push_back(id);
            }
        }

        Systest::FileResultDecoder decoder;
        Systest::ResultComparator resultComparator;
        Systest::TextComparator textComparator;
        Systest::CaseValidator validator(decoder, resultComparator, textComparator);
        Systest::ConsoleRunReporter consoleReporter(*resolved, config.showQueryPerformance.getValue() || config.benchmark.getValue());
        std::vector<std::reference_wrapper<Systest::RunReporter>> reporters{consoleReporter};
        std::unique_ptr<Systest::BenchmarkRunReporter> benchmarkReporter;
        if (config.benchmark.getValue())
        {
            benchmarkReporter = std::make_unique<Systest::BenchmarkRunReporter>(
                *resolved, std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json");
            reporters.emplace_back(*benchmarkReporter);
        }
        Systest::CompositeRunReporter reporter(std::move(reporters));
        const auto summary = Systest::RunCoordinator{}.run(*resolved, std::move(runSetup), *backend, validator, reporter);

        const auto fatalDiagnostic = std::ranges::find_if(
            summary.diagnostics,
            [](const Systest::Diagnostic& diagnostic)
            {
                return diagnostic.kind == Systest::DiagnosticKind::Execution || diagnostic.kind == Systest::DiagnosticKind::Reporting
                    || diagnostic.kind == Systest::DiagnosticKind::Scheduling;
            });
        if (summary.failed != 0 || summary.cancelled || fatalDiagnostic != summary.diagnostics.end())
        {
            std::vector<std::string> failedQueries;
            for (const auto& result : summary.results)
            {
                if (result.verdict != Systest::Verdict::Failed)
                {
                    continue;
                }
                const auto& query = resolved->preparedCases->at(result.id).query;
                failedQueries.push_back(fmt::format("[{}, systest -t {}:{}]", query.testName, query.testFilePath, query.queryIdInFile));
            }
            std::string outputMessage;
            if (!failedQueries.empty())
            {
                outputMessage = fmt::format("The following queries failed:\n[Name, Command]\n- {}", fmt::join(failedQueries, "\n- "));
            }
            else if (summary.cancelled)
            {
                outputMessage = "The systest run was cancelled before completion.";
            }
            else
            {
                outputMessage = fmt::format("The systest run failed: {}", fatalDiagnostic->message);
            }
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = std::move(outputMessage),
                .errorCode = ErrorCode::QueryStatusFailed};
        }
        return {.returnType = SystestExecutorResult::ReturnType::SUCCESS, .outputMessage = "\nAll queries passed."};
    }
    CPPTRACE_CATCH(Exception & e)
    {
        tryLogCurrentException();
        const auto currentErrorCode = getCurrentErrorCode();
        return {
            .returnType = SystestExecutorResult::ReturnType::FAILED,
            .outputMessage = fmt::format("Failed with exception: {}, {}", currentErrorCode, e.what()),
            .errorCode = currentErrorCode};
    }
    return {
        .returnType = SystestExecutorResult::ReturnType::FAILED,
        .outputMessage = "Fatal error, should never reach this point.",
        .errorCode = ErrorCode::UnknownException};
}
}
