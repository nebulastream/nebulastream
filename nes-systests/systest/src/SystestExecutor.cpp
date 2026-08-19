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
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/NESStrongTypeYaml.hpp> ///NOLINT(misc-include-cleaner)
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runner/QuerySubmitter.hpp>
#include <Runner/SystestRunner.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <rfl/json/write.hpp>
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <Progress.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <SystestState.hpp>
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
using OverrideQueriesMap = std::unordered_map<ConfigurationOverride, std::vector<Systest::SystestQuery>>;

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
            for (const auto& [key, value] : overrideConfig)
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

SystestExecutorResult SystestExecutor::executeSystests()
{
    setupLogging(config);

    CPPTRACE_TRY
    {
        /// Read the configuration
        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());

        auto discoveredTestFiles = discoverTestFiles(config);
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

        if (!config.remoteWorker.getValue())
        {
            /// Enable in-memory communication between workers
            enable_memcom();
        }

        if (queries.empty())
        {
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "No queries were run.",
                .errorCode = ErrorCode::TestException};
        }

        progressTracker.reset();

        if (config.endlessMode)
        {
            runEndlessMode(queries);
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = "Endless mode should not stop.",
                .errorCode = ErrorCode::TestException};
        }

        if (config.randomQueryOrder)
        {
            std::mt19937 rng(std::random_device{}());
            std::ranges::shuffle(queries, rng);
        }
        const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
        std::vector<Systest::RunningQuery> failedQueries;
        if (config.remoteWorker.getValue())
        {
            progressTracker.reset();
            progressTracker.setTotalQueries(queries.size());
            const Systest::QueryPerformanceMessageBuilder performanceMessage = config.showQueryPerformance.getValue()
                ? Systest::QueryPerformanceMessageBuilder{[](Systest::RunningQuery& runningQuery)
                                                          { return fmt::format(" in {}", runningQuery.getElapsedTime()); }}
                : Systest::QueryPerformanceMessageBuilder{Systest::discardPerformanceMessage};
            auto failed
                = runQueriesAtRemoteWorker(queries, numberConcurrentQueries, config.clusterConfig, progressTracker, performanceMessage);
            failedQueries.insert(failedQueries.end(), failed.begin(), failed.end());
        }
        else
        {
            auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{});
            if (not config.workerConfig.getValue().empty())
            {
                singleNodeWorkerConfiguration.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
            }
            else if (config.singleNodeWorkerConfig.has_value())
            {
                singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value();
            }
            if (config.benchmark)
            {
                std::vector<Systest::BenchmarkResult> benchmarkResults;
                std::vector<Systest::SystestQuery> benchmarkQueries;
                benchmarkQueries.reserve(queries.size());

                for (const auto& query : queries)
                {
                    if (query.differentialQueryPlan.has_value())
                    {
                        std::cout << "Skipping differential query for benchmarking: " << query.testName << ":"
                                  << query.queryIdInFile.toString() << "\n";
                        continue;
                    }

                    if (std::holds_alternative<ExpectedError>(query.expectation))
                    {
                        std::cout << "Skipping query expecting error for benchmarking: " << query.testName << ":"
                                  << query.queryIdInFile.toString() << "\n";
                        continue;
                    }

                    benchmarkQueries.push_back(query);
                }

                /// Benchmarked queries honour their configuration override just like the regular path does: queries
                /// are grouped by override and each group runs against a worker configured with it.
                std::unordered_map<ConfigurationOverride, std::vector<Systest::SystestQuery>> benchmarkQueriesByOverride;
                for (const auto& query : benchmarkQueries)
                {
                    benchmarkQueriesByOverride[query.configurationOverride].push_back(query);
                }

                progressTracker.reset();
                progressTracker.setTotalQueries(benchmarkQueries.size());
                for (const auto& [overrideConfig, queriesForConfig] : benchmarkQueriesByOverride)
                {
                    auto configCopy = singleNodeWorkerConfiguration;
                    for (const auto& [key, value] : overrideConfig)
                    {
                        configCopy.overwriteConfigWithCommandLineInput({{key, value}});
                    }
                    auto failed
                        = runQueriesAndBenchmark(queriesForConfig, configCopy, benchmarkResults, config.clusterConfig, progressTracker);
                    failedQueries.insert(failedQueries.end(), failed.begin(), failed.end());
                }
                const auto serializedResults = rfl::json::write(benchmarkResults, rfl::json::pretty);
                std::cout << serializedResults;
                const auto outputPath = std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json";
                std::ofstream outputFile(outputPath);
                outputFile << serializedResults;
                outputFile.close();
            }
            else
            {
                std::unordered_map<ConfigurationOverride, std::vector<Systest::SystestQuery>> queriesByOverride;
                for (const auto& query : queries)
                {
                    queriesByOverride[query.configurationOverride].push_back(query);
                }

                progressTracker.reset();
                progressTracker.setTotalQueries(queries.size());
                for (const auto& [overrideConfig, queriesForConfig] : queriesByOverride)
                {
                    auto configCopy = singleNodeWorkerConfiguration;
                    for (const auto& [key, value] : overrideConfig)
                    {
                        configCopy.overwriteConfigWithCommandLineInput({{key, value}});
                    }
                    const Systest::QueryPerformanceMessageBuilder performanceMessage = config.showQueryPerformance.getValue()
                        ? Systest::QueryPerformanceMessageBuilder{[](Systest::RunningQuery& runningQuery)
                                                                  { return fmt::format(" in {}", runningQuery.getElapsedTime()); }}
                        : Systest::QueryPerformanceMessageBuilder{Systest::discardPerformanceMessage};
                    auto failed = runQueriesAtLocalWorker(
                        queriesForConfig, numberConcurrentQueries, config.clusterConfig, configCopy, progressTracker, performanceMessage);
                    failedQueries.insert(failedQueries.end(), failed.begin(), failed.end());
                }
            }
        }
        if (not failedQueries.empty())
        {
            std::stringstream outputMessage;
            outputMessage << fmt::format("The following queries failed:\n[Name, Command]\n- {}", fmt::join(failedQueries, "\n- "));
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = outputMessage.str(),
                .errorCode = ErrorCode::QueryStatusFailed};
        }
        std::stringstream outputMessage;
        outputMessage << '\n' << "All queries passed.";
        return {.returnType = SystestExecutorResult::ReturnType::SUCCESS, .outputMessage = outputMessage.str()};
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
