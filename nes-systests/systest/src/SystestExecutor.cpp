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
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

#include <Configurations/Util.hpp>
#include <QueryManager/EmbeddedWorkerQueryManager.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <SystestConfiguration.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>
#include <from_current.hpp>


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
    const URI& grpcURI,
    Systest::SystestProgressTracker& progressTracker)
{
    auto queryManager = std::make_unique<GRPCQueryManager>(grpc::CreateChannel(grpcURI.toString(), grpc::InsecureChannelCredentials()));
    Systest::QuerySubmitter querySubmitter(std::move(queryManager));

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

            auto queryManager = std::make_unique<EmbeddedWorkerQueryManager>(configCopy);
            Systest::QuerySubmitter querySubmitter(std::move(queryManager));

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
    const auto grpcURI = config.grpcAddressUri.getValue();
    const bool runRemote = not grpcURI.empty();

    if (runRemote)
    {
        runEndlessRemote(queriesByOverride, rng, numberConcurrentQueries, grpcURI, progressTracker);
    }
    else
    {
        runEndlessLocal(queriesByOverride, rng, numberConcurrentQueries, singleNodeWorkerConfiguration, progressTracker);
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

    if (exists(symlinkPath) || is_symlink(symlinkPath))
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
        Systest::SystestBinder binder{config.workingDir.getValue(), config.testDataDir.getValue(), config.configDir.getValue()};
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

        if (config.sequentialQueryOrder)
        {
            std::ranges::sort(queries, {}, &Systest::SystestQuery::queryIdInFile);
        }

        const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
        std::vector<Systest::RunningQuery> failedQueries;
        if (const auto grpcURI = config.grpcAddressUri.getValue(); not grpcURI.empty())
        {
            progressTracker.reset();
            progressTracker.setTotalQueries(queries.size());
            auto failed = runQueriesAtRemoteWorker(queries, numberConcurrentQueries, grpcURI, progressTracker);
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
                nlohmann::json benchmarkResults;
                progressTracker.reset();
                progressTracker.setTotalQueries(queries.size());
                auto failed = runQueriesAndBenchmark(queries, singleNodeWorkerConfiguration, benchmarkResults, progressTracker);
                failedQueries.insert(failedQueries.end(), failed.begin(), failed.end());
                std::cout << benchmarkResults.dump(4);
                const auto outputPath = std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json";
                std::ofstream outputFile(outputPath);
                outputFile << benchmarkResults.dump(4);
                outputFile.close();
            }
            else if (config.sequentialQueryOrder)
            {
                std::map<Systest::SystestQueryId, Systest::ConfigurationOverride> queriesByOverride;
                for (const auto& query : queries)
                {
                    queriesByOverride[query.queryIdInFile] = query.configurationOverride;
                }

                progressTracker.reset();
                progressTracker.setTotalQueries(queries.size());
                for (const auto& [queryId, overrideConfig] : queriesByOverride)
                {
                    auto configCopy = singleNodeWorkerConfiguration;
                    for (const auto& [key, value] : overrideConfig.overrideParameters)
                    {
                        configCopy.overwriteConfigWithCommandLineInput({{key, value}});
                    }

                    auto failed = runQueriesAtLocalWorker(
                        {queries.at(queryId.getRawValue() - 1)}, numberConcurrentQueries, configCopy, progressTracker);
                    failedQueries.insert(failedQueries.end(), failed.begin(), failed.end());
                }
            }
            else
            {
                std::unordered_map<Systest::ConfigurationOverride, std::vector<Systest::SystestQuery>> queriesByOverride;
                for (const auto& query : queries)
                {
                    queriesByOverride[query.configurationOverride].push_back(query);
                }

                progressTracker.reset();
                progressTracker.setTotalQueries(queries.size());
                for (const auto& [overrideConfig, queriesForConfig] : queriesByOverride)
                {
                    auto configCopy = singleNodeWorkerConfiguration;
                    for (const auto& [key, value] : overrideConfig.overrideParameters)
                    {
                        configCopy.overwriteConfigWithCommandLineInput({{key, value}});
                    }

                    auto failed = runQueriesAtLocalWorker(queriesForConfig, numberConcurrentQueries, configCopy, progressTracker);
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
