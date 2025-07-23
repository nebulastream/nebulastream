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
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>
#include <unistd.h>

#include <Configurations/Util.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
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

void SystestExecutor::runEndlessMode(const std::vector<Systest::SystestQuery>& queries)
{
    std::cout << std::format("Running endlessly over a total of {} queries (across all configuration overrides).", queries.size()) << '\n';

    Systest::SystestProgressTracker progressTracker{queries.size()};
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
    std::mt19937 rng(std::random_device{}());
    const auto grpcURI = config.grpcAddressUri.getValue();
    const auto runRemote = not grpcURI.empty();

    std::unordered_map<Systest::ConfigurationOverride, std::vector<Systest::SystestQuery>> queriesByOverride;
    for (const auto& query : queries)
    {
        queriesByOverride[query.configurationOverride].push_back(query);
    }

    std::unordered_map<Systest::ConfigurationOverride, std::unique_ptr<Systest::QuerySubmitter>> submitters;
    for (const auto& [overrideConfig, _] : queriesByOverride)
    {
        if (runRemote)
        {
            submitters[overrideConfig] = std::make_unique<Systest::RemoteWorkerQuerySubmitter>(grpcURI);
        }
        else
        {
            auto configCopy = singleNodeWorkerConfiguration;
            for (const auto& [key, value] : overrideConfig)
            {
                configCopy.overwriteConfigWithCommandLineInput({{key, value}});
            }
            submitters[overrideConfig] = std::make_unique<Systest::LocalWorkerQuerySubmitter>(configCopy);
        }
    }

    while (true)
    {
        for (auto& [overrideConfig, queries] : queriesByOverride)
        {
            std::ranges::shuffle(queries, rng);
            auto& submitter = *submitters[overrideConfig];
            const auto failedQueries = runQueries(queries, numberConcurrentQueries, submitter, progressTracker);
            if (!failedQueries.empty())
            {
                std::stringstream outputMessage;
                outputMessage << fmt::format("The following queries failed:\n[Name, Command]\n- {}", fmt::join(failedQueries, "\n- "));
                NES_ERROR("{}", outputMessage.str());
                std::cout << '\n' << outputMessage.str() << '\n';
                std::exit(1); ///NOLINT(concurrency-mt-unsafe)
            }
        }
    }
}

SystestExecutorResult SystestExecutor::executeSystests()
{
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

        progressTracker.setTotalQueries(queries.size());
        const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
        std::vector<Systest::RunningQuery> failedQueries;
        if (const auto grpcURI = config.grpcAddressUri.getValue(); not grpcURI.empty())
        {
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
                auto failed = runQueriesAndBenchmark(queries, singleNodeWorkerConfiguration, benchmarkResults, progressTracker);
                failedQueries.insert(failedQueries.end(), failed.begin(), failed.end());
                std::cout << benchmarkResults.dump(4);
                const auto outputPath = std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json";
                std::ofstream outputFile(outputPath);
                outputFile << benchmarkResults.dump(4);
                outputFile.close();
            }
            else
            {
                std::unordered_map<Systest::ConfigurationOverride, std::vector<Systest::SystestQuery>> queriesByOverride;
                for (const auto& query : queries)
                {
                    queriesByOverride[query.configurationOverride].push_back(query);
                }

                for (const auto& [overrideConfig, queriesForConfig] : queriesByOverride)
                {
                    if (!config.workerConfig.getValue().empty())
                    {
                        singleNodeWorkerConfiguration.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
                    }
                    else if (config.singleNodeWorkerConfig.has_value())
                    {
                        singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value();
                    }

                    auto configCopy = singleNodeWorkerConfiguration;
                    for (const auto& [key, value] : overrideConfig)
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
