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
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
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
#include <argparse/argparse.hpp>
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
SystestConfiguration readConfiguration(int argc, const char** argv)
{
    using argparse::ArgumentParser;
    ArgumentParser program("systest");
    SystestConfiguration config;
    ArgParseVisitor parserCreator{program};
    config.accept(parserCreator);
    program.add_argument("--")
        .help("arguments passed to the worker config, e.g., `-- --worker.queryEngine.numberOfWorkerThreads=10`")
        .default_value(std::vector<std::string>{})
        .remaining();

    try
    {
        program.parse_args(argc, argv);
        ArgParseParserVisitor configCreator{program};
        config.accept(configCreator);
    }
    catch (const std::exception& e)
    {
        fmt::println(std::cerr, "{}", e.what());
        fmt::println(std::cerr, "{}", fmt::streamed(program));
        exit(-1);
    }

    auto workerArgs = program.get<std::vector<std::string>>("--");
    workerArgs.insert(workerArgs.begin(), "worker");

    SingleNodeWorkerConfiguration workerConfig;
    {
        ArgumentParser program("worker");
        ArgParseVisitor parserCreator{program};
        workerConfig.accept(parserCreator);
        try
        {
            ArgParseParserVisitor configCreator{program};
            program.parse_args(workerArgs);
            workerConfig.accept(configCreator);
        }
        catch (const std::exception& e)
        {
            fmt::println(std::cerr, "{}", e.what());
            fmt::println(std::cerr, "{}", fmt::streamed(program));
            exit(-1);
        }
    }
    config.singleNodeWorkerConfig = workerConfig;
    print(config, std::cout);
    print(*config.singleNodeWorkerConfig, std::cout);
    return config;
}

void runEndlessMode(std::vector<Systest::SystestQuery> queries, SystestConfiguration& config)
{
    std::cout << std::format("Running endlessly over a total of {} queries.", queries.size()) << '\n';

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

    auto queryManager = [&]() -> std::unique_ptr<QueryManager>
    {
        if (runRemote)
        {
            return std::make_unique<GRPCQueryManager>(grpc::CreateChannel(grpcURI, grpc::InsecureChannelCredentials()));
        }
        return std::make_unique<EmbeddedWorkerQueryManager>(singleNodeWorkerConfiguration);
    }();
    Systest::QuerySubmitter querySubmitter(std::move(queryManager));

    while (true)
    {
        std::ranges::shuffle(queries, rng);
        const auto failedQueries = runQueries(queries, numberConcurrentQueries, querySubmitter, Systest::discardPerformanceMessage);
        if (!failedQueries.empty())
        {
            std::stringstream outputMessage;
            outputMessage << fmt::format(
                "The following queries ({} of {}) failed:\n[Name, Command]\n- {}",
                failedQueries.size(),
                queries.size(),
                fmt::join(failedQueries, "\n- "));
            NES_ERROR("{}", outputMessage.str());
            std::cout << '\n' << outputMessage.str() << '\n';
            std::exit(1); ///NOLINT(concurrency-mt-unsafe)
        }
    }
}

void createSymlink(const std::filesystem::path& absoluteLogPath, const std::filesystem::path& symlinkPath)
{
    std::error_code errorCode;
    const auto relativeLogPath = std::filesystem::relative(absoluteLogPath, symlinkPath.parent_path(), errorCode);
    if (errorCode)
    {
        std::cerr << "Error calculating relative path during logger setup: " << errorCode.message() << "\n";
        return;
    }

    if (std::filesystem::exists(symlinkPath) || std::filesystem::is_symlink(symlinkPath))
    {
        std::filesystem::remove(symlinkPath, errorCode);
        if (errorCode)
        {
            std::cerr << "Error removing existing symlink during logger setup:  " << errorCode.message() << "\n";
        }
    }

    try
    {
        std::filesystem::create_symlink(relativeLogPath, symlinkPath);
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
        std::string logFileName = fmt::format("SystemTest_{:%Y-%m-%d_%H-%M-%S}_{:d}.log", now, pid);

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

SystestExecutorResult executeSystests(SystestConfiguration config)
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

        if (config.endlessMode)
        {
            runEndlessMode(queries, config);
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
        if (const auto grpcURI = config.grpcAddressUri.getValue(); not grpcURI.empty())
        {
            failedQueries = runQueriesAtRemoteWorker(queries, numberConcurrentQueries, grpcURI);
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
                failedQueries = Systest::runQueriesAndBenchmark(queries, singleNodeWorkerConfiguration, benchmarkResults);
                std::cout << benchmarkResults.dump(4);
                const auto outputPath = std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json";
                std::ofstream outputFile(outputPath);
                outputFile << benchmarkResults.dump(4);
                outputFile.close();
            }
            else
            {
                failedQueries = runQueriesAtLocalWorker(queries, numberConcurrentQueries, singleNodeWorkerConfiguration);
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
    CPPTRACE_CATCH(...)
    {
        tryLogCurrentException();
        const auto currentErrorCode = getCurrentErrorCode();
        return {
            .returnType = SystestExecutorResult::ReturnType::FAILED,
            .outputMessage = fmt::format("Failed with exception code: {}", currentErrorCode),
            .errorCode = currentErrorCode};
    }
    return {
        .returnType = SystestExecutorResult::ReturnType::FAILED,
        .outputMessage = "Fatal error, should never reach this point.",
        .errorCode = ErrorCode::UnknownException};
}
}
