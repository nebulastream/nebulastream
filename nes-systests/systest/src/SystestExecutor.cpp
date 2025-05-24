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
#include <SystestConfiguration.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>
#include <from_current.hpp>


using namespace std::literals;

namespace NES
{
Configuration::SystestConfiguration readConfiguration(int argc, const char** argv)
{
    using argparse::ArgumentParser;
    ArgumentParser program("systest");

    /// test discovery
    program.add_argument("-t", "--testLocation")
        .help(
            "directly specified test file, e.g., fliter.test or a directory to discover test files in.  Use "
            "'path/to/testfile:testnumber' to run a specific test by testnumber within a file. Default: " TEST_DISCOVER_DIR);
    program.add_argument("-g", "--groups").help("run a specific test groups").nargs(argparse::nargs_pattern::at_least_one);
    program.add_argument("-e", "--exclude-groups")
        .help("ignore groups, takes precedence over -g")
        .nargs(argparse::nargs_pattern::at_least_one);

    /// list queries
    program.add_argument("-l", "--list").flag().help("list all discovered tests and test groups");

    /// debug mode
    program.add_argument("-d", "--debug").flag().help("dump the query plan and enable debug logging");

    /// input data
    program.add_argument("--data").help("path to the directory where input CSV files are stored");

    /// configs
    program.add_argument("-w", "--workerConfig").help("load worker config file (.yaml)");
    program.add_argument("-q", "--queryCompilerConfig").help("load query compiler config file (.yaml)");

    /// result dir
    program.add_argument("--workingDir")
        .help(
            "change the working directory. This directory contains source and result files. Default: " PATH_TO_BINARY_DIR "/nes-systests/");

    /// server/remote mode
    program.add_argument("-s", "--server").help("grpc uri, e.g., 127.0.0.1:8080, if not specified local single-node-worker is used.");

    /// test query order
    program.add_argument("--shuffle").flag().help("run queries in random order");
    program.add_argument("-n", "--numberConcurrentQueries")
        .help("number of concurrent queries. Default: 6")
        .default_value(6)
        .scan<'i', int>();
    program.add_argument("--sequential").flag().help("force sequential query execution. Equivalent to `-n 1`");

    /// endless mode
    program.add_argument("--endless").flag().help("continuously issue queries to the worker");

    /// single node worker config
    program.add_argument("--")
        .help("arguments passed to the worker config, e.g., `-- --worker.queryEngine.numberOfWorkerThreads=10`")
        .default_value(std::vector<std::string>{})
        .remaining();

    /// Benchmark (time) all specified queries
    program.add_argument("-b")
        .help("Benchmark (time) all specified queries and store results into 'BenchmarkResults.json' in the result directory")
        .default_value(false)
        .implicit_value(true);

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err)
    {
        std::cerr << "Error parsing arguments: " << err.what() << '\n';
        std::cerr << program << '\n';
        std::exit(1); ///NOLINT(concurrency-mt-unsafe)
    }
    catch (const std::exception& err)
    {
        std::cerr << "Unexpected error during argument parsing: " << err.what() << '\n';
        std::exit(1); ///NOLINT(concurrency-mt-unsafe)
    }

    auto config = Configuration::SystestConfiguration();

    if (program.is_used("-b"))
    {
        config.benchmark = true;
        if ((program.is_used("-n") || program.is_used("--numberConcurrentQueries"))
            && (program.get<int>("--numberConcurrentQueries") > 1 || program.get<int>("-n") > 1))
        {
            NES_FATAL_ERROR("Cannot run systest in Benchmarking mode with concurrency enabled!");
            std::cout << "Cannot run systest in benchmarking mode with concurrency enabled!\n";
            exit(-1); ///NOLINT(concurrency-mt-unsafe)
        }
        std::cout << "Running systests in benchmarking mode. Only one query is run at a time!\n";
        config.numberConcurrentQueries = 1;
    }

    if (program.is_used("-d"))
    {
        Logger::setupLogging("systest.log", LogLevel::LOG_DEBUG);
    }

    if (program.is_used("--data"))
    {
        config.testDataDir = program.get<std::string>("--data");
    }

    if (program.is_used("--testLocation"))
    {
        auto testFileDefinition = program.get<std::string>("--testLocation");
        std::string testFilePath;
        /// Check for test numbers (e.g., "testfile.test:5")
        const size_t delimiterPos = testFileDefinition.find(':');
        if (delimiterPos != std::string::npos)
        {
            testFilePath = testFileDefinition.substr(0, delimiterPos);
            const std::string testNumberStr = testFileDefinition.substr(delimiterPos + 1);

            std::stringstream ss(testNumberStr);
            std::string item;
            /// handle sequences (e.g., "1,2")
            while (std::getline(ss, item, ','))
            {
                const size_t dashPos = item.find('-');
                if (dashPos != std::string::npos)
                {
                    /// handle ranges (e.g., "3-5")
                    const int start = std::stoi(item.substr(0, dashPos));
                    const int end = std::stoi(item.substr(dashPos + 1));
                    for (int i = start; i <= end; ++i)
                    {
                        config.testQueryNumbers.add(i);
                    }
                }
                else
                {
                    config.testQueryNumbers.add(std::stoi(item));
                }
            }
        }
        else
        {
            testFilePath = std::filesystem::path(testFileDefinition);
        }

        if (std::filesystem::is_directory(testFilePath))
        {
            config.testsDiscoverDir = testFilePath;
        }
        else if (std::filesystem::is_regular_file(testFilePath))
        {
            config.directlySpecifiedTestFiles = testFilePath;
        }
        else
        {
            std::cerr << testFilePath << " is not a file or directory.\n";
            std::exit(1); ///NOLINT(concurrency-mt-unsafe)
        }
    }

    if (program.is_used("-g"))
    {
        auto expectedGroups = program.get<std::vector<std::string>>("-g");
        for (const auto& expectedGroup : expectedGroups)
        {
            config.testGroups.add(expectedGroup);
        }
    }

    if (program.is_used("--exclude-groups"))
    {
        auto excludedGroups = program.get<std::vector<std::string>>("--exclude-groups");
        for (const auto& excludedGroup : excludedGroups)
        {
            config.excludeGroups.add(excludedGroup);
        }
    }

    if (program.is_used("--shuffle"))
    {
        config.randomQueryOrder = true;
    }

    if (program.is_used("-s"))
    {
        config.grpcAddressUri = program.get<std::string>("-s");
    }

    if (program.is_used("-n"))
    {
        config.numberConcurrentQueries = program.get<int>("-n");
    }

    if (program.is_used("--sequential"))
    {
        config.numberConcurrentQueries = 1;
    }

    if (program.is_used("-w"))
    {
        config.workerConfig = program.get<std::string>("-w");
        if (not std::filesystem::is_regular_file(config.workerConfig.getValue()))
        {
            std::cerr << config.workerConfig.getValue() << " is not a file.\n";
            std::exit(1); ///NOLINT(concurrency-mt-unsafe)
        }
    }

    if (program.is_used("-q"))
    {
        config.queryCompilerConfig = program.get<std::string>("-q");
        if (not std::filesystem::is_regular_file(config.queryCompilerConfig.getValue()))
        {
            std::cerr << config.queryCompilerConfig.getValue() << " is not a file.\n";
            std::exit(1); ///NOLINT(concurrency-mt-unsafe)
        }
    }

    if (program.is_used("--"))
    {
        auto confVec = program.get<std::vector<std::string>>("--");

        const int argc = confVec.size() + 1;
        std::vector<const char*> argv;
        argv.reserve(argc + 1);
        argv.push_back("systest"); /// dummy option as arg expects first arg to be the program name
        for (auto& arg : confVec)
        {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }

        config.singleNodeWorkerConfig
            = NES::Configurations::loadConfiguration<Configuration::SingleNodeWorkerConfiguration>(argc, argv.data());
    }

    /// Setup Working Directory
    if (program.is_used("--workingDir"))
    {
        config.workingDir = program.get<std::string>("--workingDir");
        config.singleNodeWorkerConfig.value().workerConfiguration.queryOptimizer.fileBackedWorkingDir = config.workingDir;
    }

    if (program.is_used("--endless"))
    {
        config.endlessMode = true;
    }

    if (program.is_used("--list"))
    {
        std::cout << Systest::loadTestFileMap(config);
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }
    else if (program.is_used("--help"))
    {
        std::cout << program << '\n';
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }
    return config;
}

void runEndlessMode(
    std::vector<Systest::SystestQuery> queries, Configuration::SystestConfiguration& config, const Systest::QueryResultMap& queryResultMap)
{
    std::cout << std::format("Running endlessly over a total of {} queries.", queries.size()) << '\n';

    const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();

    auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(Configuration::SingleNodeWorkerConfiguration{});
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

    auto submitter = [&]() -> std::unique_ptr<Systest::QuerySubmitter>
    {
        if (runRemote)
        {
            return std::make_unique<Systest::RemoteWorkerQuerySubmitter>(grpcURI);
        }
        return std::make_unique<Systest::LocalWorkerQuerySubmitter>(singleNodeWorkerConfiguration);
    }();

    while (true)
    {
        std::ranges::shuffle(queries, rng);
        const auto failedQueries = runQueries(queries, numberConcurrentQueries, *submitter, queryResultMap);
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

void setupLogging()
{
    const std::filesystem::path logDir = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests";

    std::error_code errorCode;
    std::filesystem::create_directories(logDir, errorCode);
    if (errorCode)
    {
        std::cerr << "Error creating log directory during logger setup: " << errorCode.message() << "\n";
        return;
    }

    const auto now = std::chrono::system_clock::now();
    const auto pid = ::getpid();
    const auto logFileName = fmt::format("SystemTest_{:%Y-%m-%d_%H-%M-%S}_{:d}.log", now, pid);

    const auto absoluteLogPath = logDir / logFileName;
    NES::Logger::setupLogging(absoluteLogPath.string(), NES::LogLevel::LOG_DEBUG, false);
    if (const char* hostLoggingPath = std::getenv("HOST_LOGGING_PATH"))
    {
        /// Set the correct logging path when using docker
        fmt::println(std::cout, "Find the log at: file://{}/nes-systests/{}", std::filesystem::path(hostLoggingPath).string(), logFileName);
    }
    else
    {
        /// Set the correct logging path without docker
        fmt::println(std::cout, "Find the log at: file://{}/nes-systests/{}", PATH_TO_BINARY_DIR, logFileName);
    }

    const auto symlinkPath = logDir / "latest.log";
    createSymlink(absoluteLogPath, symlinkPath);
}

SystestExecutorResult executeSystests(Configuration::SystestConfiguration config)
{
    setupLogging();

    CPPTRACE_TRY
    {
        /// Read the configuration
        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());

        Systest::SystestStarterGlobals systestStarterGlobals{
            config.workingDir.getValue(), config.testDataDir.getValue(), config.configDir.getValue(), Systest::loadTestFileMap(config)};
        auto queries = loadQueries(systestStarterGlobals);
        if (queries.empty())
        {
            std::stringstream outputMessage;
            outputMessage << "No queries were run.";
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = outputMessage.str(),
                .errorCode = ErrorCode::TestException};
        }

        if (config.endlessMode)
        {
            runEndlessMode(queries, config, systestStarterGlobals.getQueryResultMap());
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
            failedQueries = runQueriesAtRemoteWorker(queries, numberConcurrentQueries, grpcURI, systestStarterGlobals.getQueryResultMap());
        }
        else
        {
            auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(Configuration::SingleNodeWorkerConfiguration{});
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
                failedQueries = Systest::runQueriesAndBenchmark(
                    queries, singleNodeWorkerConfiguration, benchmarkResults, systestStarterGlobals.getQueryResultMap());
                std::cout << benchmarkResults.dump(4);
                const auto outputPath = std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json";
                std::ofstream outputFile(outputPath);
                outputFile << benchmarkResults.dump(4);
                outputFile.close();
            }
            else
            {
                failedQueries = runQueriesAtLocalWorker(
                    queries, numberConcurrentQueries, singleNodeWorkerConfiguration, systestStarterGlobals.getQueryResultMap());
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
