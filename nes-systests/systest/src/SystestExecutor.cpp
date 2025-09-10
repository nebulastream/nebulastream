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
#include <from_current.hpp>

#include <argparse/argparse.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)

#include <Configurations/Util.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <SystestConfiguration.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

using namespace std::literals;

/// Enables in-memory data channel for network sources and sink. Which is required to to the embedded deployment.
extern void enable_memcom();

namespace NES
{

SystestConfiguration readConfiguration(int argc, const char** argv)
{
    using argparse::ArgumentParser;
    ArgumentParser program("systest");

    /// test discovery
    program.add_argument("-t", "--testLocation")
        .help("directly specified test file, e.g., filter.test or a directory to discover test files in. Use "
              "'path/to/testFile:testNumber' to run a specific test by test number within a file. Default: " TEST_DISCOVER_DIR);

    program.add_argument("-c", "--configurationLocation")
        .help("Path to configuration folder. The configuration folder contains inputFormatter, sources, topology configuration and "
              "defaults to: " DEFAULT_TEST_CONFIGURATION_DIR);

    program.add_argument("-g", "--groups").help("run a specific test groups").nargs(argparse::nargs_pattern::at_least_one);
    program.add_argument("-e", "--exclude-groups")
        .help("ignore groups, takes precedence over -g")
        .nargs(argparse::nargs_pattern::at_least_one);

    /// list queries
    program.add_argument("-l", "--list").flag().help("list all discovered tests and test groups");

    /// log path
    program.add_argument("--log-path").help("set the logging path");

    /// debug mode
    program.add_argument("-d", "--debug").flag().help("dump the query plan and enable debug logging");

    /// input data
    program.add_argument("--data").help("path to the directory where input CSV files are stored");

    /// configs
    program.add_argument("-w", "--workerConfig").help("load worker config file (.yaml)");
    program.add_argument("-q", "--queryCompilerConfig").help("load query compiler config file (.yaml)");

    /// result dir
    program.add_argument("--workingDir")
        .help("change the working directory. This directory contains source and result files. Default: " PATH_TO_BINARY_DIR
              "/nes-systests/");

    /// distributed mode, otherwise fall back to local (dummy topology, will lead to single-node execution
    program.add_argument("--topology")
        .help("path to a topology file. If none is provided the default topology at \"configs/topologies/default_distributed.yaml\" will "
              "be picked")
        .nargs(1);

    program.add_argument("--remote")
        .help("use a remote instance(s) of nebulastream to run the system test queries. By default the worker will be started in-process "
              "and communicate via an in-memory channel")
        .flag();

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
        .flag();

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

    auto config = SystestConfiguration();

    if (program.is_used("-b"))
    {
        config.benchmark = true;
        if ((program.is_used("-n") || program.is_used("--numberConcurrentQueries"))
            && (program.get<int>("--numberConcurrentQueries") > 1 || program.get<int>("-n") > 1))
        {
            NES_ERROR("Cannot run systest in Benchmarking mode with concurrency enabled");
            std::cout << "Cannot run systest in benchmarking mode with concurrency enabled\n";
            exit(-1); ///NOLINT(concurrency-mt-unsafe)
        }
        std::cout << "Running systests in benchmarking mode, a query at a time\n";
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

    if (program.is_used("--log-path"))
    {
        config.logFilePath = program.get<std::string>("--log-path");
    }

    if (program.is_used("--configurationLocation"))
    {
        config.configDir = program.get<std::string>("--configurationLocation");
    }

    if (program.is_used("--testLocation"))
    {
        auto testFileDefinition = program.get<std::string>("--testLocation");
        bool hasQueryNumber = false;
        std::string testFilePath;
        /// Check for test numbers (e.g., "testfile.test:5")
        const size_t delimiterPos = testFileDefinition.find(':');
        if (delimiterPos != std::string::npos)
        {
            hasQueryNumber = true;
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
                    if (auto testNumber = Util::from_chars<uint64_t>(item))
                    {
                        config.testQueryNumbers.add(*testNumber);
                    }
                    else
                    {
                        fmt::println(std::cerr, "Error parsing test number: {}", item);
                        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
                    }
                }
            }
        }
        else
        {
            testFilePath = std::filesystem::path(testFileDefinition);
        }

        if (!std::filesystem::exists(testFilePath))
        {
            fmt::println(std::cerr, "Test file/directory does not exist: {}", testFileDefinition);
            std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
        }
        if (std::filesystem::is_directory(testFilePath) && hasQueryNumber)
        {
            fmt::println(std::cerr, "'{}' is not a test file", testFilePath);
            std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
        }

        if (std::filesystem::is_directory(testFilePath))
        {
            config.testsDiscoverDir = std::filesystem::absolute(testFilePath);
        }
        else if (std::filesystem::is_regular_file(testFilePath))
        {
            config.directlySpecifiedTestFiles = testFilePath;
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

    if (auto topology = program.present<std::string>("--topology"))
    {
        config.topologyPath = *topology;
    }

    if (program.is_used("--remote"))
    {
        config.remote = true;
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
            argv.push_back(arg.c_str());
        }

        config.singleNodeWorkerConfig = loadConfiguration<SingleNodeWorkerConfiguration>(argc, argv.data());
    }

    /// Setup Working Directory
    if (program.is_used("--workingDir"))
    {
        config.workingDir = program.get<std::string>("--workingDir");
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

CLI::ClusterConfig loadTopology(const SystestConfiguration& config)
{
    /// 1. topologyPath is a full path to a topology file.
    /// 2. topolgoyPath is just a name in the default config dir
    /// 3. pick default topology
    auto paths = std::to_array<std::filesystem::path>(
        {std::filesystem::path(config.topologyPath.getValue()),
         std::filesystem::path(config.configDir.getValue()) / "topologies" / config.topologyPath.getValue(),
         std::filesystem::path(config.configDir.getValue()) / "topologies" / "default_distributed.yaml"});

    auto it = std::ranges::find_if(
        paths, [](const auto& path) { return std::filesystem::exists(path) && std::filesystem::is_regular_file(path); });
    if (it == paths.end())
    {
        throw TestException("Could not find topology file in: {}", fmt::join(paths, ", "));
    }

    try
    {
        const auto topology = YAML::LoadFile(*it).as<CLI::ClusterConfig>();
        fmt::println("Loading topology from: {}", *it);
        return topology;
    }
    catch (const std::exception& e)
    {
        throw TestException("{} contains an invalid topology: {}", *it, e.what());
    }
}

SystestExecutorResult executeSystests(SystestConfiguration config)
{
    setupLogging(config);

    CPPTRACE_TRY
    {
        /// Read the configuration
        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());
        auto topology = loadTopology(config);

        auto discoveredTestFiles = Systest::loadTestFileMap(config);
        Systest::SystestBinder binder{config.workingDir.getValue(), config.testDataDir.getValue(), config.configDir.getValue(), topology};
        auto [queries, loadedFiles] = binder.loadPlanQueries(discoveredTestFiles);
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

        const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
        Systest::SystestRunner::ExecutionMode executionMode = [&]() -> Systest::SystestRunner::ExecutionMode
        {
            if (config.remote)
            {
                return Systest::SystestRunner::DistributedExecution{topology};
            }

            /// Enable the in-memory communcation mechansim of the embedded node engine
            enable_memcom();

            auto singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{});
            if (not config.workerConfig.getValue().empty())
            {
                singleNodeWorkerConfiguration.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
            }
            else if (config.singleNodeWorkerConfig.has_value())
            {
                singleNodeWorkerConfiguration = config.singleNodeWorkerConfig.value();
            }

            return Systest::SystestRunner::LocalExecution{topology, singleNodeWorkerConfiguration};
        }();

        const Systest::SystestRunner::RunnerConfig runnerConfig{
            .endless = config.endlessMode, .shuffle = config.randomQueryOrder, .queryConcurrency = config.numberConcurrentQueries};

        std::vector<Systest::FailedQuery> failedQueries;
        if (config.benchmark)
        {
            nlohmann::json benchmarkResults;
            failedQueries = Systest::SystestRunner::from(queries, executionMode, runnerConfig).run(&benchmarkResults);
            std::cout << benchmarkResults.dump(4) << '\n';
            const auto outputPath = std::filesystem::path(config.workingDir.getValue()) / "BenchmarkResults.json";
            std::ofstream outputFile(outputPath);
            outputFile << benchmarkResults.dump(4) << '\n';
            outputFile.close();
        }
        else
        {
            failedQueries = Systest::SystestRunner::from(queries, executionMode, runnerConfig).run(nullptr);
        }


        if (not failedQueries.empty())
        {
            std::stringstream outputMessage;
            outputMessage << fmt::format("The following queries failed:\n[Name, Command]\n- {}", fmt::join(failedQueries, "\n- "));
            return {
                .returnType = SystestExecutorResult::ReturnType::FAILED,
                .outputMessage = outputMessage.str(),
                .errorCode = ErrorCode::QueryStatusFailed,
                .numFailedQueries = failedQueries.size()};
        }
        std::stringstream outputMessage;
        outputMessage << '\n' << "All queries passed.";
        return {
            .returnType = SystestExecutorResult::ReturnType::SUCCESS,
            .outputMessage = outputMessage.str(),
            .numFailedQueries = failedQueries.size()};
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
