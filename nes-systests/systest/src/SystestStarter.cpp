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

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <Configurations/Util.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestConfiguration.hpp>
#include <SystestExecutor.hpp>
#include <SystestState.hpp>
#include <Thread.hpp>

namespace
{
NES::SystestConfiguration parseConfiguration(int argc, const char** argv)
{
    using argparse::ArgumentParser;
    ArgumentParser program("systest");

    /// test discovery
    program.add_argument("-t", "--testLocation")
        .help("directly specified test file, e.g., fliter.test or a directory to discover test files in.  Use "
              "'path/to/testfile:testnumber' to run a specific test by testnumber within a file. Default: " TEST_DISCOVER_DIR);
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

    auto config = NES::SystestConfiguration();

    if (program.is_used("-b"))
    {
        config.benchmark = true;
        if ((program.is_used("-n") || program.is_used("--numberConcurrentQueries"))
            && (program.get<int>("--numberConcurrentQueries") > 1 || program.get<int>("-n") > 1))
        {
            NES_ERROR("Cannot run systest in Benchmarking mode with concurrency enabled!");
            std::cout << "Cannot run systest in benchmarking mode with concurrency enabled!\n";
            std::exit(-1); ///NOLINT(concurrency-mt-unsafe)
        }
        std::cout << "Running systests in benchmarking mode. Only one query is run at a time!\n";
        config.numberConcurrentQueries = 1;
    }

    if (program.is_used("-d"))
    {
        NES::Logger::setupLogging("systest.log", NES::LogLevel::LOG_DEBUG);
    }

    if (program.is_used("--data"))
    {
        config.testDataDir = program.get<std::string>("--data");
    }

    if (program.is_used("--log-path"))
    {
        config.logFilePath = program.get<std::string>("--log-path");
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
            /// The given path is neither a path to a directory nor a path to a file.
            /// We now check if this name belongs to a test file in nes-systests by searching through the nes-systests directory manually.
            const auto findAllInTree
                = [](const std::filesystem::path& wanted, const std::filesystem::path& root) -> std::vector<std::filesystem::path>
            {
                std::vector<std::filesystem::path> hits;
                for (const auto& entry :
                     std::filesystem::recursive_directory_iterator(root, std::filesystem::directory_options::skip_permission_denied))
                {
                    if (entry.is_regular_file() && entry.path().filename() == wanted)
                    {
                        hits.emplace_back(entry.path());
                    }
                }
                return hits;
            };

            const auto resolveTestArg
                = [&](const std::filesystem::path& arg, const std::filesystem::path& discoverRoot) -> std::vector<std::filesystem::path>
            {
                if (exists(arg))
                {
                    return {canonical(arg)};
                }
                return findAllInTree(arg.filename(), discoverRoot);
            };

            const std::filesystem::path discoverRoot = config.testsDiscoverDir.getValue();
            const auto matches = resolveTestArg(testFilePath, discoverRoot);

            switch (matches.size())
            {
                case 0:
                    std::cerr << '\'' << testFilePath << "' could not be located under '" << discoverRoot << "'.\n";
                    std::exit(EXIT_FAILURE);

                case 1:
                    config.directlySpecifiedTestFiles = matches.front();
                    break;

                default:
                    std::cerr << "Ambiguous test name '" << testFilePath << "':\n";
                    for (const auto& p : matches)
                    {
                        std::cerr << "  • " << p << '\n';
                    }
                    std::exit(EXIT_FAILURE); /// NOLINT(concurrency-mt-unsafe)
            }
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
        if (program.is_used("--sequential"))
        {
            NES_ERROR("Flags --shuffle and --sequential are mutually exclusive!")
            std::cout << "Flags --shuffle and --sequential are mutually exclusive!";
            std::exit(-1); ///NOLINT(concurrency-mt-unsafe)
        }
        config.randomQueryOrder = true;
    }

    if (program.is_used("-s"))
    {
        config.grpcAddressUri.setValue(NES::URI(program.get<std::string>("-s")));
    }

    if (program.is_used("-n"))
    {
        config.numberConcurrentQueries = program.get<int>("-n");
    }

    if (program.is_used("--sequential"))
    {
        config.numberConcurrentQueries = 1;
        config.sequentialQueryOrder = true;
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

        const int workerArgc = static_cast<int>(confVec.size()) + 1;
        std::vector<const char*> workerArgv;
        workerArgv.reserve(workerArgc + 1);
        workerArgv.push_back("systest"); /// dummy option as arg expects first arg to be the program name
        for (auto& arg : confVec)
        {
            workerArgv.push_back(const_cast<char*>(arg.c_str()));
        }

        config.singleNodeWorkerConfig = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(workerArgc, workerArgv.data());
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
        std::cout << NES::Systest::loadTestFileMap(config);
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }
    else if (program.is_used("--help"))
    {
        std::cout << program << '\n';
        std::exit(0); ///NOLINT(concurrency-mt-unsafe)
    }

    return config;
}
}

int main(int argc, const char** argv)
{
    auto startTime = std::chrono::high_resolution_clock::now();
    NES::Thread::initializeThread(NES::WorkerId("systest"), "main");

    auto config = parseConfiguration(argc, argv);
    NES::SystestExecutor executor(std::move(config));
    const auto result = executor.executeSystests();

    switch (result.returnType)
    {
        case SystestExecutorResult::ReturnType::SUCCESS: {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            std::cout << result.outputMessage << '\n';
            std::cout << "Total execution time: " << duration.count() << " ms ("
                      << std::chrono::duration_cast<std::chrono::seconds>(duration).count() << " seconds)" << '\n';
            return 0;
        }
        case SystestExecutorResult::ReturnType::FAILED: {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            PRECONDITION(result.errorCode, "Returning with as 'FAILED_WITH_EXCEPTION_CODE', but did not provide error code");
            NES_ERROR("{}", result.outputMessage);
            std::cout << result.outputMessage << '\n';
            std::cout << "Total execution time: " << duration.count() << " ms ("
                      << std::chrono::duration_cast<std::chrono::seconds>(duration).count() << " seconds)" << '\n';
            return result.errorCode.value();
        }
    }
}
