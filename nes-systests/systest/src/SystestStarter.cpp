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

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <format>
#include <iostream>
#include <utility>
#include <vector>
#include <Configurations/Util.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/MPMCQueue.h>
#include <google/protobuf/text_format.h>
#include <ErrorHandling.hpp>
#include <SystestConfiguration.hpp>
#include <SystestState.hpp>

using namespace std::literals;

namespace NES::Systest
{
Configuration::SystestConfiguration readConfiguration(int argc, const char** argv)
{
    using namespace argparse;
    ArgumentParser program("systest");

    /// test discovery
    program.add_argument("-t", "--testLocation")
        .help("directly specified test file, e.g., fliter.test or a directory to discover test files in.  Use "
              "'path/to/testfile:testnumber' to run a specific test by testnumber within a file. Default: " TEST_DISCOVER_DIR);
    program.add_argument("-g", "--groups").help("run a specific test groups").nargs(nargs_pattern::at_least_one);
    program.add_argument("-e", "--exclude-groups").help("ignore groups, takes precedence over -g").nargs(nargs_pattern::at_least_one);

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

    /// single node worker config
    program.add_argument("--")
        .help("arguments passed to the worker config, e.g., `-- --workerConfiguration.numberOfWorkerThreads=10`")
        .default_value(std::vector<std::string>{})
        .remaining();

    program.parse_args(argc, argv);

    auto config = Configuration::SystestConfiguration();

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
        size_t delimiterPos = testFileDefinition.find(':');
        if (delimiterPos != std::string::npos)
        {
            testFilePath = testFileDefinition.substr(0, delimiterPos);
            std::string testNumberStr = testFileDefinition.substr(delimiterPos + 1);

            std::stringstream ss(testNumberStr);
            std::string item;
            /// handle sequences (e.g., "1,2")
            while (std::getline(ss, item, ','))
            {
                size_t dashPos = item.find('-');
                if (dashPos != std::string::npos)
                {
                    /// handle ranges (e.g., "3-5")
                    int start = std::stoi(item.substr(0, dashPos));
                    int end = std::stoi(item.substr(dashPos + 1));
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
            std::exit(1);
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
            std::exit(1);
        }
    }

    if (program.is_used("-q"))
    {
        config.queryCompilerConfig = program.get<std::string>("-q");
        if (not std::filesystem::is_regular_file(config.queryCompilerConfig.getValue()))
        {
            std::cerr << config.queryCompilerConfig.getValue() << " is not a file.\n";
            std::exit(1);
        }
    }

    if (program.is_used("--"))
    {
        auto confVec = program.get<std::vector<std::string>>("--");

        int argc_ = confVec.size() + 1;
        std::vector<const char*> argv_;
        argv_.reserve(argc_ + 1);
        argv_.push_back("systest"); /// dummy option as arg expects first arg to be the program name
        for (auto& arg : confVec)
        {
            argv_.push_back(const_cast<char*>(arg.c_str()));
        }

        try
        {
            config.singleNodeWorkerConfig
                = NES::Configurations::loadConfiguration<Configuration::SingleNodeWorkerConfiguration>(argc_, argv_.data());
        }
        catch (const std::exception& e)
        {
            tryLogCurrentException();
            std::exit(1);
        }
    }

    /// Setup Working Directory
    if (program.is_used("--workingDir"))
    {
        config.workingDir = program.get<std::string>("--workingDir");
    }

    if (program.is_used("--list"))
    {
        std::cout << loadTestFileMap(config);
        std::exit(0);
    }
    else if (program.is_used("--help"))
    {
        std::cout << program << std::endl;
        std::exit(0);
    }
    return config;
}
}

void shuffleQueries(std::vector<NES::Systest::Query> queries)
{
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(queries.begin(), queries.end(), g);
}

void setupLogging()
{
    const auto now = std::chrono::system_clock::now();
    const auto pid = ::getpid();
    const auto logFileName = fmt::format("/nes-systests/SystemTest_{:%H:%M:%S}_{}.log", now, pid);
    NES::Logger::setupLogging(fmt::format("{}{}", PATH_TO_BINARY_DIR, logFileName), NES::LogLevel::LOG_TRACE, false);

    if (const char* hostLoggingPath = std::getenv("HOST_LOGGING_PATH"))
    {
        /// Set the correct logging path when using docker
        std::cout << "Find the log at: file://" << std::filesystem::path(hostLoggingPath).string() + logFileName << std::endl;
    }
    else
    {
        /// Set the correct logging path without docker
        std::cout << "Find the log at: file://" << PATH_TO_BINARY_DIR + logFileName << std::endl;
    }
}

int main(int argc, const char** argv)
{
    using namespace NES;
    int returnCode = 0;

    setupLogging();

    try
    {
        /// Read the configuration
        auto config = Systest::readConfiguration(argc, argv);
        std::filesystem::remove_all(config.workingDir.getValue());
        std::filesystem::create_directory(config.workingDir.getValue());

        auto testMap = Systest::loadTestFileMap(config);
        const auto queries = loadQueries(testMap, config.workingDir.getValue(), config.testDataDir.getValue());
        std::cout << std::format("Running a total of {} queries.", queries.size()) << '\n';
        if (queries.empty())
        {
            std::stringstream outputMessage;
            outputMessage << "No queries were run.";
            NES_ERROR("{}", outputMessage.str());
            std::cout << outputMessage.str() << '\n';
            std::exit(1);
        }


        if (config.randomQueryOrder)
        {
            shuffleQueries(queries);
        }

        const auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
        std::vector<Systest::RunningQuery> failedQueries;
        if (const auto grpcURI = config.grpcAddressUri.getValue(); not grpcURI.empty())
        {
            failedQueries = runQueriesAtRemoteWorker(queries, numberConcurrentQueries, grpcURI);
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

            failedQueries = runQueriesAtLocalWorker(queries, numberConcurrentQueries, singleNodeWorkerConfiguration);
        }

        if (failedQueries.empty())
        {
            std::stringstream outputMessage;
            outputMessage << std::endl << "All queries passed.";
            NES_INFO("{}", outputMessage.str());
            std::cout << outputMessage.str() << std::endl;
            std::exit(0);
        }
        else
        {
            std::stringstream outputMessage;
            outputMessage << fmt::format("The following queries failed:\n[Name, Command]\n- {}", fmt::join(failedQueries, "\n- "));
            NES_ERROR("{}", outputMessage.str());
            std::cout << std::endl << outputMessage.str() << std::endl;
            std::exit(1);
        }
    }
    catch (...)
    {
        tryLogCurrentException();
        std::exit(1);
    }
    return returnCode;
}
