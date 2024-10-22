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

#include <fstream>
#include <future>
#include <regex>
#include <argparse/argparse.hpp>
#include <google/protobuf/text_format.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SystestConfiguration.hpp>
#include <SystestState.hpp>
#include <Configurations/Util.hpp>

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
    program.add_argument("-g", "--group").help("run a specific test group");

    /// list queries
    program.add_argument("-l", "--list").flag().help("list all discovered tests and test groups");

    /// debug mode
    program.add_argument("-d", "--debug").flag().help("dump the query plan and enable debug logging");

    /// configs
    program.add_argument("-w", "--workerConfig").help("load worker config file (.yaml)");
    program.add_argument("-q", "--queryCompilerConfig").help("load query compiler config file (.yaml)");

    /// result dir
    program.add_argument("--resultDir").help("change the result directory. Default: " PATH_TO_BINARY_DIR "/nes-systests/result/");

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
        .help("Arguments passed to the worker config, e.g., `-- --workerConfiguration.numberOfWorkerThreads=10`")
        .default_value(std::vector<std::string>{})
        .remaining();

    program.parse_args(argc, argv);

    auto config = Configuration::SystestConfiguration();

    if (program.is_used("-d"))
    {
        Logger::setupLogging("systest.log", LogLevel::LOG_DEBUG);
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
                        config.testNumbers.add(i);
                    }
                }
                else
                {
                    config.testNumbers.add(std::stoi(item));
                }
            }
        }
        else
        {
            testFilePath = testFileDefinition;
        }

        if (std::filesystem::is_directory(testFilePath))
        {
            config.testsDiscoverDir = testFilePath;
        }
        else if (std::filesystem::is_regular_file(testFilePath))
        {
            config.directlySpecifiedTestsFiles = testFilePath;
        }
        else
        {
            NES_FATAL_ERROR("{} is not a file or directory.", testFilePath);
            std::exit(1);
        }
    }

    if (program.is_used("-g"))
    {
        auto testMap = loadTestFileMap(config);

        auto expectedGroup = program.get<std::string>("-g");
        expectedGroup.erase(std::remove_if(expectedGroup.begin(), expectedGroup.end(), ::isspace), expectedGroup.end());

        auto found = std::any_of(
            testMap.begin(),
            testMap.end(),
            [&expectedGroup](const auto& testfilePair)
            {
                const auto& testfile = testfilePair.second;
                const auto& groups = testfile.groups;
                return std::any_of(groups.begin(), groups.end(), [&expectedGroup](const auto& group) { return group == expectedGroup; });
            });
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
            NES_FATAL_ERROR("{} is not a file.", config.workerConfig.getValue());
            std::exit(1);
        }
    }

    if (program.is_used("-q"))
    {
        config.queryCompilerConfig = program.get<std::string>("-q");
        if (not std::filesystem::is_regular_file(config.queryCompilerConfig.getValue()))
        {
            NES_FATAL_ERROR("{} is not a file.", config.queryCompilerConfig.getValue());
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
                = NES::Configurations::loadConfiguration<NES::Configuration::SingleNodeWorkerConfiguration>(argc_, argv_.data());
        }
        catch (const std::exception& e)
        {
            tryLogCurrentException();
        }
    }

    if (program.is_used("--resultDir"))
    {
        config.resultDir = program.get<std::string>("--resultDir");
        if (not std::filesystem::is_directory(config.resultDir.getValue()))
        {
            NES_FATAL_ERROR("{} is not a directory.", config.resultDir.getValue());
            std::exit(1);
        }
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

void removeFilesInDirectory(const std::filesystem::path& dirPath, const std::string& extension)
{
    if (is_directory(dirPath))
    {
        for (const auto& entry : std::filesystem::directory_iterator(dirPath))
        {
            if (entry.is_regular_file() && entry.path().extension() == extension)
            {
                std::filesystem::remove(entry.path());
            }
        }
    }
    else
    {
        std::cerr << "The specified path is not a directory.\n";
    }
}

void removeFilesInDirectory(const std::filesystem::path& dirPath)
{
    if (is_directory(dirPath))
    {
        for (const auto& entry : std::filesystem::directory_iterator(dirPath))
        {
            if (entry.is_regular_file())
            {
                std::filesystem::remove(entry.path());
            }
        }
    }
    else
    {
        std::cerr << "The specified path is not a directory.\n";
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
    std::time_t now_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char timestamp[20]; /// Buffer large enough for "YYYY-MM-DD_HH-MM-SS"
    std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d_%H-%M-%S", std::localtime(&now_time_t));

    const auto logFileName = fmt::format(PATH_TO_BINARY_DIR "/nes-systests/SystemTest_{}.log", timestamp);

    NES::Logger::setupLogging(logFileName, NES::LogLevel::LOG_DEBUG, false);
    const std::filesystem::path logPath = std::filesystem::current_path() / logFileName;
    /// file:// to make the link clickable in the console
    std::cout << "Find the log at: file://" << logPath.string() << std::endl;
}

int main(int argc, const char** argv)
{
    using namespace NES;
    int returnCode = 0;

    try
    {
        setupLogging();

        /// Read the configuration
        auto config = Systest::readConfiguration(argc, argv);

        auto testMap = Systest::loadTestFileMap(config);
        auto queries = loadQueries(std::move(testMap), config.resultDir.getValue());

        std::filesystem::remove_all(config.resultDir.getValue());
        std::filesystem::create_directory(config.resultDir.getValue());

        if (config.randomQueryOrder)
        {
            shuffleQueries(queries);
        }

        auto numberConcurrentQueries = config.numberConcurrentQueries.getValue();
        auto grpcURI = config.grpcAddressUri.getValue();
        if (not grpcURI.empty())
        {
            returnCode = runQueriesAtRemoteWorker(queries, numberConcurrentQueries, grpcURI) ? 0 : 1;
        }
        else
        {
            std::unique_ptr<Configuration::SingleNodeWorkerConfiguration> singleNodeWorkerConfiguration;

            if (config.singleNodeWorkerConfig.has_value())
            {
                singleNodeWorkerConfiguration
                    = std::make_unique<Configuration::SingleNodeWorkerConfiguration>(config.singleNodeWorkerConfig.value());
            }
            else
            {
                singleNodeWorkerConfiguration = std::make_unique<Configuration::SingleNodeWorkerConfiguration>();
            }

            if (!config.workerConfig.getValue().empty())
            {
                singleNodeWorkerConfiguration->workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig);
            }

            if (!config.queryCompilerConfig.getValue().empty())
            {
                singleNodeWorkerConfiguration->queryCompilerConfiguration.overwriteConfigWithYAMLFileInput(config.queryCompilerConfig);
            }

            returnCode = runQueriesAtLocalWorker(queries, numberConcurrentQueries, *singleNodeWorkerConfiguration) ? 0 : 1;
        }
    }
    catch (...)
    {
        tryLogCurrentException();
        std::exit(1);
    }
    return returnCode;
}
