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
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <argparse/argparse.hpp>
#include <folly/MPMCQueue.h>
#include <google/protobuf/text_format.h>
#include <grpc++/create_channel.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SystestConfiguration.hpp>
#include <SystestGrpc.hpp>
#include <SystestHelper.hpp>
#include <SystestRunner.hpp>

using namespace std::literals;

namespace NES::Systest
{

enum class Command : uint8_t
{
    run,
    cache
};

std::tuple<Configuration::SystestConfiguration, Command> readConfiguration(int argc, const char** argv)
{
    using namespace argparse;
    ArgumentParser program("systest");

    program.add_argument("-t", "--testLocation")
        .help("directly specified test file, e.g., fliter.test or a directory to discover test files in.  Use "
              "'path/to/testfile:testnumber' to run a specific test by testnumber within a file. Default: " TEST_DISCOVER_DIR);
    program.add_argument("-g", "--group").help("run a specific test group");

    program.add_argument("-l", "--list").flag().help("list all discovered tests and test groups");

    program.add_argument("-d", "--debug").flag().help("dump the query plan and enable debug logging");

    program.add_argument("-c", "--generateCache").flag().help("do not run the tests directly but cache them in the cache dir: " CACHE_DIR);
    program.add_argument("--cacheDir").help("change the cache directory. Default: " CACHE_DIR);
    program.add_argument("--useCache").flag().help("use cached queries");

    program.add_argument("-w", "--workerConfig").help("load worker config file (.yaml)");
    program.add_argument("-q", "--queryCompilerConfig").help("load query compiler config file (.yaml)");

    program.add_argument("--resultDir").help("change the result directory. Default: " PATH_TO_BINARY_DIR "/tests/result/");

    program.add_argument("-s", "--server").help("grpc uri, e.g., 127.0.0.1:8080, if not specified local single-node-worker is used.");

    program.add_argument("--shuffle").flag().help("run queries in random order");
    program.add_argument("-n", "--numberConcurrentQueries")
        .help("number of concurrent queries. Default: 6")
        .default_value(6)
        .scan<'i', int>();
    program.add_argument("--sequential").flag().help("force sequential query execution. Equivalent to `-n 1`");

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
            // handle sequences (e.g., "1,2")
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
        auto testFiles = discoverTestFiles(config);
        auto testGroups = getTestGroups(testFiles);
        auto group = program.get<std::string>("-g");
        group.erase(std::remove_if(group.begin(), group.end(), ::isspace), group.end());

        auto found = std::any_of(testGroups.begin(), testGroups.end(), [&](const TestGroup& tg) {
            return tg.name == group;
        });
        if (!found)
        {
            NES_FATAL_ERROR("Could not find tests in group {}.", group);
            std::exit(1);
        }
        config.testGroup = group;
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

    if (program.is_used("--cacheDir"))
    {
        config.cacheDir = program.get<std::string>("--cacheDir");
        if (not std::filesystem::is_directory(config.cacheDir.getValue()))
        {
            NES_FATAL_ERROR("{} is not a directory.", config.cacheDir.getValue());
            std::exit(1);
        }
    }

    if (program.is_used("--useCache"))
    {
        config.useCachedQueries = true;
        std::cout << "use cached queries provided in: " << config.cacheDir.getValue() << "\n";
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

    if (program.is_used("--resultDir"))
    {
        config.resultDir = program.get<std::string>("--resultDir");
        if (not std::filesystem::is_directory(config.resultDir.getValue()))
        {
            NES_FATAL_ERROR("{} is not a directory.", config.resultDir.getValue());
            std::exit(1);
        }
    }

    Command com;
    if (program.is_used("--list"))
    {
        printTestList(config);
        std::exit(0);
    }
    else if (program.is_used("--help"))
    {
        std::cout << program << std::endl;
        std::exit(0);
    }
    else if (program.is_used("--generateCache"))
    {
        com = Command::cache;
    }
    else
    {
        com = Command::run;
    }
    return {config, com};
}
}


int main(int argc, const char** argv)
{
    using namespace NES;

    try
    {
        Logger::setupLogging("systest.log", LogLevel::LOG_ERROR);

        auto [config, com] = Systest::readConfiguration(argc, argv);

        DecomposedQueryPlanPtr decomposedQueryPlan;

        if (com == Systest::Command::run)
        {
            auto resultDir = std::filesystem::path(config.resultDir.getValue());
            Systest::removeFilesInDirectory(resultDir);

            std::vector<Systest::Query> queries = Systest::collectQueriesToTest(config);

            if (config.randomQueryOrder)
            {
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(queries.begin(), queries.end(), g);
            }

            auto capacity = config.numberConcurrentQueries.getValue();
            folly::MPMCQueue<Systest::Query> runningQueries(capacity);

            if (not config.grpcAddressUri.getValue().empty()) /// case: send requests over grpc to different single-node-worker
            {
                auto serverUri = config.grpcAddressUri.getValue();
                GRPCClient client(grpc::CreateChannel(serverUri, grpc::InsecureChannelCredentials()));

                std::atomic<size_t> runningQueryCount{0};
                std::atomic finishedProducing{false};

                std::thread producer(
                    [&]()
                    {
                        for (auto& query : queries)
                        {
                            while (runningQueryCount.load() >= capacity)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                            }

                            SerializableDecomposedQueryPlan serialized;
                            DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(query.queryPlan, &serialized);

                            auto queryId = client.registerQuery(query.queryPlan);
                            client.start(queryId);
                            query.queryId = queryId;

                            runningQueries.blockingWrite(std::move(query));
                            runningQueryCount.fetch_add(1);
                        }
                        finishedProducing = true;
                    });

                while (not finishedProducing)
                {
                    Systest::Query query;
                    runningQueries.blockingRead(query);
                    INVARIANT(query.queryId.has_value(), "QueryId should exist after startup")
                    while (client.status(query.queryId.value()).status() != Stopped)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    }
                    client.unregister(query.queryId.value());
                    checkResult(query);
                    runningQueryCount.fetch_sub(1);
                }
                producer.join();
            }
            else /// case: run locally without grpc
            {
                Configuration::SingleNodeWorkerConfiguration conf = Configuration::SingleNodeWorkerConfiguration();
                if (not config.workerConfig.getValue().empty())
                {
                    conf.engineConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig.getValue());
                }
                if (not config.queryCompilerConfig.getValue().empty())
                {
                    conf.queryCompilerConfiguration.overwriteConfigWithYAMLFileInput(config.queryCompilerConfig.getValue());
                }
                SingleNodeWorker worker = SingleNodeWorker(conf);

                std::atomic<size_t> queriesToResultCheck{0};
                std::atomic finishedProducing{false};

                std::thread producer(
                    [&]()
                    {
                        for (auto& query : queries)
                        {
                            while (queriesToResultCheck.load() >= capacity)
                            {
                                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                            }

                            SerializableDecomposedQueryPlan serialized;
                            DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(query.queryPlan, &serialized);

                            auto queryId = worker.registerQuery(query.queryPlan);
                            worker.startQuery(queryId);
                            query.queryId = queryId.getRawValue();

                            runningQueries.blockingWrite(std::move(query));
                            queriesToResultCheck.fetch_add(1);
                        }
                        finishedProducing = true;
                    });

                while (not finishedProducing or queriesToResultCheck.load() > 0)
                {
                    Systest::Query query;
                    runningQueries.blockingRead(query);
                    INVARIANT(query.queryId.has_value(), "QueryId should exist after startup")
                    while (worker.getQuerySummary(QueryId(query.queryId.value()))->currentStatus != Runtime::Execution::QueryStatus::Stopped)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    }
                    worker.unregisterQuery(QueryId(query.queryId.value()));

                    checkResult(query);
                    queriesToResultCheck.fetch_sub(1, std::memory_order_release);
                }
                producer.join();
            }
        }
        else if (com == Systest::Command::cache)
        {
            Systest::clearCacheDir(config);
            auto testFiles = Systest::discoverTestFiles(config);

            for (const auto& testFile : testFiles)
            {
                /// Get from input the filename without the extension
                auto testname = testFile.path.stem().string();
                auto cacheDir = config.cacheDir.getValue();
                auto resultDir = std::filesystem::path(config.resultDir.getValue());

                /// A SqlLogicTest format file might have >=1 tests
                auto decomposedQueryPlans = Systest::loadFromSLTFile(testFile.path, resultDir, testname);
                for (std::size_t testnr = 0; const auto& plan : decomposedQueryPlans)
                {
                    SerializableDecomposedQueryPlan serialized;
                    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, &serialized);
                    auto outfilename = fmt::format("{}/{}_{}.pb", cacheDir, testname, testnr);
                    std::ofstream file;
                    file = std::ofstream(outfilename);
                    if (!file)
                    {
                        NES_FATAL_ERROR("Could not open output file: {}", outfilename);
                        std::exit(1);
                    }
                    if (!serialized.SerializeToOstream(&file))
                    {
                        NES_FATAL_ERROR("Failed to write message to file.");
                        std::exit(1);
                    }
                    ++testnr;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException();
        std::exit(1);
    }
}
