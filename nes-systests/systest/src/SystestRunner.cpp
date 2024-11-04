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

#include <regex>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <grpc++/create_channel.h>
#include <NebuLI.hpp>
#include <SLTParser.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SingleNodeWorker.hpp>
#include <SystestGrpc.hpp>
#include <SystestResultCheck.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

std::vector<DecomposedQueryPlanPtr>
loadFromSLTFile(const std::filesystem::path& testFilePath, const std::filesystem::path& resultDir, const std::string& testname)
{
    std::vector<DecomposedQueryPlanPtr> plans{};
    CLI::QueryConfig config{};
    SLTParser::SLTParser parser{};

    parser.registerSubstitutionRule(
        {"SINK",
         [&](std::string& substitute)
         {
             /// While parsing we keep track of the query nr to derrive the correct result parse for the query
             static uint64_t queryNr = 0;
             static std::string lastTestname = "";
             if (testname == lastTestname)
             {
                 queryNr++;
             }
             else
             {
                 lastTestname = testname;
                 queryNr = 0;
             }

             auto resultFile = fmt::format("{}/{}{}.csv", resultDir.string(), testname, queryNr);
             substitute = fmt::format(R"(sink(FileSinkDescriptor::create("{}", "CSV_FORMAT", "OVERWRITE"));)", resultFile);
         }});

    parser.registerSubstitutionRule(
        {"TESTDATA", [&](std::string& substitute) { substitute = std::string(PATH_TO_BINARY_DIR) + "/test/testdata"; }});

    if (!parser.loadFile(testFilePath))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", testFilePath);
        return {};
    }

    /// We add new found sources to our config
    parser.registerOnCSVSourceCallback(
        [&](SLTParser::SLTParser::CSVSource&& source)
        {
            config.logical.emplace_back(CLI::LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<CLI::SchemaField> schema;
                    for (const auto& field : source.fields)
                    {
                        schema.push_back({.name = field.name, .type = field.type});
                    }
                    return schema;
                }()});

            config.physical.emplace_back(
                CLI::PhysicalSource{.logical = source.name, .config = {{"type", "CSV_SOURCE"}, {"filePath", source.csvFilePath}}});
        });

    const auto tmpSourceDir = std::string(PATH_TO_BINARY_DIR) + "/nes-systests/";
    parser.registerOnSLTSourceCallback(
        [&](SLTParser::SLTParser::SLTSource&& source)
        {
            static uint64_t sourceIndex = 0;

            config.logical.emplace_back(CLI::LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<CLI::SchemaField> schema;
                    for (const auto& field : source.fields)
                    {
                        schema.push_back({.name = field.name, .type = field.type});
                    }
                    return schema;
                }()});

            config.physical.emplace_back(CLI::PhysicalSource{
                .logical = source.name,
                .config = {{"type", "CSV"}, {"filePath", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv"}}});

            /// Write the tuples to a tmp file
            std::ofstream sourceFile(tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
            if (!sourceFile)
            {
                NES_FATAL_ERROR("Failed to open source file: {}", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
                return;
            }

            /// Write tuples to csv file
            for (const auto& tuple : source.tuples)
            {
                sourceFile << tuple << std::endl;
            }
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](SLTParser::SLTParser::Query&& query)
        {
            config.query = query;
            try
            {
                auto plan = CLI::createFullySpecifiedQueryPlan(config);
                plans.emplace_back(plan);
            }
            catch (const std::exception& e)
            {
                NES_FATAL_ERROR("Failed to create query plan: {}", e.what());
                std::exit(-1);
            }
        });
    try
    {
        parser.parse();
    }
    catch (const Exception&)
    {
        tryLogCurrentException();
        return {};
    }
    return plans;
}

bool runQueriesAtLocalWorker(
    std::vector<Query> queries, uint64_t numConcurrentQueries, const NES::Configuration::SingleNodeWorkerConfiguration& configuration)
{
    bool allTestSucceeded = true;

    folly::MPMCQueue<std::unique_ptr<RunningQuery>> runningQueries(numConcurrentQueries);
    NES::SingleNodeWorker worker = NES::SingleNodeWorker(configuration);

    std::atomic<size_t> queriesToResultCheck{0};
    std::atomic finishedProducing{false};

    std::thread producer(
        [&]()
        {
            for (auto& query : queries)
            {
                while (queriesToResultCheck.load() >= numConcurrentQueries)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(25));
                }

                auto queryId = worker.registerQuery(query.queryPlan);
                worker.startQuery(queryId);

                RunningQuery const runningQuery = {query, queryId};

                auto runningQueryPtr = std::make_unique<RunningQuery>(query, NES::QueryId(queryId));
                runningQueries.blockingWrite(std::move(runningQueryPtr));
                queriesToResultCheck.fetch_add(1);
            }
            finishedProducing = true;
        });

    while (not finishedProducing or queriesToResultCheck.load() > 0)
    {
        std::unique_ptr<RunningQuery> runningQuery;
        runningQueries.blockingRead(runningQuery);
        while (worker.getQuerySummary(NES::QueryId(runningQuery->queryId))->currentStatus != NES::Runtime::Execution::QueryStatus::Stopped)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        worker.unregisterQuery(NES::QueryId(runningQuery->queryId));

        if (!checkResult(runningQuery->query))
        {
            allTestSucceeded = false;
        }
        queriesToResultCheck.fetch_sub(1, std::memory_order_release);
    }
    producer.join();
    return allTestSucceeded;
}

bool runQueriesAtRemoteWorker(std::vector<Query> queries, uint64_t numConcurrentQueries, const std::string& serverURI)
{
    bool allTestSucceeded = true;

    folly::MPMCQueue<std::unique_ptr<RunningQuery>> runningQueries(numConcurrentQueries);
    GRPCClient client(grpc::CreateChannel(serverURI, grpc::InsecureChannelCredentials()));

    std::atomic<size_t> runningQueryCount{0};
    std::atomic finishedProducing{false};

    std::thread producer(
        [&]()
        {
            for (auto& query : queries)
            {
                while (runningQueryCount.load() >= numConcurrentQueries)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(25));
                }

                auto queryId = client.registerQuery(query.queryPlan);
                client.start(queryId);
                RunningQuery const runningQuery = {query, NES::QueryId(queryId)};

                auto runningQueryPtr = std::make_unique<RunningQuery>(query, NES::QueryId(queryId));
                runningQueries.blockingWrite(std::move(runningQueryPtr));
                runningQueryCount.fetch_add(1);
            }
            finishedProducing = true;
        });

    while (not finishedProducing)
    {
        std::unique_ptr<RunningQuery> runningQuery;
        runningQueries.blockingRead(runningQuery);
        INVARIANT(runningQuery != nullptr, "query is not running");
        while (client.status(runningQuery->queryId.getRawValue()).status() != Stopped)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        client.unregister(runningQuery->queryId.getRawValue());
        if (!checkResult(runningQuery->query))
        {
            allTestSucceeded = false;
        }
        runningQueryCount.fetch_sub(1);
    }
    producer.join();
    return allTestSucceeded;
}

}
