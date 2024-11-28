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

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <folly/MPMCQueue.h>

#include <ErrorHandling.hpp>
#include <NebuLI.hpp>
#include <SingleNodeWorker.hpp>
#include <SystestGrpc.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::Systest
{

std::vector<LoadedQueryPlan>
loadFromSLTFile(const std::filesystem::path& testFilePath, const std::filesystem::path& workingDir, const std::string& testFileName)
{
    std::vector<LoadedQueryPlan> plans{};
    CLI::QueryConfig config{};
    SystestParser parser{};
    std::unordered_map<std::string, SystestParser::Schema> sinkNamesToSchema{
        {"CHECKSUM", {{BasicType::UINT64, "S$Count"}, {BasicType::UINT64, "S$Checksum"}}}};

    parser.registerSubstitutionRule({"TESTDATA", [&](std::string& substitute) { substitute = std::string(TEST_DATA_DIR); }});
    if (!parser.loadFile(testFilePath))
    {
        throw TestException("Could not successfully load test file://{}", testFilePath.string());
    }

    /// We create a map from sink names to their schema
    parser.registerOnSinkCallBack([&](SystestParser::Sink&& sinkParsed)
                                  { sinkNamesToSchema.insert_or_assign(sinkParsed.name, sinkParsed.fields); });

    /// We add new found sources to our config
    parser.registerOnCSVSourceCallback(
        [&](SystestParser::CSVSource&& source)
        {
            config.logical.emplace_back(CLI::LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<CLI::SchemaField> schema;
                    for (const auto& [type, name] : source.fields)
                    {
                        schema.emplace_back(name, type);
                    }
                    return schema;
                }()});

            config.physical.emplace_back(CLI::PhysicalSource{
                .logical = source.name,
                .parserConfig = {{"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", ","}},
                .sourceConfig = {{"type", "File"}, {"filePath", source.csvFilePath}}});
        });

    parser.registerOnSLTSourceCallback(
        [&](SystestParser::SLTSource&& source)
        {
            static uint64_t sourceIndex = 0;

            config.logical.emplace_back(CLI::LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<CLI::SchemaField> schema;
                    for (const auto& [type, name] : source.fields)
                    {
                        schema.emplace_back(name, type);
                    }
                    return schema;
                }()});

            const auto sourceFile = Query::sourceFile(workingDir, testFileName, sourceIndex++);
            config.physical.emplace_back(CLI::PhysicalSource{
                .logical = source.name,
                .parserConfig = {{"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", ","}},
                .sourceConfig = {{"type", "File"}, {"filePath", sourceFile}}});


            {
                std::ofstream testFile(sourceFile);
                if (!testFile.is_open())
                {
                    throw TestException("Could not open source file \"{}\"", sourceFile);
                }

                for (const auto& tuple : source.tuples)
                {
                    testFile << tuple << "\n";
                }
                testFile.flush();
            }

            NES_INFO("Written in file: {}. Number of Tuples: {}", sourceFile, source.tuples.size());
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](SystestParser::Query&& query)
        {
            /// For system level tests, a single file can hold arbitrary many tests. We need to generate a unique sink name for
            /// every test by counting up a static query number. We then emplace the unique sinks in the global (per test file) query config.
            static size_t currentQueryNumber = 0;
            static std::string currentTestFileName;

            /// We reset the current query number once we see a new test file
            if (currentTestFileName != testFileName)
            {
                currentTestFileName = testFileName;
                currentQueryNumber = 0;
            }
            else
            {
                ++currentQueryNumber;
            }

            /// We expect at least one sink to be defined in the test file
            if (sinkNamesToSchema.empty())
            {
                throw TestException("No sinks defined in test file: {}", testFileName);
            }

            /// We have to get all sink names from the query and then create custom paths for each sink.
            /// The filepath can not be the sink name, as we might have multiple queries with the same sink name, i.e., sink20Booleans in FunctionEqual.test
            /// We assume:
            /// - the INTO keyword is the last keyword in the query
            /// - the sink name is the last word in the INTO clause
            const auto sinkName = [&query]() -> std::string
            {
                const auto intoClause = query.find("INTO");
                if (intoClause == std::string::npos)
                {
                    NES_ERROR("INTO clause not found in query: {}", query);
                    return "";
                }
                const auto intoLength = std::string("INTO").length();
                auto trimmedSinkName = std::string(Util::trimWhiteSpaces(query.substr(intoClause + intoLength)));

                /// As the sink name might have a semicolon at the end, we remove it
                if (trimmedSinkName.back() == ';')
                {
                    trimmedSinkName.pop_back();
                }
                return trimmedSinkName;
            }();

            if (sinkName.empty() or not sinkNamesToSchema.contains(sinkName))
            {
                throw UnknownSinkType("Failed to find sink name <{}>", sinkName);
            }


            /// Replacing the sinkName with the created unique sink name
            const auto sinkForQuery = sinkName + std::to_string(currentQueryNumber);
            query = std::regex_replace(query, std::regex(sinkName), sinkForQuery);

            /// Adding the sink to the sink config, such that we can create a fully specified query plan
            const auto resultFile = Query::resultFile(workingDir, testFileName, currentQueryNumber);

            if (sinkName == "CHECKSUM")
            {
                auto sink = CLI::Sink{sinkName, "Checksum", {std::make_pair("filePath", resultFile)}};
                config.sinks.emplace(sinkForQuery, std::move(sink));
            }
            else
            {
                auto sinkCLI = CLI::Sink{
                    sinkForQuery,
                    "File",
                    {std::make_pair("inputFormat", "CSV"), std::make_pair("filePath", resultFile), std::make_pair("append", "false")}};
                config.sinks.emplace(sinkForQuery, std::move(sinkCLI));
            }

            config.query = query;
            auto plan = createFullySpecifiedQueryPlan(config);
            plans.emplace_back(plan, query, sinkNamesToSchema[sinkName]);
        });
    try
    {
        parser.parse();
    }
    catch (Exception& exception)
    {
        tryLogCurrentException();
        exception.what() += fmt::format("Could not successfully parse test file://{}", testFilePath.string());
        throw exception;
    }
    return plans;
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<Query>& queries,
    const uint64_t numConcurrentQueries,
    const Configuration::SingleNodeWorkerConfiguration& configuration)
{
    folly::Synchronized<std::vector<RunningQuery>> failedQueries;
    folly::MPMCQueue<std::shared_ptr<RunningQuery>> runningQueries(numConcurrentQueries);
    SingleNodeWorker worker(configuration);

    std::atomic<size_t> queriesToResultCheck{0};
    std::atomic<size_t> queryFinishedCounter{0};
    const auto totalQueries = queries.size();
    std::atomic finishedProducing{false};

    std::mutex mtx;
    std::condition_variable cv;

    std::jthread producer(
        [&queries, &queriesToResultCheck, &worker, &numConcurrentQueries, &runningQueries, &finishedProducing, &mtx, &cv](
            std::stop_token stopToken)
        {
            for (auto& query : queries)
            {
                {
                    std::unique_lock lock(mtx);
                    cv.wait(
                        lock,
                        [&stopToken, &queriesToResultCheck, &numConcurrentQueries]
                        { return stopToken.stop_requested() or queriesToResultCheck.load() < numConcurrentQueries; });
                    if (stopToken.stop_requested())
                    {
                        finishedProducing = true;
                        return;
                    }
                    const auto queryId = worker.registerQuery(query.queryPlan);
                    if (queryId == INVALID_QUERY_ID)
                    {
                        throw QueryInvalid("Received an invalid query id from the worker");
                    }
                    worker.startQuery(queryId);

                    const RunningQuery runningQuery = {query, queryId};
                    auto runningQueryPtr = std::make_unique<RunningQuery>(query, QueryId(queryId));
                    runningQueries.blockingWrite(std::move(runningQueryPtr));
                    queriesToResultCheck.fetch_add(1);
                }
            }
            finishedProducing = true;
            cv.notify_all();
        });

    while (not finishedProducing or queriesToResultCheck > 0)
    {
        std::shared_ptr<RunningQuery> runningQuery;
        runningQueries.blockingRead(runningQuery);

        if (runningQuery)
        {
            while (worker.getQuerySummary(QueryId(runningQuery->queryId))->currentStatus != Runtime::Execution::QueryStatus::Stopped
                   and worker.getQuerySummary(QueryId(runningQuery->queryId))->currentStatus != Runtime::Execution::QueryStatus::Failed)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
            worker.unregisterQuery(QueryId(runningQuery->queryId));
            auto errorMessage = checkResult(*runningQuery);
            printQueryResultToStdOut(runningQuery->query, errorMessage.value_or(""), queryFinishedCounter.fetch_add(1), totalQueries);
            if (errorMessage.has_value())
            {
                failedQueries.wlock()->emplace_back(*runningQuery);
            }
            queriesToResultCheck.fetch_sub(1, std::memory_order_release);
            cv.notify_one();
        }
    }

    return failedQueries.copy();
}

std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<Query>& queries, const uint64_t numConcurrentQueries, const std::string& serverURI)
{
    folly::Synchronized<std::vector<RunningQuery>> failedQueries;
    folly::MPMCQueue<std::shared_ptr<RunningQuery>> runningQueries(numConcurrentQueries);
    const GRPCClient client(CreateChannel(serverURI, grpc::InsecureChannelCredentials()));

    std::atomic<size_t> runningQueryCount{0};
    std::atomic<size_t> queryFinishedCounter{0};
    const auto totalQueries = queries.size();
    std::atomic finishedProducing{false};

    std::mutex mtx;
    std::condition_variable cv;

    std::jthread producer(
        [&queries, &runningQueryCount, &client, &numConcurrentQueries, &runningQueries, &finishedProducing, &mtx, &cv](
            std::stop_token stopToken)
        {
            for (auto& query : queries)
            {
                {
                    std::unique_lock lock(mtx);
                    cv.wait(lock, [&] { return stopToken.stop_requested() or runningQueryCount.load() < numConcurrentQueries; });
                    if (stopToken.stop_requested())
                    {
                        finishedProducing = true;
                        return;
                    }

                    const auto queryId = client.registerQuery(*query.queryPlan);
                    client.start(queryId);
                    const RunningQuery runningQuery = {query, QueryId(queryId)};

                    auto runningQueryPtr = std::make_unique<RunningQuery>(query, QueryId(queryId));
                    runningQueries.blockingWrite(std::move(runningQueryPtr));
                    runningQueryCount.fetch_add(1);
                }
            }
            finishedProducing = true;
            cv.notify_all();
        });

    while (not finishedProducing or runningQueryCount > 0)
    {
        std::shared_ptr<RunningQuery> runningQuery;
        runningQueries.blockingRead(runningQuery);

        if (runningQuery)
        {
            INVARIANT(runningQuery != nullptr, "query is not running");
            while (client.status(runningQuery->queryId.getRawValue()).status() != Stopped
                   and client.status(runningQuery->queryId.getRawValue()).status() != Failed)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
            client.unregister(runningQuery->queryId.getRawValue());
            auto errorMessage = checkResult(*runningQuery);
            printQueryResultToStdOut(runningQuery->query, errorMessage.value_or(""), queryFinishedCounter.fetch_add(1), totalQueries);
            if (errorMessage.has_value())
            {
                failedQueries.wlock()->emplace_back(*runningQuery);
            }
            runningQueryCount.fetch_sub(1);
            cv.notify_one();
        }
    }

    return failedQueries.copy();
}

void printQueryResultToStdOut(const Query& query, const std::string& errorMessage, const size_t queryCounter, const size_t totalQueries)
{
    const auto queryNameLength = query.name.size();
    const auto queryNumberAsString = std::to_string(query.queryIdInFile + 1);
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << query.name << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0') << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (errorMessage.empty())
    {
        std::cout << "PASSED" << std::endl;
    }
    else
    {
        std::cout << "FAILED" << std::endl;
        /// spd logger cannot handle multiline prints with proper color and pattern.
        /// And as this is only for test runs we use stdout here.
        std::cout << "===================================================================" << '\n';
        std::cout << query.queryDefinition << std::endl;
        std::cout << "===================================================================" << '\n';
        std::cout << errorMessage;
        std::cout << "===================================================================" << std::endl;
    }
}
}
