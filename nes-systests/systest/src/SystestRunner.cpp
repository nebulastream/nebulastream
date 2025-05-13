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
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ostream.h>
#include <folly/MPMCQueue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceValidationProvider.hpp>

#include <API/Schema.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <NebuLI.hpp>
#include <SingleNodeWorker.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Systest
{

/// NOLINTBEGIN(readability-function-cognitive-complexity)
std::vector<LoadedQueryPlan> SystestBinder::loadFromSLTFile(
    const std::filesystem::path& testFilePath,
    const std::filesystem::path& workingDir,
    std::string_view testFileName,
    const std::filesystem::path& testDataDir)
{
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    std::vector<LoadedQueryPlan> plans{};
    std::unordered_map<std::string, std::shared_ptr<Sinks::SinkDescriptor>> sinks;
    SystestParser parser{};

    std::unordered_map<std::string, Schema> sinkNamesToSchema{};
    auto [checksumSinkPair, success] = sinkNamesToSchema.emplace("CHECKSUM", Schema{Schema::MemoryLayoutType::ROW_LAYOUT});
    checksumSinkPair->second.addField("S$Count", DataTypeProvider::provideBasicType(BasicType::UINT64));
    checksumSinkPair->second.addField("S$Checksum", DataTypeProvider::provideBasicType(BasicType::UINT64));

    parser.registerSubstitutionRule({"TESTDATA", [&](std::string& substitute) { substitute = testDataDir; }});
    if (!parser.loadFile(testFilePath))
    {
        throw TestException("Could not successfully load test file://{}", testFilePath.string());
    }

    /// We create a map from sink names to their schema
    parser.registerOnSinkCallBack(
        [&](const SystestParser::Sink& sinkParsed)
        {
            auto [sinkPair, success] = sinkNamesToSchema.emplace(sinkParsed.name, Schema{Schema::MemoryLayoutType::ROW_LAYOUT});
            if (not success)
            {
                throw SourceAlreadyExists("{}", sinkParsed.name);
            }
            for (const auto& [type, name] : sinkParsed.fields)
            {
                sinkPair->second.addField(name, type);
            }
        });

    /// We add new found sources to our config
    parser.registerOnCSVSourceCallback(
        [&](SystestParser::CSVSource&& source)
        {
            Schema schema{Schema::MemoryLayoutType::ROW_LAYOUT};
            for (const auto& [type, name] : source.fields)
            {
                schema.addField(name, type);
            }
            const auto logicalSource = sourceCatalog->addLogicalSource(source.name, schema);
            if (not logicalSource.has_value())
            {
                throw SourceAlreadyExists("{}", source.name);
            }

            auto sourceConfig = Sources::SourceValidationProvider::provide(
                "File", std::unordered_map<std::string, std::string>{{"filePath", source.csvFilePath}});
            const auto parserConfig = Sources::ParserConfig{.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};

            const auto physicalSource = sourceCatalog->addPhysicalSource(
                logicalSource.value(), INITIAL<WorkerId>, "File", -1, std::move(sourceConfig), parserConfig);
            if (not physicalSource.has_value())
            {
                NES_ERROR("Concurrent deletion of just created logical source \"{}\" by another thread", source.name);
                throw UnregisteredSource("{}", source.name);
            }
        });

    parser.registerOnSLTSourceCallback(
        [&](SystestParser::SLTSource&& source)
        {
            const auto sourceFile = Query::sourceFile(workingDir, testFileName, sourceIndex++);
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
                NES_INFO("Written in file: {}. Number of Tuples: {}", sourceFile, source.tuples.size());
            }

            Schema schema{Schema::MemoryLayoutType::ROW_LAYOUT};
            for (const auto& [type, name] : source.fields)
            {
                schema.addField(name, type);
            }
            const auto logicalSource = sourceCatalog->addLogicalSource(source.name, schema);
            if (not logicalSource.has_value())
            {
                throw SourceAlreadyExists("{}", source.name);
            }

            auto sourceConfig = Sources::SourceValidationProvider::provide(
                "File", std::unordered_map<std::string, std::string>{{"filePath", sourceFile}});
            const auto parserConfig = Sources::ParserConfig{.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};

            const auto physicalSource = sourceCatalog->addPhysicalSource(
                logicalSource.value(), INITIAL<WorkerId>, "File", -1, std::move(sourceConfig), parserConfig);
            if (not physicalSource.has_value())
            {
                NES_ERROR("Concurrent deletion of just created logical source \"{}\" by another thread", source.name);
                throw UnregisteredSource("{}", source.name);
            }
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](SystestParser::Query&& query)
        {
            /// For system level tests, a single file can hold arbitrary many tests. We need to generate a unique sink name for
            /// every test by counting up a static query number. We then emplace the unique sinks in the global (per test file) query config.
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
            std::shared_ptr<Sinks::SinkDescriptor> sink{};
            if (sinkName == "CHECKSUM")
            {
                auto validatedSinkConfig
                    = Sinks::SinkDescriptor::validateAndFormatConfig("Checksum", {std::make_pair("filePath", resultFile)});
                sink = std::make_shared<Sinks::SinkDescriptor>("Checksum", std::move(validatedSinkConfig), false);
            }
            else
            {
                auto validatedSinkConfig = Sinks::SinkDescriptor::validateAndFormatConfig(
                    "File",
                    {std::make_pair("inputFormat", "CSV"), std::make_pair("filePath", resultFile), std::make_pair("append", "false")});
                sink = std::make_shared<Sinks::SinkDescriptor>("File", std::move(validatedSinkConfig), false);
            }
            sinks.emplace(sinkForQuery, sink);

            const auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
            const auto sinkOperators = plan->getSinkOperators();

            PRECONDITION(
                sinkOperators.size() == 1,
                "NebulaStream currently only supports a single sink per query, but the query contains: {}",
                sinkOperators.size());

            if (const auto sinkIter = sinks.find(sinkOperators.at(0)->sinkName); sinkIter == sinks.end())
            {
                throw UnknownSinkType(
                    "Sinkname {} not specified in the configuration {}",
                    sinkOperators.front()->sinkName,
                    fmt::join(std::views::keys(sinks), ","));
            }
            sinkOperators.at(0)->sinkDescriptor = sink;
            plans.emplace_back(plan, sourceCatalog, query, sinkNamesToSchema[sinkName]);
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
/// NOLINTEND(readability-function-cognitive-complexity)

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<Query>& queries,
    const uint64_t numConcurrentQueries,
    const Configuration::SingleNodeWorkerConfiguration& configuration)
{
    folly::Synchronized<std::vector<RunningQuery>> failedQueries;
    folly::Synchronized<std::vector<std::shared_ptr<RunningQuery>>> runningQueries;
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

                    auto runningQueryPtr = std::make_unique<RunningQuery>(
                        query, QueryId(queryId), QueryExecutionInfo{std::chrono::high_resolution_clock::now()});
                    runningQueries.wlock()->emplace_back(std::move(runningQueryPtr));

                    /// We are waiting if we have reached the maximum number of concurrent queries
                    queriesToResultCheck.fetch_add(1);
                    while (queriesToResultCheck.load() == numConcurrentQueries)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    }
                }
            }
            finishedProducing = true;
            cv.notify_all();
        });

    while (not finishedProducing or queriesToResultCheck > 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        {
            auto runningQueriesLock = runningQueries.wlock();
            for (auto runningQueriesIter = runningQueriesLock->begin(); runningQueriesIter != runningQueriesLock->end();)
            {
                const auto& runningQuery = *runningQueriesIter;
                if (runningQuery)
                {
                    const auto queryStatus = worker.getQuerySummary(QueryId(runningQuery->queryId))->currentStatus;
                    if (queryStatus == Runtime::Execution::QueryStatus::Stopped or queryStatus == Runtime::Execution::QueryStatus::Failed)
                    {
                        worker.unregisterQuery(QueryId(runningQuery->queryId));
                        runningQuery->queryExecutionInfo.endTime = std::chrono::high_resolution_clock::now();
                        auto errorMessage = checkResult(*runningQuery);
                        printQueryResultToStdOut(*runningQuery, errorMessage.value_or(""), queryFinishedCounter.fetch_add(1), totalQueries);
                        if (errorMessage.has_value())
                        {
                            failedQueries.wlock()->emplace_back(*runningQuery);
                        }
                        runningQueriesIter = runningQueriesLock->erase(runningQueriesIter);
                        queriesToResultCheck.fetch_sub(1);
                        cv.notify_one();
                        continue;
                    }
                }
                ++runningQueriesIter;
            }
        }
    }

    return failedQueries.copy();
}

std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<Query>& queries, const uint64_t numConcurrentQueries, const std::string& serverURI)
{
    folly::Synchronized<std::vector<RunningQuery>> failedQueries;
    folly::Synchronized<std::vector<std::shared_ptr<RunningQuery>>> runningQueries;
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
                    auto runningQueryPtr = std::make_unique<RunningQuery>(
                        query, QueryId(queryId), QueryExecutionInfo{std::chrono::high_resolution_clock::now()});
                    runningQueries.wlock()->emplace_back(std::move(runningQueryPtr));

                    /// We are waiting if we have reached the maximum number of concurrent queries
                    runningQueryCount.fetch_add(1);
                    while (runningQueryCount.load() <= numConcurrentQueries)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    }
                }
            }
            finishedProducing = true;
            cv.notify_all();
        });

    while (not finishedProducing or runningQueryCount > 0)
    {
        auto runningQueriesLock = runningQueries.wlock();
        for (auto runningQueriesIter = runningQueriesLock->begin(); runningQueriesIter != runningQueriesLock->end();)
        {
            const auto& runningQuery = *runningQueriesIter;
            if (runningQuery and (client.status(runningQuery->queryId.getRawValue()).status() == Stopped or client.status(runningQuery->queryId.getRawValue()).status() == Failed))
            {
                client.unregister(runningQuery->queryId.getRawValue());
                runningQuery->queryExecutionInfo.endTime = std::chrono::high_resolution_clock::now();
                auto errorMessage = checkResult(*runningQuery);
                printQueryResultToStdOut(*runningQuery, errorMessage.value_or(""), queryFinishedCounter.fetch_add(1), totalQueries);
                if (errorMessage.has_value())
                {
                    failedQueries.wlock()->emplace_back(*runningQuery);
                }
                runningQueriesIter = runningQueriesLock->erase(runningQueriesIter);
                runningQueryCount.fetch_sub(1);
                cv.notify_one();
            }
            else
            {
                ++runningQueriesIter;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    return failedQueries.copy();
}

std::vector<RunningQuery> serializeExecutionResults(const std::vector<RunningQuery>& queries, nlohmann::json& resultJson)
{
    std::vector<RunningQuery> failedQueries;
    for (const auto& queryRan : queries)
    {
        if (!queryRan.queryExecutionInfo.passed)
        {
            failedQueries.emplace_back(queryRan);
        }
        const auto executionTimeInNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              queryRan.queryExecutionInfo.endTime - queryRan.queryExecutionInfo.startTime)
                                              .count();
        resultJson.push_back({{"query name", queryRan.query.name}, {"time", executionTimeInNanos}});
    }
    return failedQueries;
}

Runtime::QuerySummary waitForQueryTermination(SingleNodeWorker& worker, QueryId queryId)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        const auto summary = worker.getQuerySummary(queryId);
        if (summary)
        {
            if (summary->currentStatus == Runtime::Execution::QueryStatus::Stopped)
            {
                return *summary;
            }
        }
    }
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<Query>& queries, const Configuration::SingleNodeWorkerConfiguration& configuration, nlohmann::json& resultJson)
{
    SingleNodeWorker worker(configuration);
    std::vector<RunningQuery> ranQueries;
    std::size_t queryFinishedCounter = 0;
    const auto totalQueries = queries.size();
    for (const auto& queryToRun : queries)
    {
        const auto queryId = worker.registerQuery(queryToRun.queryPlan);
        ranQueries.emplace_back(queryToRun, queryId, QueryExecutionInfo{std::chrono::high_resolution_clock::now()});
        worker.startQuery(queryId);
        const auto summary = waitForQueryTermination(worker, queryId);

        if (summary.runs.back().error.has_value())
        {
            fmt::println(std::cerr, "Query {} has failed with: {}", queryId, summary.runs.back().error->what());
            continue;
        }

        auto errorMessage = checkResult(ranQueries.back());
        ranQueries.back().queryExecutionInfo.passed = !errorMessage.has_value();
        ranQueries.back().queryExecutionInfo.endTime = std::chrono::high_resolution_clock::now();

        printQueryResultToStdOut(ranQueries.back(), errorMessage.value_or(""), queryFinishedCounter, totalQueries);
        worker.unregisterQuery(queryId);
        queryFinishedCounter += 1;
    }

    return serializeExecutionResults(ranQueries, resultJson);
}

void printQueryResultToStdOut(
    const RunningQuery& runningQuery, const std::string& errorMessage, const size_t queryCounter, const size_t totalQueries)
{
    const auto queryNameLength = runningQuery.query.name.size();
    const auto queryNumberAsString = std::to_string(runningQuery.query.queryIdInFile + 1);
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);
    const std::chrono::duration<double> queryDurationInMs
        = (runningQuery.queryExecutionInfo.endTime - runningQuery.queryExecutionInfo.startTime);
    const auto queryDurationTime = fmt::format(" in {}", queryDurationInMs);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << runningQuery.query.name << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0') << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (errorMessage.empty())
    {
        std::cout << "PASSED" << queryDurationTime << '\n';
    }
    else
    {
        std::cout << "FAILED" << queryDurationTime << '\n';
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.query.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        std::cout << errorMessage;
        std::cout << "===================================================================" << '\n';
    }
}
}
