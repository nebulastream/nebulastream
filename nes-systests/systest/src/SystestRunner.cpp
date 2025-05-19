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

#include <SystestRunner.hpp>

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
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fmt/std.h>
#include <folly/MPMCQueue.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <yaml-cpp/node/parse.h>

#include <Sources/SourceDataProvider.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <Sources/SourceDataProvider.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <ErrorHandling.hpp>
#include <NebuLI.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <SystestGrpc.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>



namespace NES::Systest
{

/// NOLINTBEGIN(readability-function-cognitive-complexity)
std::vector<LoadedQueryPlan>
loadFromSLTFile(SystestStarterGlobals& systestStarterGlobals, const std::filesystem::path& testFilePath, std::string_view testFileName)
{
    std::vector<LoadedQueryPlan> plans{};
    CLI::QueryConfig config{};
    SystestParser parser{};
    std::unordered_map<std::string, std::filesystem::path> sourceNamesToFilepath;
    std::unordered_map<std::string, SystestSchema> sinkNamesToSchema{
        {"CHECKSUM",
         {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "S$Count"},
          {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "S$Checksum"}}}};

    parser.registerSubstitutionRule(
        {.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = systestStarterGlobals.getTestDataDir(); }});
    parser.registerSubstitutionRule({"CONFIG", [&](std::string& substitute) { substitute = systestStarterGlobals.getConfigDir(); }});
    if (!parser.loadFile(testFilePath))
    {
        throw TestException("Could not successfully load test file://{}", testFilePath.string());
    }

    /// We create a map from sink names to their schema
    parser.registerOnSystestSystestSinkCallback([&](SystestParser::SystestSink&& sinkParsed)
                                                { sinkNamesToSchema.insert_or_assign(sinkParsed.name, sinkParsed.fields); });

    /// We add new found sources to our config
    parser.registerOnSystestLogicalSourceCallback(
        [&](SystestParser::SystestLogicalSource&& source)
        {
            config.logical.emplace_back(
                CLI::LogicalSource{
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
        });
    parser.registerOnAttachSourceCallback(
        [&](SystestAttachSource&& attachSource)
        {
            static uint64_t sourceIndex = 0;
            systestStarterGlobals.setDataServerThreadsInAttachSource(attachSource);

            /// Load physical source from file and overwrite logical source name with value from attach source
            const auto initialPhysicalSourceConfig
                = [](const std::string& logicalSourceName, const std::string& sourceConfigPath, const std::string& inputFormatterConfigPath)
            {
                try
                {
                    auto loadedPhysicalSourceConfig
                        = CLI::loadSystestPhysicalSourceFromYAML(logicalSourceName, sourceConfigPath, inputFormatterConfigPath);
                    return loadedPhysicalSourceConfig;
                }
                catch (const std::exception& e)
                {
                    throw CannotLoadConfig("Failed to parse source: {}", e.what());
                }
            }(attachSource.logicalSourceName, attachSource.sourceConfigurationPath, attachSource.inputFormatterConfigurationPath);

            switch (attachSource.testDataIngestionType)
            {
                case TestDataIngestionType::INLINE: {
                    if (attachSource.tuples.has_value())
                    {
                        const auto sourceFile
                            = SystestQuery::sourceFile(systestStarterGlobals.getWorkingDir(), testFileName, sourceIndex++);
                        config.physical.emplace_back(
                            Sources::SourceDataProvider::provideInlineDataSource(initialPhysicalSourceConfig, attachSource, sourceFile));
                        break;
                    }
                    throw CannotLoadConfig("An InlineData source must have tuples, but tuples was null.");
                }
                case TestDataIngestionType::FILE: {
                    config.physical.emplace_back(Sources::SourceDataProvider::provideFileDataSource(
                        initialPhysicalSourceConfig, attachSource, systestStarterGlobals.getTestDataDir()));
                    break;
                }
            }
            sourceNamesToFilepath[attachSource.logicalSourceName] = testFilePath;
        });
    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](std::string query, const size_t currentQueryNumberInTest)
        {
            /// For system level tests, a single file can hold arbitrary many tests. We need to generate a unique sink name for
            /// every test by counting up a static query number. We then emplace the unique sinks in the global (per test file) query config.
            static std::string currentTestFileName;

            /// We reset the current query number once we see a new test file
            if (currentTestFileName != testFileName)
            {
                currentTestFileName = testFileName;
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
            const auto sinkForQuery = sinkName + std::to_string(currentQueryNumberInTest);
            query = std::regex_replace(query, std::regex(sinkName), sinkForQuery);

            /// Adding the sink to the sink config, such that we can create a fully specified query plan
            const auto resultFile = SystestQuery::resultFile(systestStarterGlobals.getWorkingDir(), testFileName, currentQueryNumberInTest);

            if (sinkName == "CHECKSUM")
            {
                auto sink = CLI::Sink{.name = sinkName, .type = "Checksum", .config = {std::make_pair("filePath", resultFile)}};
                config.sinks.emplace(sinkForQuery, std::move(sink));
            }
            else
            {
                auto sinkCLI = CLI::Sink{
                    .name = sinkForQuery,
                    .type = "File",
                    .config
                    = {std::make_pair("inputFormat", "CSV"), std::make_pair("filePath", resultFile), std::make_pair("append", "false")}};
                config.sinks.emplace(sinkForQuery, std::move(sinkCLI));
            }

            config.query = query;
            auto plan = createFullySpecifiedQueryPlan(config);
            std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCountForQuery;
            for (const auto& logicalSource : NES::getOperatorByType<SourceDescriptorLogicalOperator>(*plan))
            {
                if (const auto sourceName = logicalSource.getSourceDescriptor().getLogicalSource().getLogicalSourceName();
                    sourceNamesToFilepath.contains(sourceName))
                {
                    auto& entry = sourceNamesToFilepathAndCountForQuery[sourceName];
                    entry = {sourceNamesToFilepath.at(sourceName), entry.second + 1};
                }
                else
                {
                    throw CannotLoadConfig("SourceName {} does not exist in sourceNamesToFilepathAndCount!", sourceName);
                }
            }
            INVARIANT(not sourceNamesToFilepathAndCountForQuery.empty(), "sourceNamesToFilepathAndCountForQuery should not be empty!");
            plans.emplace_back(*plan, query, sinkNamesToSchema[sinkName], currentQueryNumberInTest, sourceNamesToFilepathAndCountForQuery);
        });
    try
    {
        parser.parse(systestStarterGlobals, testFileName);
    }
    catch (Exception& exception)
    {
        tryLogCurrentException();
        exception.what() += fmt::format("Could not successfully parse test file://{}", testFilePath.string());
        throw;
    }
    return plans;
}
/// NOLINTEND(readability-function-cognitive-complexity)


std::vector<RunningQuery> runQueries(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    QuerySubmitter& submitter,
    const QueryResultMap& queryResultMap)
{
    std::vector<std::shared_ptr<RunningQuery>> terminatedQueries;
    std::vector<std::shared_ptr<RunningQuery>> runningQueries;
    std::vector<std::shared_ptr<RunningQuery>> failedQueries;
    size_t queryFinishedCounter = 0;
    std::vector queriesToRunReverseOrder(queries.rbegin(), queries.rend());

    while (!queriesToRunReverseOrder.empty() || !runningQueries.empty())
    {
        for (size_t concurrentQueries = runningQueries.size();
             concurrentQueries < numConcurrentQueries && !queriesToRunReverseOrder.empty();
             ++concurrentQueries)
        {
            const auto query = std::move(queriesToRunReverseOrder.back());
            queriesToRunReverseOrder.pop_back();

            const auto id = submitter.registerQuery(query);
            if (id == INVALID<QueryId>)
            {
                failedQueries.emplace_back(std::make_shared<RunningQuery>(query, id));
            }
            submitter.startQuery(id);
            runningQueries.emplace_back(std::make_shared<RunningQuery>(query, id));
        }

        for (auto querySummary : submitter.finishedQueries())
        {
            if (auto currentQuery
                = std::ranges::find(runningQueries, querySummary.queryId, [](const auto& runningQuery) { return runningQuery->queryId; });
                currentQuery != runningQueries.end())
            {
                if (auto queryFailed = querySummary.runs.back().error)
                {
                    fmt::println(std::cerr, "Query {} failed with exception: {}", querySummary.queryId, queryFailed->what());
                    failedQueries.emplace_back(*currentQuery);
                }
                auto error = checkResult(**currentQuery, queryResultMap);
                auto result = std::make_shared<RunningQuery>((*currentQuery)->query, (*currentQuery)->queryId);
                if (error.has_value())
                {
                    printQueryResultToStdOut(*result, error.value(), queryFinishedCounter++, queries.size(), "");
                    failedQueries.emplace_back(std::move(result));
                }
                else
                {
                    result->passed = true;
                    printQueryResultToStdOut(*result, "", queryFinishedCounter++, queries.size(), "");
                    terminatedQueries.emplace_back(std::move(result));
                }
                std::erase_if(runningQueries, [&](const auto& runningQuery) { return runningQuery->queryId == (*currentQuery)->queryId; });
            }
            else
            {
                throw TestException("Received non registered queryId: {}", (*currentQuery)->queryId);
            }
        }
    }

    return failedQueries | std::views::transform([](const auto& query) { return *query; }) | std::ranges::to<std::vector>();
}

namespace
{
std::vector<RunningQuery> serializeExecutionResults(const std::vector<RunningQuery>& queries, nlohmann::json& resultJson)
{
    std::vector<RunningQuery> failedQueries;
    for (const auto& queryRan : queries)
    {
        if (!queryRan.passed)
        {
            failedQueries.emplace_back(queryRan);
        }
        const auto executionTimeInSeconds = queryRan.getElapsedTime().count();
        resultJson.push_back({
            {"query name", queryRan.query.testName},
            {"time", executionTimeInSeconds},
            {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
            {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
        });
    }
    return failedQueries;
}
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<SystestQuery>& queries,
    const Configuration::SingleNodeWorkerConfiguration& configuration,
    nlohmann::json& resultJson,
    const QueryResultMap& queryResultMap)
{
    LocalWorkerQuerySubmitter submitter(configuration);
    std::vector<std::shared_ptr<RunningQuery>> ranQueries;
    std::size_t queryFinishedCounter = 0;
    const auto totalQueries = queries.size();
    for (const auto& queryToRun : queries)
    {
        const auto queryId = submitter.registerQuery(queryToRun);
        auto runningQueryPtr = std::make_shared<RunningQuery>(queryToRun, queryId);
        runningQueryPtr->passed = false;
        ranQueries.emplace_back(runningQueryPtr);
        submitter.startQuery(queryId);
        const auto summary = submitter.finishedQueries().at(0);

        if (summary.runs.empty() or summary.runs.back().error.has_value())
        {
            fmt::println(std::cerr, "Query {} has failed with: {}", queryId, summary.runs.back().error->what());
            continue;
        }
        runningQueryPtr->querySummary = summary;

        /// Getting the size and no. tuples of all input files to pass this information to currentRunningQuery.bytesProcessed
        size_t bytesProcessed = 0;
        size_t tuplesProcessed = 0;
        for (const auto& [sourcePath, sourceOccurrencesInQuery] : queryToRun.sourceNamesToFilepathAndCount | std::views::values)
        {
            bytesProcessed += (std::filesystem::file_size(sourcePath) * sourceOccurrencesInQuery);

            /// Counting the lines, i.e., \n in the sourcePath
            std::ifstream inFile(sourcePath);
            tuplesProcessed
                += std::count(std::istreambuf_iterator(inFile), std::istreambuf_iterator<char>(), '\n') * sourceOccurrencesInQuery;
        }
        ranQueries.back()->bytesProcessed = bytesProcessed;
        ranQueries.back()->tuplesProcessed = tuplesProcessed;

        auto errorMessage = checkResult(*ranQueries.back(), queryResultMap);
        ranQueries.back()->passed = not errorMessage.has_value();
        const auto queryPerformanceMessage
            = fmt::format(" in {} ({})", ranQueries.back()->getElapsedTime(), ranQueries.back()->getThroughput());
        printQueryResultToStdOut(
            *ranQueries.back(), errorMessage.value_or(""), queryFinishedCounter, totalQueries, queryPerformanceMessage);
        ranQueries.emplace_back(ranQueries.back());

        queryFinishedCounter += 1;
    }

    return serializeExecutionResults(
        ranQueries | std::views::transform([](const auto& query) { return *query; }) | std::ranges::to<std::vector>(), resultJson);
}

void printQueryResultToStdOut(
    const RunningQuery& runningQuery,
    const std::string& errorMessage,
    const size_t queryCounter,
    const size_t totalQueries,
    const std::string_view queryPerformanceMessage)
{
    const auto queryNameLength = runningQuery.query.testName.size();
    const auto queryNumberAsString = std::to_string(runningQuery.query.queryIdInFile);
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << runningQuery.query.testName << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0') << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (runningQuery.passed)
    {
        std::cout << "PASSED" << queryPerformanceMessage << '\n';
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.query.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", errorMessage);
        std::cout << "===================================================================" << '\n';
    }
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const Configuration::SingleNodeWorkerConfiguration& configuration,
    const NES::Systest::QueryResultMap& queryResultMap)
{
    LocalWorkerQuerySubmitter submitter(configuration);
    return runQueries(queries, numConcurrentQueries, submitter, queryResultMap);
}
std::vector<RunningQuery> runQueriesAtRemoteWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const std::string& serverURI,
    const NES::Systest::QueryResultMap& queryResultMap)
{
    RemoteWorkerQuerySubmitter submitter(serverURI);
    return runQueries(queries, numConcurrentQueries, submitter, queryResultMap);
}

}
