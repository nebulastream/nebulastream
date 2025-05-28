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
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fmt/std.h>
#include <folly/MPMCQueue.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
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
std::vector<LoadedQueryPlan> loadFromSLTFile(
    const std::filesystem::path& testFilePath,
    const std::filesystem::path& workingDir,
    std::string_view testFileName,
    const std::filesystem::path& testDataDir)
{
    std::vector<LoadedQueryPlan> plans{};
    CLI::QueryConfig config{};
    SystestParser parser{};
    std::unordered_map<std::string, std::filesystem::path> sourceNamesToFilepath;
    std::unordered_map<std::string, SystestParser::Schema> sinkNamesToSchema{
        {"CHECKSUM",
         {{.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "S$Count"},
          {.type = DataTypeProvider::provideDataType(DataType::Type::UINT64), .name = "S$Checksum"}}}};

    parser.registerSubstitutionRule({.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
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
                .sourceConfig = {{"type", "File"}, {"filePath", source.csvFilePath}, {"numberOfBuffersInLocalPool", "-1"}}});
            sourceNamesToFilepath[source.name] = source.csvFilePath;
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
            sourceNamesToFilepath[source.name] = sourceFile;
            config.physical.emplace_back(CLI::PhysicalSource{
                .logical = source.name,
                .parserConfig = {{"type", "CSV"}, {"tupleDelimiter", "\n"}, {"fieldDelimiter", ","}},
                .sourceConfig = {{"type", "File"}, {"filePath", sourceFile}, {"numberOfBuffersInLocalPool", "-1"}}});
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
            try
            {
                auto plan = createFullySpecifiedQueryPlan(config);
                std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCountForQuery;
                for (const auto& logicalSource : NES::getOperatorByType<SourceDescriptorLogicalOperator>(*plan))
                {
                    if (const auto sourceName = logicalSource.getSourceDescriptor().getLogicalSource().getLogicalSourceName();
                        sourceNamesToFilepath.contains(sourceName))
                    {
                        auto& entry = sourceNamesToFilepathAndCountForQuery[sourceName];
                        entry = {sourceNamesToFilepath.at(sourceName), entry.second + 1};
                        continue;
                    }
                    throw CannotLoadConfig("SourceName {} does not exist in sourceNamesToFilepathAndCount!");
                }
                INVARIANT(not sourceNamesToFilepathAndCountForQuery.empty(), "sourceNamesToFilepathAndCountForQuery should not be empty!");
                plans.emplace_back(*plan, query, sinkNamesToSchema[sinkName], sourceNamesToFilepathAndCountForQuery);
            }
            catch (Exception& e)
            {
                plans.emplace_back(std::unexpected(e), query, sinkNamesToSchema[sinkName]);
            }
        });

    parser.registerOnErrorExpectationCallback(
        [&](SystestParser::ErrorExpectation&& errorExpectation)
        {
            /// Error always belongs to the last parsed plan
            auto& lastPlan = plans.back();
            lastPlan.expectedError = ExpectedError{.code = errorExpectation.code, .message = errorExpectation.message};
        });
    try
    {
        parser.parse();
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

namespace
{
template <typename ErrorCallable>
void reportResult(
    std::shared_ptr<RunningQuery>& runningQuery,
    std::size_t& finishedCount,
    std::size_t total,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    ErrorCallable&& errorBuilder)
{
    std::string msg = errorBuilder();
    runningQuery->passed = msg.empty();
    printQueryResultToStdOut(*runningQuery, msg, finishedCount++, total, "");
    if (!msg.empty())
    {
        failed.push_back(runningQuery);
    }
}

bool passes(const std::shared_ptr<RunningQuery>& runningQuery)
{
    return runningQuery->passed;
}

}

std::vector<RunningQuery> runQueries(const std::vector<Query>& queries, size_t maxConcurrency, QuerySubmitter& submitter)
{
    std::queue<Query> pending;
    for (auto it = queries.rbegin(); it != queries.rend(); ++it)
    {
        pending.push(*it);
    }

    std::unordered_map<QueryId, std::shared_ptr<RunningQuery>> active;
    std::vector<std::shared_ptr<RunningQuery>> failed;
    std::size_t finished = 0;

    const auto startMoreQueries = [&]
    {
        while (active.size() < maxConcurrency && !pending.empty())
        {
            Query q = std::move(pending.front());
            pending.pop();

            /// Parsing
            if (!q.queryPlan)
            {
                auto runningQuery = std::make_shared<RunningQuery>(q);
                runningQuery->exception = q.queryPlan.error();
                reportResult(
                    runningQuery,
                    finished,
                    queries.size(),
                    failed,
                    [&]
                    {
                        if (q.expectedError and q.expectedError->code == runningQuery->exception->code())
                        {
                            return std::string{};
                        }
                        return fmt::format("unexpected parsing error: {}", *runningQuery->exception);
                    });
                continue; /// skip registration
            }

            /// Registration
            if (auto reg = submitter.registerQuery(*q.queryPlan))
            {
                submitter.startQuery(*reg);
                active.emplace(*reg, std::make_shared<RunningQuery>(q, *reg));
            }
            else
            {
                auto runningQuery = std::make_shared<RunningQuery>(q);
                runningQuery->exception = reg.error();
                reportResult(
                    runningQuery,
                    finished,
                    queries.size(),
                    failed,
                    [&]
                    {
                        if (q.expectedError && q.expectedError->code == runningQuery->exception->code())
                        {
                            return std::string{};
                        }
                        return fmt::format("unexpected registration error: {}", *runningQuery->exception);
                    });
            }
        }
    };

    while (not pending.empty() or not active.empty())
    {
        startMoreQueries();

        for (const auto& summary : submitter.finishedQueries())
        {
            auto it = active.find(summary.queryId);
            if (it == active.end())
            {
                throw TestException("received unregistered queryId: {}", summary.queryId);
            }

            auto& runningQuery = it->second;

            if (summary.currentStatus == QueryStatus::Failed)
            {
                runningQuery->exception = summary.runs.back().error;
                reportResult(
                    runningQuery,
                    finished,
                    queries.size(),
                    failed,
                    [&]
                    {
                        if (runningQuery->query.expectedError
                            and runningQuery->query.expectedError->code == runningQuery->exception->code())
                        {
                            return std::string{};
                        }
                        return fmt::format("unexpected runtime error: {}", *runningQuery->exception);
                    });
            }
            else
            {
                reportResult(
                    runningQuery,
                    finished,
                    queries.size(),
                    failed,
                    [&]
                    {
                        if (runningQuery->query.expectedError)
                        {
                            return fmt::format("expected error {} but query succeeded", runningQuery->query.expectedError->code);
                        }
                        runningQuery->querySummary = summary;
                        if (auto err = checkResult(*runningQuery))
                        {
                            return *err;
                        }
                        return std::string{};
                    });
            }
            active.erase(it);
        }
    }

    auto failedViews = failed | std::views::filter(std::not_fn(passes)) | std::views::transform([](auto& p) { return *p; });
    return {failedViews.begin(), failedViews.end()};
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
            {"query name", queryRan.query.resultFile().stem()},
            {"time", executionTimeInSeconds},
            {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
            {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
        });
    }
    return failedQueries;
}
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<Query>& queries, const Configuration::SingleNodeWorkerConfiguration& configuration, nlohmann::json& resultJson)
{
    LocalWorkerQuerySubmitter submitter(configuration);
    std::vector<std::shared_ptr<RunningQuery>> ranQueries;
    std::size_t queryFinishedCounter = 0;
    const auto totalQueries = queries.size();
    for (const auto& queryToRun : queries)
    {
        if (not queryToRun.queryPlan.has_value())
        {
            std::cout << "skip failing query: " << queryToRun.name << std::endl;
            continue;
        }

        const auto registrationResult = submitter.registerQuery(queryToRun.queryPlan.value());
        if (not registrationResult.has_value())
        {
            std::cout << "skip failing query: " << queryToRun.name << std::endl;
            continue;
        }
        auto queryId = registrationResult.value();

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

        auto errorMessage = checkResult(*ranQueries.back());
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
    const auto queryNameLength = runningQuery.query.name.size();
    const auto queryNumberAsString = std::to_string(runningQuery.query.queryIdInFile + 1);
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(queryCounter + 1);

    /// spd logger cannot handle multiline prints with proper color and pattern.
    /// And as this is only for test runs we use stdout here.
    std::cout << std::string(padSizeQueryCounter - queryCounterAsString.size(), ' ');
    std::cout << queryCounterAsString << "/" << totalQueries << " ";
    std::cout << runningQuery.query.name << ":" << std::string(padSizeQueryNumber - queryNumberLength, '0') << queryNumberAsString;
    std::cout << std::string(padSizeSuccess - (queryNameLength + padSizeQueryNumber), '.');
    if (runningQuery.passed)
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", queryPerformanceMessage);
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
    const std::vector<Query>& queries, uint64_t numConcurrentQueries, const Configuration::SingleNodeWorkerConfiguration& configuration)
{
    LocalWorkerQuerySubmitter submitter(configuration);
    return runQueries(queries, numConcurrentQueries, submitter);
}
std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<Query>& queries, uint64_t numConcurrentQueries, const std::string& serverURI)
{
    RemoteWorkerQuerySubmitter submitter(serverURI);
    return runQueries(queries, numConcurrentQueries, submitter);
}

}
