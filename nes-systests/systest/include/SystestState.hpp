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

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <fmt/base.h>
#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum/magic_enum.hpp>
#include <SystestConfiguration.hpp>


namespace NES::Systest
{
struct LoadedQueryPlan;

struct ExpectedError
{
    ErrorCode code;
    std::optional<std::string> message;
};

class SystestRunner;

using TestName = std::string;
using TestGroup = std::string;

struct SystestField
{
    DataType type;
    std::string name;
    friend std::ostream& operator<<(std::ostream& os, const SystestField& field)
    {
        os << fmt::format("{} {}", magic_enum::enum_name(field.type.type), field.name);
        return os;
    }
    bool operator==(const SystestField& other) const = default;
    bool operator!=(const SystestField& other) const = default;
};

struct SystestQuery
{
    static std::filesystem::path resultFile(const std::filesystem::path& workingDir, std::string_view testName, uint64_t queryIdInTestFile);

    static std::filesystem::path sourceFile(const std::filesystem::path& workingDir, std::string_view testName, uint64_t sourceId);
    SystestQuery() = default;
    explicit SystestQuery(
        TestName testName,
        std::string queryDefinition,
        std::filesystem::path sqlLogicTestFile,
        std::expected<LogicalPlan, Exception> queryPlan,
        uint64_t queryIdInFile,
        std::filesystem::path workingDir,
        const Schema& sinkSchema,
        std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCount,
        std::optional<ExpectedError> expectedError);

    [[nodiscard]] std::filesystem::path resultFile() const;

    TestName testName;
    std::string queryDefinition;
    std::filesystem::path sqlLogicTestFile;
    std::expected<LogicalPlan, Exception> queryPlan;
    uint64_t queryIdInFile{};
    std::filesystem::path workingDir;
    Schema expectedSinkSchema;
    std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCount;
    std::optional<ExpectedError> expectedError;
};


struct RunningQuery
{
    SystestQuery query;
    QueryId queryId = INVALID_QUERY_ID;
    QuerySummary querySummary;
    std::optional<uint64_t> bytesProcessed{0};
    std::optional<uint64_t> tuplesProcessed{0};
    bool passed = false;
    std::optional<Exception> exception;

    std::chrono::duration<double> getElapsedTime() const;
    [[nodiscard]] std::string getThroughput() const;
};

/// Assures that the number of parsed queries matches the number of parsed results
class SystestQueryNumberAssigner
{
    static constexpr size_t INITIAL_QUERY_NUMBER = QueryId::INITIAL;

public:
    explicit SystestQueryNumberAssigner() = default;

    [[nodiscard]] size_t getNextQueryNumber()
    {
        if (currentQueryNumber != currentQueryResultNumber)
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        return currentQueryNumber++;
    }

    [[nodiscard]] size_t getNextQueryResultNumber()
    {
        if (currentQueryNumber != (currentQueryResultNumber + 1))
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        return currentQueryResultNumber++;
    }

    void skipQueryResultOfQueryWithExpectedError()
    {
        if (currentQueryNumber != (currentQueryResultNumber + 1))
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        ++currentQueryResultNumber;
    }

private:
    size_t currentQueryNumber = INITIAL_QUERY_NUMBER;
    size_t currentQueryResultNumber = INITIAL_QUERY_NUMBER;
};

struct TestFile
{
    explicit TestFile(const std::filesystem::path& file);
    explicit TestFile(const std::filesystem::path& file, std::unordered_set<uint64_t> onlyEnableQueriesWithTestQueryNumber);
    [[nodiscard]] std::string getLogFilePath() const;
    [[nodiscard]] TestName name() const { return file.stem().string(); }

    std::filesystem::path file;
    std::unordered_set<uint64_t> onlyEnableQueriesWithTestQueryNumber;
    std::vector<TestGroup> groups;
    std::vector<SystestQuery> queries;
};

/// intermediate representation storing all considered test files
using TestFileMap = std::unordered_map<std::filesystem::path, TestFile>;
using QueryResultMap = std::unordered_map<std::filesystem::path, std::vector<std::string>>;

/// Groups (global) variables used throughout the main function call of SystestStarter
/// Only selected classes may modify its internals
class SystestStarterGlobals
{
    class SystestBinder
    {
    public:
        SystestBinder() = default; /// Load query plan objects by parsing an SLT file for queries and lowering it
        /// Returns a triplet of the lowered query plan, the query name and the schema of the sink
        [[nodiscard]] std::vector<LoadedQueryPlan> loadFromSLTFile(
            SystestStarterGlobals& systestStarterGlobals, const std::filesystem::path& testFilePath, std::string_view testFileName);

    private:
        uint64_t sourceIndex = 0;
        uint64_t currentQueryNumber = 0;
        std::string currentTestFileName;
    };

public:
    SystestStarterGlobals() = default;
    explicit SystestStarterGlobals(std::filesystem::path workingDir, std::filesystem::path testDataDir, TestFileMap testFileMap)
        : workingDir(std::move(workingDir))
        , testDataDir(std::move(testDataDir))
        , testFileMap(std::move(testFileMap))
        , binder(SystestBinder{})
    {
    }

    [[nodiscard]] const std::filesystem::path& getWorkingDir() const { return workingDir; }
    [[nodiscard]] const std::filesystem::path& getTestDataDir() const { return testDataDir; }
    [[nodiscard]] const TestFileMap& getTestFileMap() const { return testFileMap; }
    [[nodiscard]] const QueryResultMap& getQueryResultMap() const { return queryResultMap; }

private:
    friend void loadQueriesFromTestFile(const TestFile& testfile, SystestStarterGlobals& systestStarterGlobals);
    friend std::vector<LoadedQueryPlan>
    loadFromSLTFile(SystestStarterGlobals& systestStarterGlobals, const std::filesystem::path& testFilePath, std::string_view testFileName);

    void addQueryResult(const std::string_view testFileName, const size_t queryResultNumber, std::vector<std::string> resultLines)
    {
        queryResultMap.emplace(SystestQuery::resultFile(workingDir, testFileName, queryResultNumber), std::move(resultLines));
    }
    void addQuery(
        TestName testName,
        std::string queryDefinition,
        std::filesystem::path sqlLogicTestFile,
        std::expected<LogicalPlan, Exception> queryPlan,
        const uint64_t queryIdInFile,
        std::filesystem::path workingDir,
        Schema sinkSchema,
        std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCount,
        std::optional<ExpectedError> expectedError)
    {
        if (const auto it = testFileMap.find(sqlLogicTestFile); it != testFileMap.end())
        {
            it->second.queries.emplace_back(
                std::move(testName),
                std::move(queryDefinition),
                std::move(sqlLogicTestFile),
                std::move(queryPlan),
                queryIdInFile,
                std::move(workingDir),
                std::move(sinkSchema),
                std::move(sourceNamesToFilepathAndCount),
                std::move(expectedError));
            return;
        }
        throw TestException("Tried to add query to testFile {}, which does not exist.", sqlLogicTestFile.string());
    }

    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    TestFileMap testFileMap;
    QueryResultMap queryResultMap;
    SystestBinder binder;
};
std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config);

/// returns a vector of queries to run derived for our testfilemap
std::vector<SystestQuery> loadQueries(SystestStarterGlobals& systestStarterGlobals);
}

FMT_OSTREAM(NES::Systest::SystestField);

template <>
struct fmt::formatter<NES::Systest::RunningQuery> : formatter<std::string>
{
    static constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    static auto format(const NES::Systest::RunningQuery& runningQuery, format_context& ctx) -> decltype(ctx.out())
    {
        return fmt::format_to(
            ctx.out(),
            "[{}, systest -t {}:{}]",
            runningQuery.query.testName,
            runningQuery.query.sqlLogicTestFile,
            runningQuery.query.queryIdInFile);
    }
};
