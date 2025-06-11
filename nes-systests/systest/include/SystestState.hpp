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

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SystestConfiguration.hpp>


namespace NES::Systest
{

struct ExpectedError
{
    ErrorCode code;
    std::optional<std::string> message;
};

class SystestBinder;
using TestName = std::string;
using TestGroup = std::string;

struct SystestField
{
    DataType type;
    std::string name;
    bool operator==(const SystestField& other) const { return type == other.type && name == other.name; }
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
    uint64_t queryIdInFile;
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
public:
    explicit SystestQueryNumberAssigner() : currentQueryNumber(0), currentQueryResultNumber(0) { }

    size_t getNextQueryNumber()
    {
        if (currentQueryNumber != currentQueryResultNumber)
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        return currentQueryNumber++;
    }

    size_t getNextQueryResultNumber()
    {
        if (currentQueryNumber != (currentQueryResultNumber + 1))
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        return currentQueryResultNumber++;
    }

private:
    size_t currentQueryNumber = 0;
    size_t currentQueryResultNumber = 0;
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
using TestFileMap = std::unordered_map<TestName, TestFile>;
using QueryResultMap = std::unordered_map<std::filesystem::path, std::vector<std::string>>;
std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config);

/// returns a vector of queries to run derived for our testfilemap
std::vector<SystestQuery> loadQueries(
    TestFileMap& testmap,
    const std::filesystem::path& workingDir,
    const std::filesystem::path& testDataDir,
    QueryResultMap& queryResultMap,
    Systest::SystestBinder& systestBinder);
}

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
            runningQuery.query.queryIdInFile + 1);
    }
};
