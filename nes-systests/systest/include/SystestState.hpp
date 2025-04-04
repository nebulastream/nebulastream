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
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SystestConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestRunner.hpp>


namespace NES::Systest
{
using TestName = std::string;
using TestGroup = std::string;

struct Query
{
    static std::filesystem::path
    resultFile(const std::filesystem::path& workingDir, std::string_view testName, const uint64_t queryIdInTestFile)
    {
        auto resultDir = workingDir / "results";
        if (not is_directory(resultDir))
        {
            create_directory(resultDir);
            std::cout << "Created working directory: file://" << resultDir.string() << "\n";
        }

        return resultDir / std::filesystem::path(fmt::format("{}_{}.csv", testName, queryIdInTestFile));
    }

    static std::filesystem::path sourceFile(const std::filesystem::path& workingDir, std::string_view testName, const uint64_t sourceId)
    {
        auto sourceDir = workingDir / "sources";
        if (not is_directory(sourceDir))
        {
            create_directory(sourceDir);
            std::cout << "Created working directory: file://" << sourceDir.string() << "\n";
        }

        return sourceDir / std::filesystem::path(fmt::format("{}_{}.csv", testName, sourceId));
    }

    Query() = default;
    explicit Query(
        TestName name,
        std::string queryDefinition,
        std::filesystem::path sqlLogicTestFile,
        std::shared_ptr<DecomposedQueryPlan> queryPlan,
        const uint64_t queryIdInFile,
        std::filesystem::path workingDir,
        SystestParser::Schema sinkSchema)
        : name(std::move(name))
        , queryDefinition(std::move(queryDefinition))
        , sqlLogicTestFile(std::move(sqlLogicTestFile))
        , queryPlan(std::move(queryPlan))
        , queryIdInFile(queryIdInFile)
        , workingDir(std::move(workingDir))
        , expectedSinkSchema(std::move(sinkSchema))
    {
    }

    [[nodiscard]] std::filesystem::path resultFile() const { return resultFile(workingDir, name, queryIdInFile); }

    TestName name;
    std::string queryDefinition;
    std::filesystem::path sqlLogicTestFile;
    std::shared_ptr<DecomposedQueryPlan> queryPlan;
    uint64_t queryIdInFile;
    std::filesystem::path workingDir;
    SystestParser::Schema expectedSinkSchema;
};

struct QueryExecutionInfo
{
    explicit QueryExecutionInfo(std::chrono::time_point<std::chrono::high_resolution_clock> startTime)
        : passed(false), startTime(std::move(startTime))
    {
    }
    bool passed;
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
    std::chrono::time_point<std::chrono::high_resolution_clock> endTime;
};

struct RunningQuery
{
    Query query;
    QueryId queryId = INVALID_QUERY_ID;
    QueryExecutionInfo queryExecutionInfo;
};

struct TestFile
{
    explicit TestFile(std::filesystem::path file);

    /// Load a testfile but consider only queries with a specific query number (location in test file)
    explicit TestFile(std::filesystem::path file, std::vector<uint64_t> onlyEnableQueriesWithTestQueryNumber);

    /// Returns the correct logging path, depending on if the HOST_NEBULASTREAM_ROOT environment variable is set
    [[nodiscard]] std::string getLogFilePath() const
    {
        if (const char* hostNebulaStreamRoot = std::getenv("HOST_NEBULASTREAM_ROOT"))
        {
            /// Set the correct logging path when using docker
            /// To do this, we need to combine the path to the host's nebula-stream root with the path to the file.
            /// We assume that the last part of hostNebulaStreamRoot is the common folder name.
            auto commonFolder = std::filesystem::path(hostNebulaStreamRoot).filename();

            /// Find the position of the common folder in the file path
            auto filePathIter = file.begin();
            if (const auto it = std::ranges::find(file, commonFolder); it != file.end())
            {
                filePathIter = std::next(it);
            }

            /// Combining the path to the host's nebula-stream root with the path to the file
            std::filesystem::path resultPath(hostNebulaStreamRoot);
            for (; filePathIter != file.end(); ++filePathIter)
            {
                resultPath /= *filePathIter;
            }

            return resultPath.string();
        }

        /// Set the correct logging path without docker
        return std::filesystem::path(file);
    }

    [[nodiscard]] TestName name() const { return file.stem().string(); }

    std::filesystem::path file;
    std::vector<uint64_t> onlyEnableQueriesWithTestQueryNumber;
    std::vector<TestGroup> groups;

    std::vector<Query> queries;
};

/// intermediate representation storing all considered test files
using TestFileMap = std::unordered_map<TestName, TestFile>;
std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config);

/// returns a vector of queries to run derived for our testfilemap
std::vector<Query> loadQueries(TestFileMap& testmap, const std::filesystem::path& workingDir, const std::filesystem::path& testDataDir);
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
            runningQuery.query.name,
            runningQuery.query.sqlLogicTestFile,
            runningQuery.query.queryIdInFile + 1);
    }
};
