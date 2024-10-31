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
#include <filesystem>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SystestConfiguration.hpp>
#include <SystestRunner.hpp>


namespace NES::Systest
{
using TestName = std::string;
using TestGroup = std::string;

struct Query
{
    Query() = default;
    explicit Query(TestName name, std::filesystem::path sqlLogicTestFile, DecomposedQueryPlanPtr queryPlan, uint64_t queryIdInFile)
        : name(std::move(name)), sqlLogicTestFile(std::move(sqlLogicTestFile)), queryPlan(queryPlan), queryIdInFile(queryIdInFile) {};

    [[nodiscard]] inline std::filesystem::path resultFile() const
    {
        return std::filesystem::path(fmt::format("{}/nes-systests/result/{}_{}.csv", PATH_TO_BINARY_DIR, name, queryIdInFile.value()));
    }

    friend std::ostream& operator<<(std::ostream& os, const Query& query)
    {
        return os << "Query: " << query.name << " " << query.sqlLogicTestFile << " " << query.queryIdInFile.value();
    }

    TestName name;
    std::filesystem::path sqlLogicTestFile;
    DecomposedQueryPlanPtr queryPlan;
    std::optional<uint64_t> queryIdInFile;
};

struct RunningQuery
{
    Query query;
    QueryId queryId;
};

class TestFile
{
public:
    explicit TestFile(std::filesystem::path file) : file(std::move(file)), groups(readGroups()) {};
    /// Load a testfile but consider only queries with a specific id (location in test file)
    explicit TestFile(std::filesystem::path file, std::vector<uint64_t> onlyEnableQueriesWithId)
        : file(std::move(file)), onlyEnableQueriesWithId(std::move(onlyEnableQueriesWithId)), groups(readGroups()) {};

    [[nodiscard]] TestName name() { return file.stem().string(); }

    const std::filesystem::path file;
    const std::vector<uint64_t> onlyEnableQueriesWithId{};
    const std::vector<TestGroup> groups;

    std::vector<Query> queries;

private:
    std::vector<TestGroup> readGroups();
};

/// intermediate representation storing all considered test files
using TestFileMap = std::unordered_map<TestName, TestFile>;
std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const Configuration::SystestConfiguration& config);

/// returns a vector of queries to run derived for our testfilemap
std::vector<Query> loadQueries(TestFileMap&& testmap, const std::filesystem::path& resultDir);
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
            runningQuery.query.queryIdInFile.value() + 1);
    }
};
