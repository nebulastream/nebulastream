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
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestQueryModel.hpp>
#include <SystestResolver.hpp>

namespace NES::Systest
{
namespace
{

class TemporaryDirectory
{
public:
    explicit TemporaryDirectory(const std::string_view name)
    {
        static std::atomic<uint64_t> sequence = 0;
        directory = std::filesystem::temp_directory_path()
            / (std::string{name} + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-"
               + std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(directory);
    }

    ~TemporaryDirectory()
    {
        std::error_code error;
        std::filesystem::remove_all(directory, error);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const { return directory; }

private:
    std::filesystem::path directory;
};

ParsedTestFile
parseTestFile(const std::filesystem::path& root, const std::filesystem::path& relativeTestFile, const std::string_view contents)
{
    const auto file = root / relativeTestFile;
    std::filesystem::create_directories(file.parent_path());
    std::ofstream(file) << contents;

    SystestParser parser;
    if (!parser.loadFile(file, relativeTestFile))
    {
        throw TestException("Could not load test input {}", file);
    }
    return parser.parse();
}

ConfigurationDirective configuration(const std::filesystem::path& file, std::string key, std::vector<std::string> values, const bool global)
{
    return ConfigurationDirective{
        .key = std::move(key),
        .values = std::move(values),
        .global = global,
        .source = Origin{.file = file, .firstLine = 1, .lastLine = 1}};
}

ParsedCase parsedCase(
    const std::filesystem::path& relativeTestFile,
    const uint64_t queryNumber,
    std::vector<ConfigurationDirective> configurations = {},
    const std::optional<uint64_t> dependency = std::nullopt)
{
    const auto key = CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = SystestQueryId{queryNumber}};
    return ParsedCase{
        .key = key,
        .source = Origin{.file = relativeTestFile, .firstLine = queryNumber, .lastLine = queryNumber},
        .action = QueryAction{.sql = "SELECT 1 INTO File();", .kind = QueryKind::Execute},
        .expectation = RowsExpectation{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows},
        .runAfter
        = dependency.transform([&](const uint64_t predecessor)
                               { return CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = SystestQueryId{predecessor}}; }),
        .configuration = std::move(configurations)};
}

ParsedTestFile parsedFile(std::filesystem::path relativeTestFile, std::vector<ParsedCase> cases)
{
    const auto sourceFile = std::filesystem::path{"/resolved-input"} / relativeTestFile;
    return ParsedTestFile{.file = sourceFile, .relativeTestFile = std::move(relativeTestFile), .fixtures = {}, .cases = std::move(cases)};
}

EffectiveConfiguration effectiveConfiguration(std::initializer_list<std::pair<std::string, std::string>> values)
{
    return EffectiveConfiguration{.values = values};
}

TestCaseId id(const std::filesystem::path& file, const uint64_t queryNumber, const uint32_t variant)
{
    return TestCaseId{
        .source = CaseKey{.relativeTestFile = file, .queryNumber = SystestQueryId{queryNumber}}, .configurationVariant = variant};
}

}

TEST(SystestRedesignTest, ParserProducesAuthoritativeFileModel)
{
    const TemporaryDirectory temporary{"systest-parser-authoritative"};
    const auto parsed = parseTestFile(
        temporary.path(),
        "suite/model.test",
        R"(# ignored
GLOBALCONFIGURATION worker.mode: [B, A]
CREATE LOGICAL SOURCE input(id UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1
2

SELECT id FROM input INTO File();
----
1
2

SEQUENTIAL_EXECUTION
CONFIGURATION worker.size: 8
SELECT id FROM input INTO File();
====
SELECT id + UINT64(0) AS id FROM input INTO File();

SEQUENTIAL_EXECUTION
EXPLAIN (LOGICAL) FORMAT TEXT SELECT id FROM input INTO File();
----
<REGEX>SOURCE\(INPUT\)</REGEX>
==END==
)");

    ASSERT_EQ(parsed.file, std::filesystem::weakly_canonical(temporary.path() / "suite/model.test"));
    EXPECT_EQ(parsed.relativeTestFile, "suite/model.test");
    ASSERT_EQ(parsed.fixtures.size(), 2);
    EXPECT_EQ(parsed.fixtures[0].sql, "CREATE LOGICAL SOURCE input(id UINT64 NOT NULL);");
    ASSERT_TRUE(parsed.fixtures[1].attachment);
    const auto* inlineData = std::get_if<InlineSourceData>(&*parsed.fixtures[1].attachment);
    ASSERT_NE(inlineData, nullptr);
    EXPECT_EQ(inlineData->rows, (std::vector<std::string>{"1", "2"}));
    EXPECT_EQ(parsed.fixtures[0].source.firstLine, 3);
    EXPECT_EQ(parsed.fixtures[1].source.firstLine, 4);
    EXPECT_EQ(parsed.fixtures[1].source.lastLine, 7);

    ASSERT_EQ(parsed.cases.size(), 3);
    const auto& normal = parsed.cases[0];
    const auto& differential = parsed.cases[1];
    const auto& explain = parsed.cases[2];

    EXPECT_EQ(normal.key, (CaseKey{.relativeTestFile = "suite/model.test", .queryNumber = SystestQueryId{1}}));
    EXPECT_EQ(normal.source.file, parsed.file);
    EXPECT_EQ(normal.source.firstLine, 9);
    EXPECT_EQ(normal.source.lastLine, 12);
    ASSERT_EQ(normal.configuration.size(), 1);
    EXPECT_TRUE(normal.configuration.front().global);
    EXPECT_EQ(normal.configuration.front().values, (std::vector<std::string>{"B", "A"}));
    EXPECT_TRUE(std::holds_alternative<QueryAction>(normal.action));
    EXPECT_EQ(std::get<QueryAction>(normal.action).kind, QueryKind::Execute);
    EXPECT_EQ(std::get<RowsExpectation>(normal.expectation).rows, (std::vector<std::string>{"1", "2"}));

    ASSERT_TRUE(differential.runAfter);
    EXPECT_EQ(*differential.runAfter, normal.key);
    ASSERT_TRUE(std::holds_alternative<DifferentialAction>(differential.action));
    EXPECT_EQ(std::get<DifferentialAction>(differential.action).leftSql, "SELECT id FROM input INTO File();");
    EXPECT_EQ(std::get<DifferentialAction>(differential.action).rightSql, "SELECT id + UINT64(0) AS id FROM input INTO File();");
    EXPECT_TRUE(std::holds_alternative<DifferentialExpectation>(differential.expectation));
    ASSERT_EQ(differential.configuration.size(), 2);
    EXPECT_TRUE(differential.configuration[0].global);
    EXPECT_FALSE(differential.configuration[1].global);
    EXPECT_EQ(differential.configuration[1].key, "worker.size");

    EXPECT_FALSE(explain.runAfter);
    ASSERT_TRUE(std::holds_alternative<QueryAction>(explain.action));
    EXPECT_EQ(std::get<QueryAction>(explain.action).kind, QueryKind::Explain);
    ASSERT_TRUE(std::holds_alternative<TextExpectation>(explain.expectation));
    EXPECT_EQ(std::get<TextExpectation>(explain.expectation).matching, TextMatchPolicy::Automatic);
    EXPECT_TRUE(explain.configuration.empty());
}

TEST(SystestRedesignTest, ResolverCreatesDeterministicProductsPrecedenceAndStableIds)
{
    const auto alpha = std::filesystem::path{"alpha/variants.test"};
    const auto zeta = std::filesystem::path{"zeta/single.test"};
    const auto alphaSource = std::filesystem::path{"/resolved-input"} / alpha;

    const auto makeAlpha = [&](std::vector<std::string> modes, std::vector<std::string> threads)
    {
        std::vector<ParsedCase> cases;
        cases.push_back(parsedCase(
            alpha,
            1,
            {configuration(alphaSource, "worker.mode", std::move(modes), true),
             configuration(alphaSource, "worker.threads", std::move(threads), false)}));
        cases.push_back(parsedCase(
            alpha,
            2,
            {configuration(alphaSource, "worker.mode", {"B", "A"}, true),
             configuration(alphaSource, "worker.mode", {"local", "local"}, false)}));
        return parsedFile(alpha, std::move(cases));
    };
    const auto makeZeta = [&] { return parsedFile(zeta, {parsedCase(zeta, 1)}); };

    auto first = resolveSystestFiles({makeZeta(), makeAlpha({"B", "A"}, {"2", "1"})}, {});
    auto second = resolveSystestFiles({makeAlpha({"A", "B"}, {"1", "2"}), makeZeta()}, {});

    ASSERT_TRUE(first) << first.error().what();
    ASSERT_TRUE(second) << second.error().what();
    ASSERT_EQ(first->cases.size(), 6);
    ASSERT_EQ(first->environments.size(), 6);
    ASSERT_EQ(second->cases.size(), 6);
    ASSERT_EQ(second->environments.size(), 6);
    ASSERT_EQ(first->ids(), second->ids());

    const std::vector<EffectiveConfiguration> expectedConfigurations{
        effectiveConfiguration({{"worker.mode", "A"}, {"worker.threads", "1"}}),
        effectiveConfiguration({{"worker.mode", "A"}, {"worker.threads", "2"}}),
        effectiveConfiguration({{"worker.mode", "B"}, {"worker.threads", "1"}}),
        effectiveConfiguration({{"worker.mode", "B"}, {"worker.threads", "2"}}),
        effectiveConfiguration({{"worker.mode", "local"}}),
        effectiveConfiguration({})};

    for (size_t index = 0; index < first->cases.size(); ++index)
    {
        EXPECT_EQ(first->environment(first->cases[index]->environment).configuration, expectedConfigurations[index]);
        EXPECT_EQ(second->environment(second->cases[index]->environment).configuration, expectedConfigurations[index]);
        EXPECT_EQ(first->cases[index]->environment, second->cases[index]->environment);
    }
    for (uint32_t variant = 0; variant < 4; ++variant)
    {
        EXPECT_EQ(first->cases[variant]->id, id(alpha, 1, variant));
    }
    EXPECT_EQ(first->cases[4]->id, id(alpha, 2, 0));
    EXPECT_EQ(first->cases[5]->id, id(zeta, 1, 0));
}

TEST(SystestRedesignTest, ResolverUsesExactThenGlobalLineageDependencyMatching)
{
    const auto file = std::filesystem::path{"dependencies/matching.test"};
    const auto source = std::filesystem::path{"/resolved-input"} / file;
    const auto productConfiguration = [&]
    {
        return std::vector{configuration(source, "worker.mode", {"B", "A"}, true), configuration(source, "worker.size", {"2", "1"}, false)};
    };

    std::vector<ParsedCase> cases;
    cases.push_back(parsedCase(file, 1, productConfiguration()));
    cases.push_back(parsedCase(file, 2, productConfiguration(), 1));
    cases.push_back(parsedCase(file, 3, {configuration(source, "worker.mode", {"B", "A"}, true)}, 2));

    auto resolved = resolveSystestFiles({parsedFile(file, std::move(cases))}, {});

    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->cases.size(), 10);
    for (uint32_t variant = 0; variant < 4; ++variant)
    {
        const auto& predecessor = resolved->testCase(id(file, 1, variant));
        const auto& exact = resolved->testCase(id(file, 2, variant));
        EXPECT_EQ(exact.dependencies, (std::vector<TestCaseId>{predecessor.id}));
        EXPECT_EQ(exact.environment, predecessor.environment);
    }

    const auto& globalA = resolved->testCase(id(file, 3, 0));
    const auto& globalB = resolved->testCase(id(file, 3, 1));
    EXPECT_EQ(globalA.dependencies, (std::vector<TestCaseId>{id(file, 2, 0), id(file, 2, 1)}));
    EXPECT_EQ(globalB.dependencies, (std::vector<TestCaseId>{id(file, 2, 2), id(file, 2, 3)}));
}

TEST(SystestRedesignTest, ResolverPreservesGlobalLineageWhenLocalConfigurationOverridesTheSameKey)
{
    const auto file = std::filesystem::path{"dependencies/local-override.test"};
    const auto source = std::filesystem::path{"/resolved-input"} / file;
    const auto global = [&] { return configuration(source, "worker.mode", {"B", "A"}, true); };

    std::vector<ParsedCase> cases;
    cases.push_back(parsedCase(file, 1, {global()}));
    cases.push_back(parsedCase(file, 2, {global(), configuration(source, "worker.mode", {"local"}, false)}, 1));
    cases.push_back(parsedCase(file, 3, {global()}, 2));

    auto resolved = resolveSystestFiles({parsedFile(file, std::move(cases))}, {});

    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->cases.size(), 5);
    EXPECT_EQ(resolved->testCase(id(file, 2, 0)).dependencies, (std::vector<TestCaseId>{id(file, 1, 0), id(file, 1, 1)}));
    EXPECT_EQ(resolved->testCase(id(file, 3, 0)).dependencies, (std::vector<TestCaseId>{id(file, 2, 0)}));
    EXPECT_EQ(resolved->testCase(id(file, 3, 1)).dependencies, (std::vector<TestCaseId>{id(file, 2, 0)}));
}

TEST(SystestRedesignTest, ResolverMatchesUnconfiguredSequentialExplainToConfiguredPredecessors)
{
    const auto file = std::filesystem::path{"dependencies/explain.test"};
    const auto source = std::filesystem::path{"/resolved-input"} / file;
    auto explain = parsedCase(file, 2, {}, 1);
    explain.action = QueryAction{.sql = "EXPLAIN (LOGICAL) SELECT 1 INTO File();", .kind = QueryKind::Explain};
    explain.expectation = TextExpectation{};

    auto resolved = resolveSystestFiles(
        {parsedFile(file, {parsedCase(file, 1, {configuration(source, "worker.mode", {"B", "A"}, true)}), std::move(explain)})}, {});

    ASSERT_TRUE(resolved) << resolved.error().what();
    EXPECT_EQ(resolved->testCase(id(file, 2, 0)).dependencies, (std::vector<TestCaseId>{id(file, 1, 0), id(file, 1, 1)}));
}

TEST(SystestRedesignTest, ResolverAdvancesEnvironmentEpochAcrossConfigurationChanges)
{
    const auto file = std::filesystem::path{"dependencies/epochs.test"};
    const auto source = std::filesystem::path{"/resolved-input"} / file;
    const auto base = [&] { return configuration(source, "worker.mode", {"A"}, true); };

    std::vector<ParsedCase> cases;
    cases.push_back(parsedCase(file, 1, {base()}));
    cases.push_back(parsedCase(file, 2, {base(), configuration(source, "worker.size", {"8"}, false)}, 1));
    cases.push_back(parsedCase(file, 3, {base()}, 2));

    auto resolved = resolveSystestFiles({parsedFile(file, std::move(cases))}, {});

    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->cases.size(), 3);
    ASSERT_EQ(resolved->environments.size(), 3);
    EXPECT_EQ(resolved->cases[1]->dependencies, (std::vector<TestCaseId>{resolved->cases[0]->id}));
    EXPECT_EQ(resolved->cases[2]->dependencies, (std::vector<TestCaseId>{resolved->cases[1]->id}));
    EXPECT_EQ(resolved->cases[0]->environment, EnvironmentId{.value = 1});
    EXPECT_EQ(resolved->cases[1]->environment, EnvironmentId{.value = 2});
    EXPECT_EQ(resolved->cases[2]->environment, EnvironmentId{.value = 3});
    EXPECT_EQ(resolved->environments[0].configuration, resolved->environments[2].configuration);
}

TEST(SystestRedesignTest, ResolverRejectsDuplicateCaseKeysWithDisjointConfigurations)
{
    const auto file = std::filesystem::path{"dependencies/duplicate.test"};
    const auto source = std::filesystem::path{"/resolved-input"} / file;
    auto resolved = resolveSystestFiles(
        {parsedFile(
            file,
            {parsedCase(file, 1, {configuration(source, "worker.mode", {"A"}, false)}),
             parsedCase(file, 1, {configuration(source, "worker.mode", {"B"}, false)})})},
        {});

    ASSERT_FALSE(resolved);
    EXPECT_TRUE(std::string{resolved.error().what()}.contains("Duplicate systest case"));
}

TEST(SystestRedesignTest, ResolverRejectsMissingAndCyclicDependencies)
{
    const auto file = std::filesystem::path{"dependencies/invalid.test"};

    auto missing = resolveSystestFiles({parsedFile(file, {parsedCase(file, 1, {}, 9)})}, {});
    ASSERT_FALSE(missing);
    EXPECT_TRUE(std::string{missing.error().what()}.contains("nonexistent dependency"));

    auto first = parsedCase(file, 1, {}, 2);
    auto second = parsedCase(file, 2, {}, 1);
    auto cyclic = resolveSystestFiles({parsedFile(file, {std::move(first), std::move(second)})}, {});
    ASSERT_FALSE(cyclic);
    EXPECT_TRUE(std::string{cyclic.error().what()}.contains("Dependency cycle includes"));
}

}
