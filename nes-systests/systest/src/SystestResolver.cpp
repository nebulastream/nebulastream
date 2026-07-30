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

#include <SystestResolver.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <SystestQueryModel.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
namespace
{

struct EnvironmentKey
{
    std::filesystem::path relativeTestFile;
    EffectiveConfiguration configuration;

    auto operator<=>(const EnvironmentKey&) const = default;
};

EffectiveConfiguration effectiveConfiguration(const ConfigurationOverride& configuration)
{
    EffectiveConfiguration result;
    result.values.assign(configuration.overrideParameters.begin(), configuration.overrideParameters.end());
    std::ranges::sort(result.values);
    return result;
}

EffectiveConfiguration globalConfigurationLineage(const SystestQuery& query, const EffectiveConfiguration& configuration)
{
    EffectiveConfiguration result;
    if (!query.parsedCase)
    {
        return result;
    }
    for (const auto& directive : query.parsedCase->configuration)
    {
        if (!directive.global)
        {
            continue;
        }
        const auto value = std::ranges::find(configuration.values, directive.key, &std::pair<std::string, std::string>::first);
        if (value != configuration.values.end())
        {
            result.values.push_back(*value);
        }
    }
    std::ranges::sort(result.values);
    return result;
}

bool configurationIsSubset(const EffectiveConfiguration& subset, const EffectiveConfiguration& configuration)
{
    return std::ranges::all_of(
        subset.values, [&](const auto& entry) { return std::ranges::find(configuration.values, entry) != configuration.values.end(); });
}

std::filesystem::path relativeTestFile(const std::filesystem::path& testFile, const std::filesystem::path& discoveryRoot)
{
    std::error_code error;
    auto relative = std::filesystem::relative(testFile, discoveryRoot, error);
    if (error || relative.empty())
    {
        return testFile.filename();
    }
    return relative.lexically_normal();
}

bool outputIsDiscarded(const SystestQuery& query)
{
    if (!query.planInfoOrException)
    {
        return false;
    }
    const auto sinks = getOperatorByType<SinkLogicalOperator>(query.planInfoOrException->queryPlan.getGlobalPlan());
    if (sinks.empty())
    {
        return false;
    }
    const auto sink = sinks.front().tryGetAs<SinkLogicalOperator>();
    return sink && sink.value()->getSinkDescriptor() && toUpperCase(sink.value()->getSinkDescriptor()->getSinkType()) == "VOID";
}

CaseAction actionFor(const SystestQuery& query)
{
    if (query.parsedCase)
    {
        return query.parsedCase->action;
    }
    if (query.differentialQueryPlan)
    {
        return DifferentialAction{.leftSql = query.queryDefinition, .rightSql = {}};
    }
    return QueryAction{.sql = query.queryDefinition, .kind = query.actualExplainOutput ? QueryKind::Explain : QueryKind::Execute};
}

CaseExpectation expectationFor(const SystestQuery& query, const CaseAction& action)
{
    if (const auto* expectedError = std::get_if<ExpectedError>(&query.expectedResultsOrExpectedError))
    {
        return ErrorExpectation{.code = expectedError->code, .message = expectedError->message};
    }
    if (std::holds_alternative<DifferentialAction>(action))
    {
        return DifferentialExpectation{};
    }

    const auto& expectedRows = std::get<std::vector<std::string>>(query.expectedResultsOrExpectedError);
    if (const auto* queryAction = std::get_if<QueryAction>(&action); queryAction && queryAction->kind == QueryKind::Explain)
    {
        return TextExpectation{.lines = expectedRows, .matching = TextMatchPolicy::Automatic};
    }

    RowsExpectation expectation{
        .rows = expectedRows,
        .comparison = ComparisonPolicy::UnorderedTypedRows,
        .schema = std::nullopt,
        .outputDiscarded = outputIsDiscarded(query)};
    if (query.planInfoOrException)
    {
        expectation.schema = query.planInfoOrException->sinkOutputSchema;
    }
    return expectation;
}

std::optional<CaseKey> dependencyFor(const SystestQuery& query, const std::filesystem::path& relativeFile)
{
    if (query.parsedCase && query.parsedCase->runAfter)
    {
        auto dependency = query.parsedCase->runAfter;
        dependency->relativeTestFile = relativeFile;
        return dependency;
    }
    if (!query.runAfter || query.runAfter->second.getRawValue() == 0)
    {
        return std::nullopt;
    }
    return CaseKey{.relativeTestFile = relativeFile, .queryNumber = query.runAfter->second};
}

}

const PreparedCase& PreparedCaseCatalog::at(const TestCaseId& id) const
{
    return cases.at(id);
}

const PreparedCase* PreparedCaseCatalog::find(const TestCaseId& id) const
{
    const auto found = cases.find(id);
    return found == cases.end() ? nullptr : &found->second;
}

std::vector<TestCaseId> PreparedCaseCatalog::ids() const
{
    return cases | std::views::keys | std::ranges::to<std::vector>();
}

void PreparedCaseCatalog::insert(TestCaseId id, PreparedCase preparedCase)
{
    cases.emplace(std::move(id), std::move(preparedCase));
}

const EnvironmentSpec& ResolvedRun::environment(const EnvironmentId id) const
{
    const auto found = std::ranges::find(environments, id, &EnvironmentSpec::id);
    if (found == environments.end())
    {
        throw TestException("Unknown systest environment {}", id.value);
    }
    return *found;
}

const ResolvedCase& ResolvedRun::testCase(const TestCaseId& id) const
{
    const auto found = std::ranges::find_if(cases, [&](const auto& testCase) { return testCase->id == id; });
    if (found == cases.end())
    {
        throw TestException("Unknown systest case {}:{}", id.source.relativeTestFile, id.source.queryNumber);
    }
    return **found;
}

std::expected<ResolvedRun, Exception> resolveSystestQueries(
    std::vector<SystestQuery> queries, const std::filesystem::path& discoveryRoot, const SystestClusterConfiguration& clusterConfiguration)
{
    std::ranges::sort(
        queries,
        [&](const SystestQuery& left, const SystestQuery& right)
        {
            return std::tuple{
                       relativeTestFile(left.testFilePath, discoveryRoot),
                       left.queryIdInFile,
                       effectiveConfiguration(left.configurationOverride)}
            < std::tuple{
                relativeTestFile(right.testFilePath, discoveryRoot),
                right.queryIdInFile,
                effectiveConfiguration(right.configurationOverride)};
        });

    struct SourceVariant
    {
        EffectiveConfiguration configuration;
        EffectiveConfiguration globalLineage;
        TestCaseId id;
    };

    auto preparedCases = std::make_shared<PreparedCaseCatalog>();
    std::map<CaseKey, uint32_t> nextVariant;
    std::map<std::pair<CaseKey, EffectiveConfiguration>, TestCaseId> idsBySourceAndConfiguration;
    std::map<CaseKey, std::vector<SourceVariant>> idsBySource;
    std::map<TestCaseId, EffectiveConfiguration> configurationById;

    struct PendingDependency
    {
        std::shared_ptr<ResolvedCase> testCase;
        EffectiveConfiguration configuration;
        EffectiveConfiguration globalLineage;
        std::optional<CaseKey> dependency;
    };

    std::vector<PendingDependency> pendingDependencies;
    std::vector<std::shared_ptr<ResolvedCase>> resolvedCases;

    for (auto& query : queries)
    {
        const auto relativeFile = relativeTestFile(query.testFilePath, discoveryRoot);
        const auto key = CaseKey{.relativeTestFile = relativeFile, .queryNumber = query.queryIdInFile};
        const auto variant = nextVariant[key]++;
        const auto id = TestCaseId{.source = key, .configurationVariant = variant};
        const auto configuration = effectiveConfiguration(query.configurationOverride);
        const auto globalLineage = globalConfigurationLineage(query, configuration);

        auto action = actionFor(query);
        auto expectation = expectationFor(query, action);
        auto origin = query.parsedCase ? query.parsedCase->source : Origin{.file = query.testFilePath, .firstLine = 0, .lastLine = 0};
        origin.file = query.testFilePath;
        auto resolvedCase = std::make_shared<ResolvedCase>(ResolvedCase{
            .id = id,
            .environment = {},
            .source = std::move(origin),
            .action = std::move(action),
            .expectation = std::move(expectation),
            .dependencies = {}});

        if (query.parsedCase)
        {
            query.parsedCase->key = key;
            query.parsedCase->source.file = query.testFilePath;
        }

        idsBySourceAndConfiguration.emplace(std::pair{key, configuration}, id);
        idsBySource[key].push_back(SourceVariant{.configuration = configuration, .globalLineage = globalLineage, .id = id});
        configurationById.emplace(id, configuration);
        pendingDependencies.push_back(PendingDependency{
            .testCase = resolvedCase,
            .configuration = configuration,
            .globalLineage = globalLineage,
            .dependency = dependencyFor(query, relativeFile)});
        preparedCases->insert(id, PreparedCase{.definition = resolvedCase, .query = std::move(query)});
        resolvedCases.push_back(std::move(resolvedCase));
    }

    for (auto& pending : pendingDependencies)
    {
        if (!pending.dependency)
        {
            continue;
        }
        const auto exactDependency = idsBySourceAndConfiguration.find(std::pair{*pending.dependency, pending.configuration});
        if (exactDependency != idsBySourceAndConfiguration.end())
        {
            pending.testCase->dependencies.push_back(exactDependency->second);
            continue;
        }

        const auto sourceDependencies = idsBySource.find(*pending.dependency);
        if (sourceDependencies == idsBySource.end())
        {
            return std::unexpected(TestException(
                "{}:{} has nonexistent dependency {}:{}",
                pending.testCase->id.source.relativeTestFile,
                pending.testCase->id.source.queryNumber,
                pending.dependency->relativeTestFile,
                pending.dependency->queryNumber));
        }

        auto lineageCandidates = sourceDependencies->second
            | std::views::filter([&](const SourceVariant& candidate)
                                 { return configurationIsSubset(candidate.globalLineage, pending.globalLineage); })
            | std::views::transform([](const SourceVariant& candidate) { return &candidate; }) | std::ranges::to<std::vector>();
        if (lineageCandidates.empty())
        {
            return std::unexpected(TestException(
                "{}:{} has no compatible global configuration lineage for dependency {}:{}",
                pending.testCase->id.source.relativeTestFile,
                pending.testCase->id.source.queryNumber,
                pending.dependency->relativeTestFile,
                pending.dependency->queryNumber));
        }

        size_t largestMatchingConfiguration = 0;
        for (const auto* candidate : lineageCandidates)
        {
            if (!configurationIsSubset(candidate->configuration, pending.configuration))
            {
                continue;
            }
            if (candidate->configuration.values.size() > largestMatchingConfiguration)
            {
                pending.testCase->dependencies.clear();
                largestMatchingConfiguration = candidate->configuration.values.size();
            }
            if (candidate->configuration.values.size() == largestMatchingConfiguration)
            {
                pending.testCase->dependencies.push_back(candidate->id);
            }
        }
        if (pending.testCase->dependencies.empty())
        {
            pending.testCase->dependencies = lineageCandidates
                | std::views::transform([](const SourceVariant* candidate) { return candidate->id; }) | std::ranges::to<std::vector>();
        }
    }

    std::map<TestCaseId, uint8_t> visitState;
    const auto visit = [&](const auto& self, const TestCaseId& id) -> bool
    {
        auto& state = visitState[id];
        if (state == 1)
        {
            return false;
        }
        if (state == 2)
        {
            return true;
        }
        state = 1;
        for (const auto& dependency : preparedCases->at(id).definition->dependencies)
        {
            if (!self(self, dependency))
            {
                return false;
            }
        }
        state = 2;
        return true;
    };
    for (const auto& id : preparedCases->ids())
    {
        if (!visit(visit, id))
        {
            return std::unexpected(TestException("Dependency cycle includes {}:{}", id.source.relativeTestFile, id.source.queryNumber));
        }
    }

    std::ranges::sort(resolvedCases, {}, [](const auto& testCase) { return testCase->id; });

    std::map<EnvironmentKey, EnvironmentId> latestEnvironment;
    std::map<EnvironmentId, size_t> environmentOrder;
    std::map<TestCaseId, EnvironmentId> assignedEnvironment;
    std::vector<EnvironmentSpec> environments;
    uint64_t nextEnvironment = 1;
    for (auto& testCase : resolvedCases)
    {
        size_t dependencyEnvironmentOrder = 0;
        for (const auto& dependency : testCase->dependencies)
        {
            const auto assigned = assignedEnvironment.find(dependency);
            if (assigned == assignedEnvironment.end())
            {
                return std::unexpected(TestException(
                    "Dependency {}:{} must precede {}:{}",
                    dependency.source.relativeTestFile,
                    dependency.source.queryNumber,
                    testCase->id.source.relativeTestFile,
                    testCase->id.source.queryNumber));
            }
            dependencyEnvironmentOrder = std::max(dependencyEnvironmentOrder, environmentOrder.at(assigned->second));
        }

        const auto environmentKey
            = EnvironmentKey{.relativeTestFile = testCase->id.source.relativeTestFile, .configuration = configurationById.at(testCase->id)};
        auto environment = latestEnvironment.find(environmentKey);
        if (environment == latestEnvironment.end()
            || (!testCase->dependencies.empty() && environmentOrder.at(environment->second) < dependencyEnvironmentOrder))
        {
            const auto environmentId = EnvironmentId{.value = nextEnvironment++};
            environment = latestEnvironment.insert_or_assign(environmentKey, environmentId).first;
            environmentOrder.emplace(environmentId, environments.size());
            auto setupStatements = std::vector<FixtureStatement>{};
            const auto& query = preparedCases->at(testCase->id).query;
            if (query.fixtureStatements)
            {
                setupStatements.assign(query.fixtureStatements->begin(), query.fixtureStatements->end());
            }
            environments.push_back(EnvironmentSpec{
                .id = environmentId,
                .setupStatements = std::move(setupStatements),
                .configuration = configurationById.at(testCase->id),
                .cluster = clusterConfiguration});
        }

        testCase->environment = environment->second;
        assignedEnvironment.emplace(testCase->id, environment->second);
    }

    std::vector<std::shared_ptr<const ResolvedCase>> immutableCases;
    immutableCases.reserve(resolvedCases.size());
    for (auto& testCase : resolvedCases)
    {
        immutableCases.push_back(std::move(testCase));
    }

    return ResolvedRun{
        .environments = std::move(environments), .cases = std::move(immutableCases), .preparedCases = std::move(preparedCases)};
}

}
