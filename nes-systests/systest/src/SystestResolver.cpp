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
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <SystestQueryModel.hpp>

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

struct ConfigurationVariant
{
    EffectiveConfiguration configuration;
    std::vector<EffectiveConfiguration> globalLineages;
};

EffectiveConfiguration makeConfiguration(const std::map<std::string, std::string>& values)
{
    return EffectiveConfiguration{.values = {values.begin(), values.end()}};
}

std::vector<ConfigurationVariant> expandConfiguration(const ParsedCase& parsedCase)
{
    struct Product
    {
        std::map<std::string, std::string> configuration;
        std::map<std::string, std::string> globalLineage;

        bool operator==(const Product&) const = default;
    };

    std::vector<Product> products(1);
    for (const auto& directive : parsedCase.configuration)
    {
        std::vector<Product> expanded;
        expanded.reserve(products.size() * directive.values.size());
        for (const auto& product : products)
        {
            for (const auto& value : directive.values)
            {
                auto next = product;
                next.configuration[directive.key] = value;
                if (directive.global)
                {
                    next.globalLineage[directive.key] = value;
                }
                if (std::ranges::find(expanded, next) == expanded.end())
                {
                    expanded.push_back(std::move(next));
                }
            }
        }
        products = std::move(expanded);
    }

    std::map<std::map<std::string, std::string>, std::set<std::map<std::string, std::string>>> lineagesByConfiguration;
    for (const auto& product : products)
    {
        lineagesByConfiguration[product.configuration].insert(product.globalLineage);
    }

    std::vector<ConfigurationVariant> variants;
    variants.reserve(lineagesByConfiguration.size());
    for (const auto& [configuration, lineages] : lineagesByConfiguration)
    {
        variants.push_back(ConfigurationVariant{
            .configuration = makeConfiguration(configuration),
            .globalLineages = lineages | std::views::transform(makeConfiguration) | std::ranges::to<std::vector>()});
    }
    std::ranges::sort(variants, {}, &ConfigurationVariant::configuration);
    return variants;
}

bool configurationIsSubset(const EffectiveConfiguration& subset, const EffectiveConfiguration& configuration)
{
    return std::ranges::all_of(
        subset.values, [&](const auto& entry) { return std::ranges::find(configuration.values, entry) != configuration.values.end(); });
}

bool configurationsAreCompatible(const EffectiveConfiguration& first, const EffectiveConfiguration& second)
{
    return std::ranges::all_of(
        first.values,
        [&](const auto& entry)
        {
            const auto matchingKey = std::ranges::find(second.values, entry.first, &std::pair<std::string, std::string>::first);
            return matchingKey == second.values.end() || matchingKey->second == entry.second;
        });
}

bool lineagesAreCompatible(const std::vector<EffectiveConfiguration>& first, const std::vector<EffectiveConfiguration>& second)
{
    return std::ranges::any_of(
        first,
        [&](const auto& firstLineage)
        {
            return std::ranges::any_of(
                second, [&](const auto& secondLineage) { return configurationsAreCompatible(firstLineage, secondLineage); });
        });
}

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

std::vector<TestCaseId> ResolvedRun::ids() const
{
    return cases | std::views::transform([](const auto& testCase) { return testCase->id; }) | std::ranges::to<std::vector>();
}

std::expected<ResolvedRun, Exception>
resolveSystestFiles(std::vector<ParsedTestFile> files, const SystestClusterConfiguration& clusterConfiguration)
{
    std::ranges::sort(files, {}, &ParsedTestFile::relativeTestFile);

    std::map<std::filesystem::path, std::vector<FixtureStatement>> fixturesByFile;
    std::set<std::filesystem::path> filesWithCases;

    struct SourceVariant
    {
        EffectiveConfiguration configuration;
        std::vector<EffectiveConfiguration> globalLineages;
        TestCaseId id;
    };

    struct PendingDependency
    {
        std::shared_ptr<ResolvedCase> testCase;
        EffectiveConfiguration configuration;
        std::vector<EffectiveConfiguration> globalLineages;
        std::optional<CaseKey> dependency;
    };

    std::set<CaseKey> sourceCases;
    std::map<std::pair<CaseKey, EffectiveConfiguration>, TestCaseId> idsBySourceAndConfiguration;
    std::map<CaseKey, std::vector<SourceVariant>> idsBySource;
    std::map<TestCaseId, EffectiveConfiguration> configurationById;
    std::vector<PendingDependency> pendingDependencies;
    std::vector<std::shared_ptr<ResolvedCase>> resolvedCases;

    for (auto& file : files)
    {
        file.relativeTestFile = file.relativeTestFile.lexically_normal();
        if (!fixturesByFile.emplace(file.relativeTestFile, file.fixtures).second)
        {
            return std::unexpected(TestException("Duplicate parsed systest file {}", file.relativeTestFile));
        }
        std::ranges::sort(file.cases, {}, [](const ParsedCase& testCase) { return testCase.key.queryNumber; });
        if (!file.cases.empty())
        {
            filesWithCases.insert(file.relativeTestFile);
        }
        for (auto& parsedCase : file.cases)
        {
            parsedCase.key.relativeTestFile = file.relativeTestFile;
            parsedCase.source.file = file.file;
            if (parsedCase.runAfter)
            {
                parsedCase.runAfter->relativeTestFile = file.relativeTestFile;
            }
            if (!sourceCases.insert(parsedCase.key).second)
            {
                return std::unexpected(
                    TestException("Duplicate systest case {}:{}", parsedCase.key.relativeTestFile, parsedCase.key.queryNumber));
            }

            const auto configurations = expandConfiguration(parsedCase);
            for (uint32_t variant = 0; variant < configurations.size(); ++variant)
            {
                const auto id = TestCaseId{.source = parsedCase.key, .configurationVariant = variant};
                const auto& expanded = configurations[variant];
                auto resolvedCase = std::make_shared<ResolvedCase>(ResolvedCase{
                    .id = id,
                    .environment = {},
                    .source = parsedCase.source,
                    .action = parsedCase.action,
                    .expectation = parsedCase.expectation,
                    .dependencies = {}});

                if (!idsBySourceAndConfiguration.emplace(std::pair{parsedCase.key, expanded.configuration}, id).second)
                {
                    return std::unexpected(TestException(
                        "Duplicate systest case {}:{} configuration", parsedCase.key.relativeTestFile, parsedCase.key.queryNumber));
                }
                idsBySource[parsedCase.key].push_back(
                    SourceVariant{.configuration = expanded.configuration, .globalLineages = expanded.globalLineages, .id = id});
                configurationById.emplace(id, expanded.configuration);
                pendingDependencies.push_back(PendingDependency{
                    .testCase = resolvedCase,
                    .configuration = expanded.configuration,
                    .globalLineages = expanded.globalLineages,
                    .dependency = parsedCase.runAfter});
                resolvedCases.push_back(std::move(resolvedCase));
            }
        }
    }

    for (auto& pending : pendingDependencies)
    {
        if (!pending.dependency)
        {
            continue;
        }
        if (const auto exact = idsBySourceAndConfiguration.find(std::pair{*pending.dependency, pending.configuration});
            exact != idsBySourceAndConfiguration.end())
        {
            pending.testCase->dependencies.push_back(exact->second);
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
                                 { return lineagesAreCompatible(candidate.globalLineages, pending.globalLineages); })
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
        std::ranges::sort(pending.testCase->dependencies);
        const auto duplicate = std::ranges::unique(pending.testCase->dependencies);
        pending.testCase->dependencies.erase(duplicate.begin(), duplicate.end());
    }

    std::map<TestCaseId, std::shared_ptr<ResolvedCase>> casesById;
    for (const auto& testCase : resolvedCases)
    {
        casesById.emplace(testCase->id, testCase);
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
        for (const auto& dependency : casesById.at(id)->dependencies)
        {
            if (!self(self, dependency))
            {
                return false;
            }
        }
        state = 2;
        return true;
    };
    for (const auto& id : casesById | std::views::keys)
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
            environments.push_back(EnvironmentSpec{
                .id = environmentId,
                .relativeTestFile = testCase->id.source.relativeTestFile,
                .setupStatements = fixturesByFile.at(testCase->id.source.relativeTestFile),
                .configuration = configurationById.at(testCase->id),
                .cluster = clusterConfiguration});
        }
        testCase->environment = environment->second;
        assignedEnvironment.emplace(testCase->id, environment->second);
    }
    for (const auto& [relativeTestFile, fixtures] : fixturesByFile)
    {
        if (filesWithCases.contains(relativeTestFile) || fixtures.empty())
        {
            continue;
        }
        environments.push_back(EnvironmentSpec{
            .id = EnvironmentId{.value = nextEnvironment++},
            .relativeTestFile = relativeTestFile,
            .setupStatements = fixtures,
            .configuration = {},
            .cluster = clusterConfiguration});
    }

    std::vector<std::shared_ptr<const ResolvedCase>> immutableCases;
    immutableCases.reserve(resolvedCases.size());
    for (auto& testCase : resolvedCases)
    {
        immutableCases.push_back(std::move(testCase));
    }
    return ResolvedRun{.environments = std::move(environments), .cases = std::move(immutableCases)};
}

}
