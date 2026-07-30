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

#include <expected>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include <ErrorHandling.hpp>
#include <SystestQueryModel.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

struct PreparedCase
{
    std::shared_ptr<const ResolvedCase> definition;
    SystestQuery query;
};

class PreparedCaseCatalog
{
public:
    [[nodiscard]] const PreparedCase& at(const TestCaseId& id) const;
    [[nodiscard]] const PreparedCase* find(const TestCaseId& id) const;
    [[nodiscard]] std::vector<TestCaseId> ids() const;
    void insert(TestCaseId id, PreparedCase preparedCase);

private:
    std::map<TestCaseId, PreparedCase> cases;
};

struct ResolvedRun
{
    std::vector<EnvironmentSpec> environments;
    std::vector<std::shared_ptr<const ResolvedCase>> cases;
    std::shared_ptr<const PreparedCaseCatalog> preparedCases;

    [[nodiscard]] const EnvironmentSpec& environment(EnvironmentId id) const;
    [[nodiscard]] const ResolvedCase& testCase(const TestCaseId& id) const;
};

std::expected<ResolvedRun, Exception> resolveSystestQueries(
    std::vector<SystestQuery> queries, const std::filesystem::path& discoveryRoot, const SystestClusterConfiguration& clusterConfiguration);

}
