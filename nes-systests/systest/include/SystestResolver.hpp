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
#include <memory>
#include <vector>

#include <ErrorHandling.hpp>
#include <SystestQueryModel.hpp>

namespace NES::Systest
{

struct ResolvedRun
{
    std::vector<EnvironmentSpec> environments;
    std::vector<std::shared_ptr<const ResolvedCase>> cases;

    [[nodiscard]] const EnvironmentSpec& environment(EnvironmentId id) const;
    [[nodiscard]] const ResolvedCase& testCase(const TestCaseId& id) const;
    [[nodiscard]] std::vector<TestCaseId> ids() const;
};

std::expected<ResolvedRun, Exception>
resolveSystestFiles(std::vector<ParsedTestFile> files, const SystestClusterConfiguration& clusterConfiguration);

}
