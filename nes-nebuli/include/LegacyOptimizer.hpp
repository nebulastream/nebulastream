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
#include <memory>
#include <utility>

#include <Plans/LogicalPlan.hpp>

namespace NES
{
class SourceCatalog;
}
namespace NES::CLI
{
class LegacyOptimizer
{
    std::shared_ptr<const SourceCatalog> sourceCatalog;

public:
    [[nodiscard]] LogicalPlan optimize(const LogicalPlan& plan) const;
    LegacyOptimizer() = default;
    explicit LegacyOptimizer(std::shared_ptr<const SourceCatalog> sourceCatalog) : sourceCatalog(std::move(sourceCatalog)) { }
};
}
