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

#include <LegacyOptimizer/LegacyOptimizer.hpp>

#include <memory>

#include <LegacyOptimizer/Phases/LogicalSourceExpansionRule.hpp>
#include <LegacyOptimizer/Phases/OriginIdInferencePhase.hpp>
#include <LegacyOptimizer/Phases/SourceInferencePhase.hpp>
#include <LegacyOptimizer/Phases/TypeInferencePhase.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{

LegacyOptimizer::OptimizedLogicalPlan LegacyOptimizer::optimize(LogicalPlan inputPlan, const std::shared_ptr<SourceCatalog>& sourceCatalog)
{
    auto newPlan = std::move(inputPlan);
    const auto sourceInference = SourceInferencePhase{sourceCatalog};
    const auto logicalSourceExpansionRule = LogicalSourceExpansionRule(sourceCatalog);
    constexpr auto typeInference = TypeInferencePhase{};
    constexpr auto originIdInferencePhase = OriginIdInferencePhase{};

    sourceInference.apply(newPlan);
    logicalSourceExpansionRule.apply(newPlan);
    typeInference.apply(newPlan);

    originIdInferencePhase.apply(newPlan);
    typeInference.apply(newPlan);
    return OptimizedLogicalPlan{std::move(newPlan)};
}
}
