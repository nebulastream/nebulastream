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

#include <LegacyOptimizer.hpp>

#include <LegacyOptimizer/LogicalSourceExpansionRule.hpp>
#include <LegacyOptimizer/OriginIdInferencePhase.hpp>
#include <LegacyOptimizer/RedundantProjectionRemovalRule.hpp>
#include <LegacyOptimizer/RedundantUnionRemovalRule.hpp>
#include <LegacyOptimizer/SinkBindingRule.hpp>
#include <LegacyOptimizer/SourceInferencePhase.hpp>
#include <LegacyOptimizer/TypeInferencePhase.hpp>

namespace NES::CLI
{
LogicalPlan LegacyOptimizer::optimize(const LogicalPlan& plan) const
{
    auto newPlan = LogicalPlan{plan};
    const auto sinkBindingRule = NES::LegacyOptimizer::SinkBindingRule{sinkCatalog};
    const auto sourceInference = NES::LegacyOptimizer::SourceInferencePhase{sourceCatalog};
    const auto logicalSourceExpansionRule = NES::LegacyOptimizer::LogicalSourceExpansionRule{sourceCatalog};
    constexpr auto typeInference = NES::LegacyOptimizer::TypeInferencePhase{};
    constexpr auto originIdInferencePhase = NES::LegacyOptimizer::OriginIdInferencePhase{};
    constexpr auto redundantUnionRemovalRule = NES::LegacyOptimizer::RedundantUnionRemovalRule{};
    constexpr auto redundantProjectionRemovalRule = NES::LegacyOptimizer::RedundantProjectionRemovalRule{};

    sinkBindingRule.apply(newPlan);
    sourceInference.apply(newPlan);
    logicalSourceExpansionRule.apply(newPlan);
    NES_INFO("After Source Expansion:\n{}", newPlan);
    redundantUnionRemovalRule.apply(newPlan);
    NES_INFO("After Redundant Union Removal:\n{}", newPlan);
    typeInference.apply(newPlan);

    redundantProjectionRemovalRule.apply(newPlan);
    NES_INFO("After Redundant Projection Removal:\n{}", newPlan);

    originIdInferencePhase.apply(newPlan);
    typeInference.apply(newPlan);
    return newPlan;
}
}
