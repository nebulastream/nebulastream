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


#include <Phases/RuleBasedOptimizer.hpp>

#include <memory>
#include <utility>

#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/RuleManager.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Rules/Static/RedundantUnionRemovalRule.hpp>
#include <Rules/Static/StatisticOptimizationRule.hpp>
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <CollectionDomain.hpp>
#include <Metric.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <RequestStatisticStatement.hpp>

namespace NES
{

RuleBasedOptimizer::RuleBasedOptimizer(
    QueryOptimizerConfiguration defaultQueryOptimization, std::shared_ptr<const StatisticRetrievalService> statisticRetrievalService)
    : defaultQueryOptimization(std::move(defaultQueryOptimization))
{
    RuleManager<LogicalPlan> ruleManager;
    ruleManager.addRule(DecideJoinTypesRule{this->defaultQueryOptimization.joinStrategy});
    ruleManager.addRule(DecideMemoryLayoutRule{});
    ruleManager.addRule(RedundantUnionRemovalRule{});
    ruleManager.addRule(RedundantProjectionRemovalRule{});
    ruleManager.addRule(DecideFieldMappings{});
    ruleManager.addRule(DecideFieldOrder{});
    ruleManager.addRule(OriginIdInferenceRule{});
    ruleManager.addRule(FixedPlanStructureBarrier{});
    ruleManager.addRule(PredicatePushdownRule{});
    ruleManager.addRule(WatermarkAssignerPushdownRule{});
    ruleManager.addRule(ProjectionPushdownRule{});

    /// PoC: when a statistic retrieval service is wired in, add a rule that surfaces a (mock) statistic to the
    /// optimizer for each user query. The statement below selects which statistic to fetch; correctness is
    /// irrelevant here (the PoC store returns the constant 42) — it only demonstrates the control-plane value
    /// reaching the optimizer at plan-rewrite time.
    if (statisticRetrievalService != nullptr)
    {
        ruleManager.addRule(StatisticOptimizationRule{std::move(statisticRetrievalService)});
    }

    NES_DEBUG("rule based optimizers rule sequence: {}", ruleManager.explain(ExplainVerbosity::Debug));
    ruleSequence = ruleManager.getSequence();
}

LogicalPlan RuleBasedOptimizer::optimize(LogicalPlan plan) const
{
    for (const auto& rule : ruleSequence)
    {
        plan = rule.apply(std::move(plan));
    }
    return plan;
}

}
