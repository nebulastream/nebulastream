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
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/RuleManager.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/AnonymousSourceBindingRule.hpp>
#include <Rules/Semantic/CalcTargetOrderRule.hpp>
#include <Rules/Semantic/InferModelResolutionRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Rules/Static/OriginIdInferenceRule.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Rules/Static/RedundantUnionRemovalRule.hpp>
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

RuleBasedOptimizer::RuleBasedOptimizer(
    QueryOptimizerConfiguration defaultQueryOptimization,
    std::shared_ptr<const SourceCatalog> sourceCatalog,
    std::shared_ptr<const SinkCatalog> sinkCatalog,
    std::shared_ptr<const ModelCatalog> modelCatalog)
    : defaultQueryOptimization(std::move(defaultQueryOptimization))
    , sourceCatalog(std::move(sourceCatalog))
    , sinkCatalog(std::move(sinkCatalog))
    , modelCatalog(std::move(modelCatalog))
{
    RuleManager<LogicalPlan> ruleManager;

    ruleManager.addRule(AnonymousSinkBindingRule{this->sinkCatalog});
    ruleManager.addRule(SinkBindingRule{this->sinkCatalog});
    ruleManager.addRule(AnonymousSourceBindingRule{this->sourceCatalog});
    ruleManager.addRule(LogicalSourceExpansionRule{this->sourceCatalog});
    ruleManager.addRule(InferModelResolutionRule{this->modelCatalog});
    ruleManager.addRule(TypeInferenceRule{});
    ruleManager.addRule(CalcTargetOrderRule{});

    ruleManager.addRule(SemanticAnalysisBarrier{});

    ruleManager.addRule(PredicatePushdownRule{});
    ruleManager.addRule(WatermarkAssignerPushdownRule{});
    ruleManager.addRule(ProjectionPushdownRule{});
    ruleManager.addRule(RedundantUnionRemovalRule{});
    ruleManager.addRule(RedundantProjectionRemovalRule{});

    ruleManager.addRule(FixedPlanStructureBarrier{});

    ruleManager.addRule(DecideJoinTypesRule{this->defaultQueryOptimization.joinStrategy});
    ruleManager.addRule(DecideMemoryLayoutRule{});
    ruleManager.addRule(DecideFieldMappings{});
    ruleManager.addRule(DecideFieldOrder{});
    ruleManager.addRule(OriginIdInferenceRule{});

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
