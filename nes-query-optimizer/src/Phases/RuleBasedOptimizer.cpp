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
#include <Rules/RuleManager.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <ModelCatalog.hpp>
#include <PlanRuleRegistry.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include "ErrorHandling.hpp"

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


    const PlanRuleRegistryArguments arguments{
        .defaultQueryOptimization = this->defaultQueryOptimization,
        .sourceCatalog = this->sourceCatalog,
        .sinkCatalog = this->sinkCatalog,
        .modelCatalog = this->modelCatalog,
    };

    for (auto ruleName : PlanRuleRegistry::instance().getRegisteredNames())
    {
        auto rule = PlanRuleRegistry::instance().create(ruleName, arguments);
        if (!rule.has_value())
        {
            throw UnknownOptimizerRule("Did not find the rule {} in PlanRuleRegistry", ruleName);
        }
        ruleManager.addRule(rule.value());
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
