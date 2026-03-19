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


#include <SemanticAnalyzer.hpp>

#include <memory>
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Rules/RuleManager.hpp>
#include <Rules/Semantic/InlineSinkBindingRule.hpp>
#include <Rules/Semantic/InlineSourceBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/SourceInferenceRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>

namespace NES
{

SemanticAnalyzer::SemanticAnalyzer(std::shared_ptr<SourceCatalog> sourceCatalog, std::shared_ptr<SinkCatalog> sinkCatalog)
    : sourceCatalog(std::move(sourceCatalog)), sinkCatalog(std::move(sinkCatalog))
{
    PlanRuleManager ruleManager;
    ruleManager.addRule(InlineSinkBindingRule{this->sinkCatalog});
    ruleManager.addRule(SinkBindingRule{this->sinkCatalog});
    ruleManager.addRule(InlineSourceBindingRule{this->sourceCatalog});
    ruleManager.addRule(SourceInferenceRule{this->sourceCatalog});
    ruleManager.addRule(LogicalSourceExpansionRule{this->sourceCatalog});
    ruleManager.addRule(TypeInferenceRule{});
    ruleManager.addRule(OriginIdInferenceRule{});

    this->ruleSequence = ruleManager.getSequence();
}

LogicalPlan SemanticAnalyzer::analyse(LogicalPlan plan) const
{
    for (const auto& rule : ruleSequence)
    {
        plan = rule.apply(plan);
    }
    return plan;
}


}
