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


#include <Phases/SemanticAnalyzer.hpp>

#include <Rules/Semantic/InlineSinkBindingRule.hpp>
#include <Rules/Semantic/InlineSourceBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/ModelInferenceCompilationRule.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/SourceInferenceRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>

namespace NES
{
LogicalPlan SemanticAnalyzer::analyse(const LogicalPlan& plan) const
{
    auto newPlan = LogicalPlan{plan};
    const auto sinkBindingRule = SinkBindingRule{sinkCatalog};
    const auto inlineSinkBindingRule = InlineSinkBindingRule{sinkCatalog};
    const auto inlineSourceBindingRule = InlineSourceBindingRule{sourceCatalog};
    const auto sourceInference = SourceInferenceRule{sourceCatalog};
    const auto logicalSourceExpansionRule = LogicalSourceExpansionRule{sourceCatalog};
    constexpr auto typeInferenceRule = TypeInferenceRule{};
    constexpr auto originIdInferenceRule = OriginIdInferenceRule{};

    inlineSinkBindingRule.apply(newPlan);
    sinkBindingRule.apply(newPlan);
    inlineSourceBindingRule.apply(newPlan);
    sourceInference.apply(newPlan);
    logicalSourceExpansionRule.apply(newPlan);

    const auto modelInferenceCompilationRule = ModelInferenceCompilationRule{modelCatalog};
    modelInferenceCompilationRule.apply(newPlan);

    typeInferenceRule.apply(newPlan);
    originIdInferenceRule.apply(newPlan);
    typeInferenceRule.apply(newPlan);

    return newPlan;
}
}
