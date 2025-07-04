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

#include <memory>
#include <utility>

#include <LegacyOptimizer/ModelInferenceCompilationRule.hpp>
#include "Operators/Windows/BatchingLogicalOperator.hpp"
#include "WindowTypes/Types/CountBasedWindow.hpp"

#include <InferModelLogicalOperator.hpp>
#include <InferModelNameLogicalOperator.hpp>
#include <ModelCatalog.hpp>

namespace NES::LegacyOptimizer
{

ModelInferenceCompilationRule::ModelInferenceCompilationRule(std::shared_ptr<const Nebuli::Inference::ModelCatalog> modelCatalog)
    : catalog(std::move(modelCatalog))
{
}

void ModelInferenceCompilationRule::apply(LogicalPlan& queryPlan)
{
    NES_DEBUG("ModelInferenceCompilationRule: Plan before\n{}", queryPlan);

    for (auto modelNameOperator : NES::getOperatorByType<InferModel::InferModelNameLogicalOperator>(queryPlan))
    {
        auto countBasedWindow = std::make_shared<Windowing::CountBasedWindow>(1);
        auto batching = BatchingLogicalOperator(countBasedWindow);

        auto name = modelNameOperator.getModelName();
        auto model = catalog->load(name);
        auto inferModel = InferModel::InferModelLogicalOperator(model, modelNameOperator.getInputFields());
        // USED_IN_DEBUG auto shouldReplace = replaceOperator(
        //     queryPlan, modelNameOperator, InferModel::InferModelLogicalOperator(model, modelNameOperator.getInputFields()));
        // queryPlan = std::move(shouldReplace.value());

        queryPlan = replaceSubtree(
                                queryPlan,
                                modelNameOperator,
                                inferModel.withChildren({batching.withChildren(modelNameOperator.getChildren())}))
                                .value();
    }

    NES_DEBUG("ModelInferenceCompilationRule: Plan after\n{}", queryPlan);
}

}
