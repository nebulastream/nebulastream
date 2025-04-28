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

#include <utility>
#include <memory>
#include <Operators/LogicalOperators/Inference/LogicalInferModelNameOperator.hpp>
#include <Operators/LogicalOperators/Inference/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Optimizer/QueryRewrite/ModelInferenceCompilationRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include "Operators/Operator.hpp"
#include "ModelCatalog.hpp"

namespace NES::Optimizer
{

ModelInferenceCompilationRule::ModelInferenceCompilationRule(std::shared_ptr<NES::Nebuli::Inference::ModelCatalog> modelCatalog)
    : catalog(std::move(modelCatalog))
{
}

std::shared_ptr<QueryPlan> ModelInferenceCompilationRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_DEBUG("ModelInferenceCompilationRule: Plan before\n{}", queryPlan->toString());

    for (auto& modelNameOperator : queryPlan->getOperatorByType<InferModel::LogicalInferModelNameOperator>())
    {
        auto name = modelNameOperator->getModelName();
        auto model = catalog->load(name);
        modelNameOperator->replace(std::make_shared<InferModel::LogicalInferModelOperator>(getNextOperatorId(), model, modelNameOperator->getInputFields()));
    }

    NES_DEBUG("ModelInferenceCompilationRule: Plan after\n{}", queryPlan->toString());
    return queryPlan;
}

}
