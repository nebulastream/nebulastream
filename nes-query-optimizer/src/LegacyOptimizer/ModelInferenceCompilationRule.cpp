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

#include <LegacyOptimizer/ModelInferenceCompilationRule.hpp>

#include <utility>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void ModelInferenceCompilationRule::apply(LogicalPlan& queryPlan) const
{
    if (!modelCatalog)
    {
        return;
    }
    for (const auto& inferModelNameOp : getOperatorByType<InferModelNameLogicalOperator>(queryPlan))
    {
        const auto& op = inferModelNameOp.get();
        const auto& modelName = op.getModelName();

        if (!modelCatalog->hasModel(modelName))
        {
            throw UnknownModelName("Model '{}' is not registered", modelName);
        }

        auto model = modelCatalog->load(modelName);
        auto inputFieldNames = op.getInputFieldNames();
        if (inputFieldNames.empty())
        {
            const auto& reg = modelCatalog->getRegisteredModel(modelName);
            for (const auto& [name, dt] : reg.inputs)
            {
                inputFieldNames.push_back(name);
            }
        }
        auto inferModelOp = InferModelLogicalOperator(std::move(model), std::move(inputFieldNames));
        /// Preserve children from the original operator
        inferModelOp = inferModelOp.withChildren(op.getChildren());
        auto replacement = LogicalOperator(std::move(inferModelOp));

        auto replaceResult = replaceSubtree(queryPlan, inferModelNameOp.getId(), replacement);
        INVARIANT(replaceResult.has_value(), "Failed to replace InferModelNameLogicalOperator with InferModelLogicalOperator");
        queryPlan = std::move(replaceResult.value());
    }
}

}
