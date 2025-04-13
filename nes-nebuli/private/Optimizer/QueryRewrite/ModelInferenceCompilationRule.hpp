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

#pragma once

#include <memory>
#include <set>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <ModelCatalog.hpp>

namespace NES::Optimizer
{

class ModelInferenceCompilationRule : public BaseRewriteRule
{
public:
    explicit ModelInferenceCompilationRule(std::shared_ptr<NES::Nebuli::Inference::ModelCatalog> modelCatalog);
    virtual ~ModelInferenceCompilationRule() = default;

    std::shared_ptr<QueryPlan> apply(std::shared_ptr<QueryPlan> queryPlan) override;

private:
    std::shared_ptr<NES::Nebuli::Inference::ModelCatalog> catalog;
};
}
