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
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

/// Resolves InferModelNameLogicalOperator nodes to InferModelLogicalOperator nodes
/// by loading the model from the ModelCatalog.
class InferModelResolutionRule
{
public:
    explicit InferModelResolutionRule(std::shared_ptr<const ModelCatalog> modelCatalog) : modelCatalog(std::move(modelCatalog)) { }

    static constexpr std::string_view NAME = "InferModelResolutionRule";


    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;

private:
    std::shared_ptr<const ModelCatalog> modelCatalog;
};

static_assert(RuleConcept<InferModelResolutionRule, LogicalPlan>);
}
