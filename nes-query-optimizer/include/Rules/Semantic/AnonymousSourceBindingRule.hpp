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

#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{

/// The AnonymousSourceBindingPhase replaces all anonymously defined sources with physical sources based on the given source configuration.

class AnonymousSourceBindingRule
{
public:
    explicit AnonymousSourceBindingRule(std::shared_ptr<const SourceCatalog> sourceCatalog) : sourceCatalog(std::move(sourceCatalog)) { }

    static constexpr std::string_view NAME = "AnonymousSourceBindingRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;

private:
    [[nodiscard]] LogicalOperator bindAnonymousSourceLogicalOperators(const LogicalOperator& current) const;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
};

static_assert(RuleConcept<AnonymousSourceBindingRule, LogicalPlan>);
}
