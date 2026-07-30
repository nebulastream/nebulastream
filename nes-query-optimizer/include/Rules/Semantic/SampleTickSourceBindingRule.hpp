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

/// Attaches a synthetic timer source as the second child of every SampleLogicalOperator. Runs after
/// AnonymousSourceBindingRule/LogicalSourceExpansionRule so that the data-source child (child 0) is already resolved to
/// one or more SourceDescriptorLogicalOperators — this lets the tick source be co-located on the same host as the
/// data source, instead of guessing a placement.
class SampleTickSourceBindingRule
{
public:
    explicit SampleTickSourceBindingRule(std::shared_ptr<const SourceCatalog> sourceCatalog) : sourceCatalog(std::move(sourceCatalog)) { }

    static constexpr std::string_view NAME = "SampleTickSourceBindingRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    bool operator==(const SampleTickSourceBindingRule& other) const;

private:
    [[nodiscard]] LogicalOperator bindSampleTickSource(const LogicalOperator& current) const;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
};

static_assert(RuleConcept<SampleTickSourceBindingRule, LogicalPlan>);
}
