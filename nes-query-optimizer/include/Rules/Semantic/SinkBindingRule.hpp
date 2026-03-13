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
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <Sinks/SinkCatalog.hpp>

namespace NES
{
class SinkBindingRule
{
public:
    explicit SinkBindingRule(std::shared_ptr<const SinkCatalog> sinkCatalog) : sinkCatalog(std::move(sinkCatalog)) { }

    static constexpr std::string_view NAME = "SinkBindingRule";

    [[nodiscard]] const std::type_info& getType() const;
    [[nodiscard]] std::string_view getName() const;
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    bool operator==(const SinkBindingRule& other) const;

private:
    std::shared_ptr<const SinkCatalog> sinkCatalog;
};

static_assert(PlanRuleConcept<SinkBindingRule>);
}
