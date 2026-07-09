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
#include <UdfCatalog.hpp>

namespace NES
{

/// Resolves name-only UDFCallLogicalFunction placeholders (created by the SQL parser) by attaching
/// the catalog UdfDescriptor. Runs before type inference so the resolved signature is available when
/// each function infers its result type. For the MVP it rewrites the functions of Selection (WHERE)
/// and Projection (SELECT) operators, where scalar UDF calls appear.
class UDFResolutionRule
{
public:
    explicit UDFResolutionRule(std::shared_ptr<const UdfCatalog> udfCatalog) : udfCatalog(std::move(udfCatalog)) { }

    static constexpr std::string_view NAME = "UDFResolutionRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    bool operator==(const UDFResolutionRule& other) const;

private:
    std::shared_ptr<const UdfCatalog> udfCatalog;
};

static_assert(RuleConcept<UDFResolutionRule, LogicalPlan>);
}
