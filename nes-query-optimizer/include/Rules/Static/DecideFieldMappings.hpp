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
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <vector>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

/// @brief A rewrite stage that sets the FieldMappingTrait to prevent read-write conflicts inside a single projection.
/// This phase on its own does not change anything in the execution of the query, the lowering needs to use the FieldMappingTrait.
///
/// Without this stage, the following code would yield the wrong result:
/// @code{sql}
/// SELECT attr1 + 1 as attr1, attr1 + attr2 as attr2 FROM r
/// @endcode
/// This is, because in our execution engine we would write the attr1 value in place and then use that just written value in the second read.
///
/// This stage detects these conflicts and redirects conflicting writes to other field names.
/// These alternative field names are then propagated through the plan only over the field mapping trait, WITHOUT changing the operators themselves.
class DecideFieldMappings
{
public:
    explicit DecideFieldMappings() = default;

    static constexpr std::string_view NAME = "DecideFieldMappings";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    bool operator==(const DecideFieldMappings& other) const;

private:
    [[nodiscard]] LogicalOperator apply(const LogicalOperator& logicalOperator, std::vector<LogicalOperator> children) const;
};
}
