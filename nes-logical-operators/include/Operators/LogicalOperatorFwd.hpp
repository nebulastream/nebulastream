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

#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Reflection.hpp>

namespace NES
{
enum class ExplainVerbosity : uint8_t;
class TraitSet;
struct Field;

namespace detail
{
struct ErasedLogicalOperator;
}

template <typename Checked = NES::detail::ErasedLogicalOperator>
struct TypedLogicalOperator;
using LogicalOperator = TypedLogicalOperator<>;

/// Concept defining the interface for all logical operators in the query plan.
/// This concept defines the common interface that all logical operators must implement.
/// Logical operators represent operations in the query plan and are used during query
/// planning and optimization.
template <typename T>
concept LogicalOperatorConcept = requires(
    const T& thisOperator,
    ExplainVerbosity verbosity,
    OperatorId operatorId,
    std::vector<LogicalOperator> children,
    TraitSet traitSet,
    const T& rhs,
    std::vector<Schema> inputSchemas) {
    /// Returns a string representation of the operator
    { thisOperator.explain(verbosity, operatorId) } -> std::convertible_to<std::string>;

    /// Returns the children operators of this operator
    { thisOperator.getChildren() } -> std::convertible_to<std::vector<LogicalOperator>>;

    /// Creates a new operator with the given children
    { thisOperator.withChildren(children) } -> std::convertible_to<T>;

    /// Creates a new operator with the given traits
    { thisOperator.withTraitSet(traitSet) } -> std::convertible_to<T>;

    /// Compares this operator with another for equality
    { thisOperator == rhs } -> std::convertible_to<bool>;

    /// Returns the name of the operator, used during planning and optimization
    { thisOperator.getName() } noexcept -> std::convertible_to<std::string_view>;

    /// Serialize the operator to a Reflected object
    { NES::reflect(thisOperator) } -> std::same_as<Reflected>;

    /// Returns the trait set of the operator
    { thisOperator.getTraitSet() } -> std::convertible_to<TraitSet>;

    /// Returns the input schemas of the operator
    { thisOperator.getInputSchemas() } -> std::convertible_to<std::vector<Schema>>;

    /// Returns the output schema of the operator
    { thisOperator.getOutputSchema() } -> std::convertible_to<Schema>;

    /// Creates a new operator with inferred schema based on input schemas
    { thisOperator.withInferredSchema(inputSchemas) } -> std::convertible_to<T>;
};


template <typename WeakChecked>
requires(std::is_same_v<WeakChecked, NES::detail::ErasedLogicalOperator> || LogicalOperatorConcept<WeakChecked>)
struct WeakTypedLogicalOperator;

using WeakLogicalOperator = WeakTypedLogicalOperator<NES::detail::ErasedLogicalOperator>;
}
