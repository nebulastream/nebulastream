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

#include <string>
#include <string_view>
#include <unordered_map>

#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Container for all scalar functions that need to be used during the aggregation function calculation (lift, combine, lower...),
/// keyed by the role they play (see AggregationScalarFunctionRoles).
/// Since the aggregation function could be over an arbitrary typed field (including user-defined DataType plugins), it is important to
/// properly verify that we support the aggregation over such a type. The scalar functions are verified via logical type inference and
/// lowered to physical functions alongside the aggregation function, so the physical aggregation no longer has to rely on the
/// VarVal / nautilus-internal operators like +, >, / to aggregate over complex types.
using LogicalScalarFunctions = std::unordered_map<std::string, LogicalFunction>;

/// The roles under which an aggregation function stores its scalar functions. Each aggregation only uses the roles it needs.
/// The logical aggregation builds the function for a role, the physical aggregation looks it up under the same role.
namespace AggregationScalarFunctionRoles
{
/// Folds one input value into the current state. Operands: STATE, INPUT.
constexpr std::string_view LIFT = "lift";
/// Merges a second state into the current one. Operands: STATE, OTHER_STATE.
constexpr std::string_view COMBINE = "combine";
/// Produces the final result from the state. Operands depend on the aggregation (e.g. STATE and COUNT for Avg).
constexpr std::string_view LOWER = "lower";
}

/// The operand names shared between the logical side (as AggregationOperandLogicalFunction leaves of the scalar functions) and the
/// physical side (as record fields the physical aggregation writes before executing a scalar function).
/// The names are the contract between both sides; a mismatch surfaces as a missing record field during tracing.
namespace AggregationOperands
{
constexpr std::string_view STATE = "__agg_state";
constexpr std::string_view INPUT = "__agg_input";
constexpr std::string_view OTHER_STATE = "__agg_other_state";
constexpr std::string_view COUNT = "__agg_count";
}

/// Creates a scalar function operand placeholder with a fixed data type.
LogicalFunction aggregationOperand(std::string_view operandName, DataType dataType);

/// Wraps function in a cast to resultType so that an aggregate is accumulated in its (possibly widened) result type.
/// A STRUCT result type (user-defined type plugin) cannot be cast; the plugin's scalar function result is then used as is.
LogicalFunction castUnlessStruct(const DataType& resultType, LogicalFunction function);

/// Runs type inference on every scalar function so that unsupported type combinations are rejected while the query is still logical.
/// The schema is only passed through to satisfy the inference interface; the operands carry their own types.
LogicalScalarFunctions inferScalarFunctions(LogicalScalarFunctions scalarFunctions, const Schema<Field, Unordered>& schema);
}
