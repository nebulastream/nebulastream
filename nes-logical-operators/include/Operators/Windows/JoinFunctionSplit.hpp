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

#include <optional>
#include <vector>

#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Schema/Field.hpp>

namespace NES
{

/// One `left.field = right.field` conjunct of a join function, with the fields already ordered by join side.
struct EquiJoinKey
{
    Field left;
    Field right;
};

/// A join function split into the part a hash join can use as its key and the rest.
/// The join function is equivalent to `AND(equiKeys...) AND residual`.
struct SplitJoinFunction
{
    /// Top-level conjuncts of the form `left.field = right.field`, with one field from each join input and joinable data types.
    std::vector<EquiJoinKey> equiKeys;
    /// AND of all remaining top-level conjuncts, in their original order; nullopt if every conjunct is an equi-key.
    std::optional<LogicalFunction> residual;
};

/// Splits the join function of `join` at its top-level ANDs (Spark's ExtractEquiJoinKeys, PostgreSQL's Hash Cond / Join Filter).
/// A top-level OR is a single conjunct and therefore never yields equi-keys. Expects the join function to be bound to the join's children.
SplitJoinFunction splitJoinFunction(const JoinLogicalOperator& join);

}
