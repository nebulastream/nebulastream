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

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
/// Physical function that computes the cosine similarity (dot(a, b) / (||a|| * ||b||)) between two dense vectors.
/// Expects two VARSIZED inputs storing packed little-endian FLOAT32 elements (only float32 vectors are supported) and
/// returns a FLOAT64 in [-1.0, 1.0] (0.0 for zero-norm inputs, or NULL if either input is NULL).
/// Accumulates the dot product and squared L2 norms in double precision and clamps the quotient to [-1.0, 1.0].
/// See https://en.wikipedia.org/wiki/Cosine_similarity for the algorithm definition.
class CosSimPhysicalFunction final
{
public:
    explicit CosSimPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

    static PhysicalFunctionRegistryReturnType createCosSim(PhysicalFunctionRegistryArguments arguments);

private:
    PhysicalFunction leftPhysicalFunction;
    PhysicalFunction rightPhysicalFunction;
    DataType outputType;
};

static_assert(PhysicalFunctionConcept<CosSimPhysicalFunction>);

}
