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

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>

namespace NES
{

/// Constructs a compound Point Value with components `_x`, `_y`, `_z` from
/// three scalar child physical functions.
class PointConstructPhysicalFunction final
{
public:
    PointConstructPhysicalFunction(PhysicalFunction x, PhysicalFunction y, PhysicalFunction z);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction x, y, z;
};

/// Component-wise add: takes two compound Points, returns compound Point.
class PointAddPhysicalFunction final
{
public:
    PointAddPhysicalFunction(PhysicalFunction left, PhysicalFunction right);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction left, right;
};

/// Component-wise subtract.
class PointSubPhysicalFunction final
{
public:
    PointSubPhysicalFunction(PhysicalFunction left, PhysicalFunction right);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction left, right;
};

/// Scale a Point by a scalar factor.
class PointScalePhysicalFunction final
{
public:
    PointScalePhysicalFunction(PhysicalFunction point, PhysicalFunction scalar);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction point, scalar;
};

/// Euclidean distance between two Points; returns a scalar Value (FLOAT64).
class PointDistancePhysicalFunction final
{
public:
    PointDistancePhysicalFunction(PhysicalFunction left, PhysicalFunction right);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction left, right;
};

/// Component-wise equality with AND across components. Returns a scalar Bool.
class PointEqualsPhysicalFunction final
{
public:
    PointEqualsPhysicalFunction(PhysicalFunction left, PhysicalFunction right);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction left, right;
};

static_assert(PhysicalFunctionConcept<PointConstructPhysicalFunction>);
static_assert(PhysicalFunctionConcept<PointAddPhysicalFunction>);
static_assert(PhysicalFunctionConcept<PointSubPhysicalFunction>);
static_assert(PhysicalFunctionConcept<PointScalePhysicalFunction>);
static_assert(PhysicalFunctionConcept<PointDistancePhysicalFunction>);
static_assert(PhysicalFunctionConcept<PointEqualsPhysicalFunction>);

}
