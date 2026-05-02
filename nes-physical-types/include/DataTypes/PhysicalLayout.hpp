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

#include <ostream>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// How a LogicalType maps onto one or more primitive (physical) DataType slots
/// at lowering time.
///
/// Scalar logical types (INT32, FLOAT64, ...) lower to a single component with
/// an empty suffix string ("") so a record field named `x` keeps that name in
/// the primitive schema. Compound logical types (Point, ...) lower to N
/// components whose suffix strings include the leading separator, e.g. Point
/// produces `[("_X", FLOAT64), ("_Y", FLOAT64), ("_Z", FLOAT64)]` so a Point
/// field named `P` expands to record fields `P_X`, `P_Y`, `P_Z`.
struct PhysicalLayout
{
    struct Component
    {
        std::string suffix;
        DataType physicalType;

        bool operator==(const Component&) const = default;
        friend std::ostream& operator<<(std::ostream& os, const Component& component);
    };

    std::vector<Component> components;

    [[nodiscard]] bool isCompound() const { return components.size() > 1; }
    [[nodiscard]] bool isScalar() const { return components.size() == 1 && components.front().suffix.empty(); }

    bool operator==(const PhysicalLayout&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const PhysicalLayout& layout);
};

}

FMT_OSTREAM(NES::PhysicalLayout);
FMT_OSTREAM(NES::PhysicalLayout::Component);
