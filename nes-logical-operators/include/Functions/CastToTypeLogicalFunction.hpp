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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// Casts the input to the provided data type
class CastToTypeLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Cast";

    CastToTypeLogicalFunction(LogicalType logicalType, LogicalFunction child);

    [[nodiscard]] bool operator==(const CastToTypeLogicalFunction& rhs) const;

    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] CastToTypeLogicalFunction withLogicalType(const LogicalType& logicalType) const;
    [[nodiscard]] LogicalFunction withInferredLogicalType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] CastToTypeLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType castToType;
    LogicalFunction child;
};

template <>
struct Reflector<CastToTypeLogicalFunction>
{
    Reflected operator()(const CastToTypeLogicalFunction& function) const;
};

template <>
struct Unreflector<CastToTypeLogicalFunction>
{
    CastToTypeLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<CastToTypeLogicalFunction>);

}

FMT_OSTREAM(NES::CastToTypeLogicalFunction);
