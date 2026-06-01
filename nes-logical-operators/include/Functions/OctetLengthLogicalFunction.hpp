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
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// SQL OCTET_LENGTH: returns the number of bytes (octets) in a VARSIZED text value.
/// Takes a single VARSIZED child expression and produces a UINT64 length.
class OctetLengthLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "OCTET_LENGTH";

    explicit OctetLengthLogicalFunction(LogicalFunction child);

    [[nodiscard]] bool operator==(const OctetLengthLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] OctetLengthLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<OctetLengthLogicalFunction>;
};

template <>
struct Reflector<OctetLengthLogicalFunction>
{
    Reflected operator()(const OctetLengthLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<OctetLengthLogicalFunction>
{
    OctetLengthLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<OctetLengthLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedOctetLengthLogicalFunction
{
    LogicalFunction child;
};
}

FMT_OSTREAM(NES::OctetLengthLogicalFunction);
