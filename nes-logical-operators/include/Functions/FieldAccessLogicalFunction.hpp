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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Serialization/OperatorMappingContext.hpp>
#include "Identifiers/Identifiers.hpp"

namespace NES
{

/// @brief A FieldAccessFunction reads a specific field of the current record.
/// It can be created typed or untyped.
class FieldAccessLogicalFunction
{
public:
    static constexpr std::string_view NAME = "FieldAccess";

    explicit FieldAccessLogicalFunction(Field field);

    [[nodiscard]] Field getField() const;
    [[nodiscard]] FieldAccessLogicalFunction withField(Field fieldName) const;

    [[nodiscard]] bool operator==(const FieldAccessLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FieldAccessLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    Field field;
    friend struct std::hash<FieldAccessLogicalFunction>;
};

template <>
struct Reflector<FieldAccessLogicalFunction>
{
    Reflected operator()(const FieldAccessLogicalFunction& function) const;
};


template <>
struct Unreflector<FieldAccessLogicalFunction>
{
    FieldAccessLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<FieldAccessLogicalFunction>);
}

FMT_OSTREAM(NES::FieldAccessLogicalFunction);

template <>
struct std::hash<NES::FieldAccessLogicalFunction>
{
    size_t operator()(const NES::FieldAccessLogicalFunction& fieldAccessFunction) const noexcept;
};