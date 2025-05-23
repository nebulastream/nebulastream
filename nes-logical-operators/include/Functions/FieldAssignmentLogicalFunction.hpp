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

#include <Functions/FieldAccessLogicalFunction.hpp>

namespace NES
{

/// @brief A FieldAssignmentLogicalFunction represents the assignment of a function result to a specific field.
class FieldAssignmentLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "FieldAssignment";

    FieldAssignmentLogicalFunction(const FieldAccessLogicalFunction& fieldAccess, LogicalFunction logicalFunction);

    [[nodiscard]] FieldAccessLogicalFunction getField() const;
    [[nodiscard]] LogicalFunction getAssignment() const;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] std::shared_ptr<DataType> getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(std::shared_ptr<DataType> dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    friend bool operator==(const FieldAssignmentLogicalFunction& lhs, const FieldAssignmentLogicalFunction& rhs);
    friend bool operator!=(const FieldAssignmentLogicalFunction& lhs, const FieldAssignmentLogicalFunction& rhs);

private:
    std::shared_ptr<DataType> dataType;
    FieldAccessLogicalFunction fieldAccess;
    LogicalFunction logicalFunction;
};
}
FMT_OSTREAM(NES::FieldAssignmentLogicalFunction);
