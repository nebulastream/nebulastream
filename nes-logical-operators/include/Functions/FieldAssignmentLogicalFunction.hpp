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
    FieldAssignmentLogicalFunction(const FieldAssignmentLogicalFunction& other);
    FieldAssignmentLogicalFunction& operator=(const FieldAssignmentLogicalFunction& other) = default;

    [[nodiscard]] const FieldAccessLogicalFunction& getField() const;
    [[nodiscard]] LogicalFunction getAssignment() const;

    [[nodiscard]] SerializableFunction serialize() const override;

    bool inferStamp(Schema schema) override;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const;

    const DataType& getStamp() const override {return *stamp;};
    void setStamp(std::shared_ptr<DataType> stamp) override { this->stamp = stamp; };
    bool inferStamp(Schema) override { return false; };
    std::vector<LogicalFunction> getChildren()  const override { return {fieldAccess, logicalFunction}; };
    void setChildren(std::vector<LogicalFunction> children)  override { fieldAccess = children[0].get<FieldAccessLogicalFunction>(); logicalFunction = children[1]; };
    std::string getType() const override { return std::string(NAME);}

    [[nodiscard]] std::string toString() const override;

private:
    std::shared_ptr<DataType> stamp;
    FieldAccessLogicalFunction fieldAccess;
    LogicalFunction logicalFunction;
};
}
FMT_OSTREAM(NES::FieldAssignmentLogicalFunction);
