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

#include <Functions/BinaryLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>

namespace NES
{

/// @brief A FieldAssignmentLogicalFunction represents the assignment of a function result to a specific field.
class FieldAssignmentLogicalFunction : public BinaryLogicalFunction
{
public:
    explicit FieldAssignmentLogicalFunction(std::shared_ptr<DataType> stamp);

    static std::shared_ptr<FieldAssignmentLogicalFunction> create(
        std::shared_ptr<FieldAccessLogicalFunction> fieldAccess, std::shared_ptr<LogicalFunction> LogicalFunction);

    [[nodiscard]] bool equal(std::shared_ptr<LogicalFunction> const& rhs) const override;

    [[nodiscard]] std::shared_ptr<FieldAccessLogicalFunction> getField() const;
    [[nodiscard]] std::shared_ptr<LogicalFunction> getAssignment() const;
    void inferStamp(const Schema& schema) override;

    std::shared_ptr<LogicalFunction> clone() const override;

protected:
    explicit FieldAssignmentLogicalFunction(FieldAssignmentLogicalFunction* other);

    [[nodiscard]] std::string toString() const override;
};
}
