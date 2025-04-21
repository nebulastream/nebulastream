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
#include <API/Schema.hpp>
#include <Functions/Expression.hpp>
#include <Nodes/Node.hpp>
#include <DataType.hpp>
namespace NES
{

class FieldAssignmentExpression : public Expression
{
    Schema::Identifier field;

public:
    explicit FieldAssignmentExpression(const Schema::Identifier& field) : field(field) { }
    const Schema::Identifier& getField() const { return field; }
    [[nodiscard]] std::shared_ptr<Expression> clone() const override { return std::make_shared<FieldAssignmentExpression>(*this); }
    void inferStamp(const Schema& schema, std::vector<ExpressionValue>& children) override
    {
        children.at(0).inferStamp(schema);
        stamp = children.at(0).getStamp().value();
    }
    bool equals(const ::NES::Expression& other) const override
    {
        if (const auto* otherFunction = dynamic_cast<const FieldAssignmentExpression*>(&other))
        {
            return otherFunction->field == field;
        }
        return false;
    }
    std::string toString() const override { return fmt::format("{} =", field); }
};
}
