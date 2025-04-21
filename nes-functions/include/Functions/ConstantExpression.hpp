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
#include <string>
#include <API/Schema.hpp>
#include <Functions/Expression.hpp>
#include <Nodes/Node.hpp>
#include <DataType.hpp>
namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus the samp of this function is always fixed.
class ConstantExpression : public Expression
{
public:
    explicit ConstantExpression(std::string constantValue) : constantValue(std::move(constantValue)) { }
    const std::string& getConstantValue() const { return constantValue; }

    void inferStamp(const Schema&, std::vector<ExpressionValue>& children) override
    {
        INVARIANT(children.empty(), "Constant Expression should not have any children");
        stamp = NES::constant(constantValue);
    }

    [[nodiscard]] std::shared_ptr<Expression> clone() const override { return std::make_shared<ConstantExpression>(*this); }

    bool equals(const ::NES::Expression& other) const override
    {
        if (const auto* otherFunction = dynamic_cast<const ConstantExpression*>(&other))
        {
            return otherFunction->constantValue == constantValue;
        }
        return false;
    }
    std::string toString() const override { return fmt::format("\"{}\"", constantValue); }


private:
    /// Value of this function
    std::string constantValue;
};
}
