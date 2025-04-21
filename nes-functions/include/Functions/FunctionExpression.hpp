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
#include <ranges>
#include <string>
#include <unordered_set>

#include <API/Schema.hpp>
#include <Functions/Expression.hpp>
#include <Nodes/Node.hpp>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <DataType.hpp>

namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus the samp of this function is always fixed.
class FunctionExpression : public Expression
{
    std::string functionName;

public:
    explicit FunctionExpression(std::string function_name) : functionName(std::move(function_name)) { }
    explicit FunctionExpression(std::string_view function_name) : functionName(std::string(function_name)) { }

    void inferStamp(const Schema& schema, std::vector<ExpressionValue>& children) override;
    [[nodiscard]] std::shared_ptr<Expression> clone() const override { return std::make_shared<FunctionExpression>(*this); }
    bool equals(const ::NES::Expression& other) const override
    {
        if (const auto* otherFunction = dynamic_cast<const FunctionExpression*>(&other))
        {
            return otherFunction->functionName == functionName;
        }
        return false;
    }

    std::string toString() const override { return functionName; }

    [[nodiscard]] const std::string& getFunctionName() const { return functionName; }
};
}
