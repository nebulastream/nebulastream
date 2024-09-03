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

#include <Execution/Expressions/Expression.hpp>

namespace NES::Runtime::Execution::Expressions {

/// Returns true if the left and right subexpressions are NOT equal, otherwise false.
class NotEqualsExpression final : public Expression {
public:
    NotEqualsExpression(const ExpressionPtr& leftSubExpression, const ExpressionPtr& rightSubExpression);
    VarVal execute(Record& record) const override;

private:
    const ExpressionPtr leftSubExpression;
    const ExpressionPtr rightSubExpression;
};
}
