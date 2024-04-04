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

#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/AbsExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

AbsExpression::AbsExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}

/**
  * @brief This method computes the absolute value of n.
  * This function is basically a wrapper for std::abs and enables us to use it in our execution engine framework.
  * @param n double
  * @return double
  */
double calculateAbs(double n) { return std::abs(n); }

Value<> AbsExpression::execute(NES::Nautilus::Record& record) const {
    Value subValue = subExpression->execute(record);

    // TODO: missing support for int: issue #3682
    if (subValue->isType<Float>()) {
        return FunctionCall<>("calculateAbs", calculateAbs, subValue.as<Float>());

    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("calculateAbs", calculateAbs, subValue.as<Double>());

    } else {
        throw Exceptions::NotImplementedException(
            "This expression is only defined on a numeric input argument that is ether Float or Double.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AbsExpression>> absFunction("abs");
}// namespace NES::Runtime::Execution::Expressions
