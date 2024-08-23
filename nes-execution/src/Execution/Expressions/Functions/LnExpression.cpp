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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/LnExpression.hpp>
#include <nautilus/std/cmath.h>
#include <cmath>
#include <utility>

namespace NES::Runtime::Execution::Expressions {

LnExpression::LnExpression(NES::Runtime::Execution::Expressions::ExpressionPtr  subExpression)
    : subExpression(std::move(subExpression)) {}


VarVal LnExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    const auto subValue = subExpression->execute(record);
    return subValue.customVisit(EVALUATE_FUNCTION(nautilus::log));
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<LnExpression>> lnFunction("ln");
}// namespace NES::Runtime::Execution::Expressions
