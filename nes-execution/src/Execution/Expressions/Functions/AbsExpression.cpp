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
#include <Execution/Expressions/Functions/AbsExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <cmath>
#include <nautilus/std/cmath.h>
#include <nautilus/val_ptr.hpp>
#include <utility>

namespace NES::Runtime::Execution::Expressions {

AbsExpression::AbsExpression(ExpressionPtr  subExpression)
    : subExpression(std::move(subExpression)) {}

VarVal AbsExpression::execute(NES::Nautilus::Record& record) const {
    auto subValue = subExpression->execute(record);
    /// To the PR reviewers, this is a point that we could do the following if we want to do NULL-handling
    /// if (subValue->isNull()) {
    ///     return subValue;
    /// }

    return subValue.customVisit(EVALUATE_FUNCTION(nautilus::abs));

    // return VarVal<double>(nautilus::abs(subValue);
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AbsExpression>> absFunction("abs");
}// namespace NES::Runtime::Execution::Expressions
