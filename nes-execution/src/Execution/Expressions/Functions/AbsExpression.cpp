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

#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/AbsExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <cmath>
#include <nautilus/std/cmath.h>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution::Expressions {

AbsExpression::AbsExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}

ExecDataType AbsExpression::execute(NES::Nautilus::Record& record) const {
    auto subValue = subExpression->execute(record);
    /// To the PR reviewers, this is a point that we could do the following if we want to do NULL-handling
    /// if (subValue->isNull()) {
    ///     return subValue;
    /// }
    return FixedSizeExecutableDataType<Double>::create(nautilus::abs(castAndLoadValue<double>(subValue)));
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AbsExpression>> absFunction("abs");
}// namespace NES::Runtime::Execution::Expressions
