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
#include <Execution/Expressions/Functions/AcosExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <nautilus/std/cmath.h>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

AcosExpression::AcosExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& expression) : Expression() {}

VarVal AcosExpression::execute(NES::Nautilus::Record& record) const {
    const auto subValue = expression->execute(record);

    return subValue.customVisit(EVALUATE_FUNCTION(nautilus::acos));
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AcosExpression>> acosFunction("acos");
}// namespace NES::Runtime::Execution::Expressions
