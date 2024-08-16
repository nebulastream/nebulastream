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
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/Log10Expression.hpp>
#include <cmath>
#include <nautilus/std/cmath.h>

namespace NES::Runtime::Execution::Expressions {

Log10Expression::Log10Expression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}


ExecDataType Log10Expression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    const auto subValue = subExpression->execute(record);
    return FixedSizeExecutableDataType<Double>::create(nautilus::log10(castAndLoadValue<double>(subValue)));
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<Log10Expression>> log10Expression("log10");
}// namespace NES::Runtime::Execution::Expressions
