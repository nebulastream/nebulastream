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
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/Log2Expression.hpp>
#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Nautilus/DataTypes/Operations/ExecutableDataTypeOperations.hpp>
#include <cmath>
#include <nautilus/std/cmath.h>

namespace NES::Runtime::Execution::Expressions {

Log2Expression::Log2Expression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}

ExecDataType Log2Expression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    const auto subValue = subExpression->execute(record);

    return FixedSizeExecutableDataType<Double>::create(nautilus::log2(castAndLoadValue<double>(subValue)));
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<Log2Expression>> log2Expression("log2");
}// namespace NES::Runtime::Execution::Expressions
