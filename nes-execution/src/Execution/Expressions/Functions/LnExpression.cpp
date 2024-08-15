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
#include <Execution/Expressions/Functions/LnExpression.hpp>
#include <nautilus/std/cmath.h>
#include <cmath>
#include <utility>

namespace NES::Runtime::Execution::Expressions {

LnExpression::LnExpression(NES::Runtime::Execution::Expressions::ExpressionPtr  subExpression)
    : subExpression(std::move(subExpression)) {}


ExecDataType LnExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    const auto subValue = subExpression->execute(record);

    if (subValue->instanceOf<ExecDataFloat>()) {
        return ExecDataFloat::create(nautilus::log(subValue->as<ExecDataFloat>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataDouble>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataDouble>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataUInt64>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataUInt64>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataUInt32>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataUInt32>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataUInt16>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataUInt16>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataUInt8>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataUInt8>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataInt64>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataInt64>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataInt32>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataInt32>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataInt16>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataInt16>()->valueAsType<double>()));
    } else if (subValue->instanceOf<ExecDataInt8>()) {
        return ExecDataDouble::create(nautilus::log(subValue->as<ExecDataInt8>()->valueAsType<double>()));
    } else {
        throw Exceptions::NotImplementedException(
            "This expression is only defined on a numeric input argument that is ether Float or Double.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<LnExpression>> lnFunction("ln");
}// namespace NES::Runtime::Execution::Expressions
