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
#include <cmath>
#include <nautilus/std/cmath.h>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution::Expressions {

AbsExpression::AbsExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}

ExecDataType AbsExpression::execute(NES::Nautilus::Record& record) const {
    auto subValue = subExpression->execute(record);

    if (subValue->isType<float>()) {
        return ExecutableDataType<float>::create(abs(subValue->as<float>()));
    } else if (subValue->isType<double>()) {
        return ExecutableDataType<double>::create(abs(subValue->as<double>()));
    } else if (subValue->isType<uint64_t>() || subValue->isType<uint32_t>() || subValue->isType<uint16_t>()
               || subValue->isType<uint8_t>()) {
        return subValue;
    } else if (subValue->isType<int64_t>()) {
        return ExecutableDataType<int64_t>::create(abs(subValue->as<int64_t>()));
    } else if (subValue->isType<int32_t>()) {
        return ExecutableDataType<int32_t>::create(abs(subValue->as<int32_t>()));
    } else if (subValue->isType<int16_t>()) {
        return ExecutableDataType<int16_t>::create(abs(subValue->as<int16_t>()));
    } else if (subValue->isType<int8_t>()) {
        return ExecutableDataType<int8_t>::create(abs(subValue->as<int8_t>()));
    } else {
        throw Exceptions::NotImplementedException(
            "This expression is only defined on a numeric input argument that is ether Float or Double.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AbsExpression>> absFunction("abs");
}// namespace NES::Runtime::Execution::Expressions
