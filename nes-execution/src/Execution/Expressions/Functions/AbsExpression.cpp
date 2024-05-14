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
double calculateAbsFloat(double n) { return std::abs(n); }
double calculateAbsDouble(double n) { return std::abs(n); }
int64_t calculateAbsSignedInt64(int64_t n) { return std::abs(n); }
int32_t calculateAbsSignedInt32(int32_t n) { return std::abs(n); }
int16_t calculateAbsSignedInt16(int16_t n) { return std::abs(n); }
int8_t calculateAbsSignedInt8(int8_t n) { return std::abs(n); }

Value<> AbsExpression::execute(NES::Nautilus::Record& record) const {
    Value subValue = subExpression->execute(record);

    if (subValue->isType<Float>()) {
        return FunctionCall<>("calculateAbsFloat", calculateAbsFloat, subValue.as<Float>());

    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("calculateAbsDouble", calculateAbsDouble, subValue.as<Double>());

    } else if (subValue->isType<UInt64>() || subValue->isType<UInt32>() || subValue->isType<UInt16>()
               || subValue->isType<UInt8>()) {
        return subValue;

    } else if (subValue->isType<Int64>()) {
        return FunctionCall<>("calculateAbsSignedInt64", calculateAbsSignedInt64, subValue.as<Int64>());

    } else if (subValue->isType<Int32>()) {
        return FunctionCall<>("calculateAbsSignedInt32", calculateAbsSignedInt32, subValue.as<Int32>());

    } else if (subValue->isType<Int16>()) {
        return FunctionCall<>("calculateAbsSignedInt16", calculateAbsSignedInt16, subValue.as<Int16>());

    } else if (subValue->isType<Int8>()) {
        return FunctionCall<>("calculateAbsSignedInt8", calculateAbsSignedInt8, subValue.as<Int8>());

    } else {
        throw Exceptions::NotImplementedException(
            "This expression is only defined on a numeric input argument that is ether Float or Double.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AbsExpression>> absFunction("abs");
}// namespace NES::Runtime::Execution::Expressions
