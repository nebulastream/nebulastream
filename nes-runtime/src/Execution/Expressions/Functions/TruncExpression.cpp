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
#include <Execution/Expressions/Functions/TruncExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

TruncExpression::TruncExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}
/**
 * @brief This method rounds x down to the biggest integer
 * This function is basically a wrapper for std::trunc and enables us to use it in our execution engine framework.
 * @param x double
 * @return double
 */
double calculateTrunc(double x) { return std::trunc(x); }

Value<> TruncExpression::execute(NES::Nautilus::Record& record) const {
    Value subValue = subExpression->execute(record);

    if (subValue->isType<Int8>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<Int8>());
    } else if (subValue->isType<Int16>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<Int16>());
    } else if (subValue->isType<Int32>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<Int32>());
    } else if (subValue->isType<Int64>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<Int64>());
    } else if (subValue->isType<UInt8>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<UInt8>());
    } else if (subValue->isType<UInt16>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<UInt16>());
    } else if (subValue->isType<UInt32>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<UInt32>());
    } else if (subValue->isType<UInt64>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<UInt64>());
    } else if (subValue->isType<Float>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<Float>());
    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("calculateTrunc", calculateTrunc, subValue.as<Double>());
    } else {
        // Throw an exception if no type is applicable
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
}// namespace NES::Runtime::Execution::Expressions
