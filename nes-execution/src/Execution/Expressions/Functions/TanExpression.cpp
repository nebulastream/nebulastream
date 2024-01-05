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
#include <Execution/Expressions/Functions/TanExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

TanExpression::TanExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& expression) : expression(expression) {}

double calculateTan(double x) { return std::tan(x); }

Value<> TanExpression::execute(NES::Nautilus::Record& record) const {
    Value value = expression->execute(record);

    if (value->isType<Int8>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<Int8>());
    } else if (value->isType<Int16>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<Int16>());
    } else if (value->isType<Int32>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<Int32>());
    } else if (value->isType<Int64>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<Int64>());
    } else if (value->isType<UInt8>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<UInt8>());
    } else if (value->isType<UInt16>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<UInt16>());
    } else if (value->isType<UInt32>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<UInt32>());
    } else if (value->isType<UInt64>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<UInt64>());
    } else if (value->isType<Float>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<Float>());
    } else if (value->isType<Double>()) {
        return FunctionCall<>("calculateTan", calculateTan, value.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<TanExpression>> tanFunction("tan");
}// namespace NES::Runtime::Execution::Expressions