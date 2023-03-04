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
#include <Execution/Expressions/Functions/AcosExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

AcosExpression::AcosExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& Expression) : Expression(Expression) {}

/**
* @brief This method calculates the acosinus of x.
* This function is basically a wrapper for std::acos and enables us to use it in our execution engine framework.
* @param x double
* @return double
*/
double calculateAcos(double x) { return std::acos(x); }

Value<> AcosExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the  expression and retrieve the value.
    Value Value = Expression->execute(record);

    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // Value.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateMod function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (Value->isType<Int8>()) {
        // call the calculateAcos function with the correct type
        return FunctionCall<>("calculateAcos", calculateAcos, Value.as<Int8>());
    } else if (Value->isType<Int16>()) {
        return FunctionCall<>("calculateAcos", calculateAcos, Value.as<Int16>());
    } else if (Value->isType<Int32>()) {
        return FunctionCall<>("calculateAcos", calculateAcos, Value.as<Int32>());
    } else if (Value->isType<Int64>()) {
        return FunctionCall<>("calculateAcos", calculateAcos, Value.as<Int64>());
    } else if (Value->isType<Float>()) {
        return FunctionCall<>("calculateAcos", calculateAcos, Value.as<Float>());
    } else if (Value->isType<Double>()) {
        return FunctionCall<>("calculateAcos", calculateAcos, Value.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AcosExpression>> acosFunction("acos");
}// namespace NES::Runtime::Execution::Expressions
