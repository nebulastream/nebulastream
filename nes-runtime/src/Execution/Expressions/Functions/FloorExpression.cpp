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
#include <Execution/Expressions/Functions/FloorExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

FloorExpression::FloorExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression)
    : leftSubExpression(leftSubExpression) {}

/**
* @brief This method calculates the floor of x.
* This function is basically a wrapper for std::floor and enables us to use it in our execution engine framework.
* @param x double
* @return double
*/
double calculateFloor(double x) { return std::floor(x); }

Value<> FloorExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value leftValue = leftSubExpression->execute(record);
    // Evaluate the right sub expression and retrieve the value.

    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // leftValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateFloor function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (leftValue->isType<Float>()) {
        return FunctionCall<>("calculateFloor", calculateFloor, leftValue.as<Float>());
    } else if (leftValue->isType<Double>()) {
        return FunctionCall<>("calculateFloor", calculateFloor, leftValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException("This expression is only defined on numeric input arguments that are Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<FloorExpression>> floorFunction("floor");
}// namespace NES::Runtime::Execution::Expressions