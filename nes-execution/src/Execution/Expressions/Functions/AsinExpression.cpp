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
#include <Execution/Expressions/Functions/AsinExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

AsinExpression::AsinExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& radians) : radians(radians) {}

/**
* @brief This method calculates the asin of X.
* This function is basically a wrapper for std::asin and enables us to use it in our execution engine framework.
* @param x double
* @return double
*/
double calculateAsin(double x) { return std::asin(x); }

Value<> AsinExpression::execute(NES::Nautilus::Record& record) const {

    Value subValue = radians->execute(record);
    if (subValue->isType<Float>()) {
        return FunctionCall<>("calculateAsin", calculateAsin, subValue.as<Float>());
    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("calculateAsin", calculateAsin, subValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on a numeric input argument that is ether Integer or Float.");
    }
}

static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<AsinExpression>> asinFunction("asin");

}// namespace NES::Runtime::Execution::Expressions
