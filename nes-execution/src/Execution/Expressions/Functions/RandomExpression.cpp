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
#include <Execution/Expressions/Functions/RandomExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cstdlib>
#include <iostream>
#include <utility>
namespace NES::Runtime::Execution::Expressions {

RandomExpression::RandomExpression(NES::Runtime::Execution::Expressions::ExpressionPtr subExpression)
    : subExpression(std::move(subExpression)) {}

double random(uint32_t seed) {
    std::srand(seed);
    return (double) std::rand() / RAND_MAX;
}

Value<> RandomExpression::execute(NES::Nautilus::Record& record) const {
    //Evaluate the sub expression and retrieve the value.
    Value subValue = subExpression->execute(record);
    //check the type and then call the function.
    if (subValue->isType<Int8>()) {
        return FunctionCall<>("random", random, subValue.as<Int8>());
    } else if (subValue->isType<Int16>()) {
        return FunctionCall<>("random", random, subValue.as<Int16>());
    } else if (subValue->isType<Int32>()) {
        return FunctionCall<>("random", random, subValue.as<Int32>());
    } else if (subValue->isType<Int64>()) {
        return FunctionCall<>("random", random, subValue.as<Int64>());
    } else if (subValue->isType<UInt8>()) {
        return FunctionCall<>("random", random, subValue.as<UInt8>());
    } else if (subValue->isType<UInt16>()) {
        return FunctionCall<>("random", random, subValue.as<UInt16>());
    } else if (subValue->isType<UInt32>()) {
        return FunctionCall<>("random", random, subValue.as<UInt32>());
    } else if (subValue->isType<UInt64>()) {
        return FunctionCall<>("random", random, subValue.as<UInt64>());
    } else if (subValue->isType<Float>()) {
        return FunctionCall<>("random", random, subValue.as<Float>());
    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("random", random, subValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<RandomExpression>> randomFunction("random");
}// namespace NES::Runtime::Execution::Expressions