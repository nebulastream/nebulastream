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

#include "Util/PluginRegistry.hpp"
#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/MeosOperator.hpp>
#include <Expressions/Functions/LogicalFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Expressions {

MeosOperator::MeosOperator(const ExpressionPtr& left, const ExpressionPtr& middle, const ExpressionPtr& right)
    : left(left), middle(middle), right(right) {}

/**
 * @brief This method calculates the log2 of x.
 * This function is basically a wrapper for std::log2 and enables us to use it in our execution engine framework.
 * @param x double
 * @return double
 */
double meosT(double x, double y, double z) {
    // Implement your logic here
    NES_INFO("meosT called with x: {}, y: {}, z: {}", x, y, z);
    return x;
}

Value<> MeosOperator::execute(NES::Nautilus::Record& record) const {
    Value leftValue = left->execute(record);
    Value middleValue = middle->execute(record);
    Value rightValue = right->execute(record);

    if (leftValue->isType<Int8>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<Int8>(), middleValue.as<Int8>(), rightValue.as<Int8>());
    } else if (leftValue->isType<Int16>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<Int16>(), middleValue.as<Int16>(), rightValue.as<Int16>());
    } else if (leftValue->isType<Int32>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<Int32>(), middleValue.as<Int32>(), rightValue.as<Int32>());
    } else if (leftValue->isType<Int64>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<Int64>(), middleValue.as<Int64>(), rightValue.as<Int64>());
    } else if (leftValue->isType<UInt8>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<UInt8>(), middleValue.as<UInt8>(), rightValue.as<UInt8>());
    } else if (leftValue->isType<UInt16>()) {
        return Nautilus::FunctionCall<>("meosT",
                                        meosT,
                                        leftValue.as<UInt16>(),
                                        middleValue.as<UInt16>(),
                                        rightValue.as<UInt16>());
    } else if (leftValue->isType<UInt32>()) {
        return Nautilus::FunctionCall<>("meosT",
                                        meosT,
                                        leftValue.as<UInt32>(),
                                        middleValue.as<UInt32>(),
                                        rightValue.as<UInt32>());
    } else if (leftValue->isType<UInt64>()) {
        return Nautilus::FunctionCall<>("meosT",
                                        meosT,
                                        leftValue.as<UInt64>(),
                                        middleValue.as<UInt64>(),
                                        rightValue.as<UInt64>());
    } else if (leftValue->isType<Float>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<Float>(), middleValue.as<Float>(), rightValue.as<Float>());
    } else if (leftValue->isType<Double>()) {
        return Nautilus::FunctionCall<>("meosT",
                                        meosT,
                                        leftValue.as<Double>(),
                                        middleValue.as<Double>(),
                                        rightValue.as<Double>());
    } else {
        // Throw an exception if no type is applicable
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<MeosOperator>> MeosOperator("meosT");

}// namespace NES::Runtime::Execution::Expressions
