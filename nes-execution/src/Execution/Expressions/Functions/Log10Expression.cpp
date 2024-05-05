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
#include <Execution/Expressions/Functions/Log10Expression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

Log10Expression::Log10Expression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}

/**
 * @brief This method calculates the log10 for x.
 * This function is basically a wrapper for std::log10 and enables us to use it in our execution engine framework.
 * @param x: Value to perform log10(x)
 * @return double
 */
double calculateLog10U8(uint8_t x) { return std::log10(x); }
double calculateLog10U16(uint16_t x) { return std::log10(x); }
double calculateLog10U32(uint32_t x) { return std::log10(x); }
double calculateLog10U64(uint64_t x) { return std::log10(x); }

double calculateLog10I8(int8_t x) { return std::log10(x); }
double calculateLog10I16(int16_t x) { return std::log10(x); }
double calculateLog10I32(int32_t x) { return std::log10(x); }
double calculateLog10I64(int64_t x) { return std::log10(x); }

double calculateLog10F32(float x) { return std::log10(x); }
double calculateLog10F64(double x) { return std::log10(x); }

Value<> Log10Expression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    Value subValue = subExpression->execute(record);

    if (subValue->isType<Int8>()) {
        return FunctionCall<>("calculateLog10I8", calculateLog10I8, subValue.as<Int8>());
    } else if (subValue->isType<Int16>()) {
        return FunctionCall<>("calculateLog10I16", calculateLog10I16, subValue.as<Int16>());
    } else if (subValue->isType<Int32>()) {
        return FunctionCall<>("calculateLog10I32", calculateLog10I32, subValue.as<Int32>());
    } else if (subValue->isType<Int64>()) {
        return FunctionCall<>("calculateLog10I64", calculateLog10I64, subValue.as<Int64>());
    } else if (subValue->isType<UInt8>()) {
        return FunctionCall<>("calculateLog10U8", calculateLog10U8, subValue.as<UInt8>());
    } else if (subValue->isType<UInt16>()) {
        return FunctionCall<>("calculateLog10U16", calculateLog10U16, subValue.as<UInt16>());
    } else if (subValue->isType<UInt32>()) {
        return FunctionCall<>("calculateLog10U32", calculateLog10U32, subValue.as<UInt32>());
    } else if (subValue->isType<UInt64>()) {
        return FunctionCall<>("calculateLog10U64", calculateLog10U64, subValue.as<UInt64>());
    } else if (subValue->isType<Float>()) {
        return FunctionCall<>("calculateLog10F32", calculateLog10F32, subValue.as<Float>());
    } else if (subValue->isType<Double>()) {
        return FunctionCall<>("calculateLog10F64", calculateLog10F64, subValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<Log10Expression>> log10Expression("log10");
}// namespace NES::Runtime::Execution::Expressions
