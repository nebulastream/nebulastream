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
#include <Execution/Expressions/Functions/BitcounterExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <bit>
namespace NES::Runtime::Execution::Expressions {

BitcounterExpression::BitcounterExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& SubExpression)
    : SubExpression(SubExpression) {}

uint32_t bitcounter(uint64_t number) { return std::popcount(number); }

Value<> BitcounterExpression::execute(NES::Nautilus::Record& record) const {
    Value leftValue = SubExpression->execute(record);

    if (leftValue->isType<Int8>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<Int8>());
    } else if (leftValue->isType<Int16>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<Int16>());
    } else if (leftValue->isType<Int32>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<Int32>());
    } else if (leftValue->isType<Int64>()) {
        return FunctionCall<>("calculateGamma", bitcounter, leftValue.as<Int64>());
    } else if (leftValue->isType<UInt8>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<UInt8>());
    } else if (leftValue->isType<UInt16>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<UInt16>());
    } else if (leftValue->isType<UInt32>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<UInt32>());
    } else if (leftValue->isType<UInt64>()) {
        return FunctionCall<>("bitcounter", bitcounter, leftValue.as<UInt64>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException("This expression is only defined on numeric input arguments that are Integer.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<BitcounterExpression>> bitcountFunction("bitcount");
}// namespace NES::Runtime::Execution::Expressions