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
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
namespace NES::Runtime::Execution::Expressions {

ConstantIntegerExpression::ConstantIntegerExpression(int64_t integerValue) : integerValue(integerValue) {}

Value<> ConstantIntegerExpression::execute(Record&) const { return Value<>(integerValue); }

}// namespace NES::Runtime::Execution::Expressions