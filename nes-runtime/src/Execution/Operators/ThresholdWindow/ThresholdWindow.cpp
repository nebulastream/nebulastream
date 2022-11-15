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

#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

void NES::Runtime::Execution::Operators::ThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    assert(child != nullptr);// must have an aggregation operator as a child
    // Evaluate the threshold condition
    auto val = expression->execute(record);
    if (val) {
        auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2");
        auto aggregatedValue = readF2->execute(record);

        sum = sum + aggregatedValue.as<Int32>()->getRawInt(); // TODO 3138: properly handle the data type, should just a type of Value
    } else {
        auto aggregationResult = Record({{"sum", sum}});
        child->execute(ctx, aggregationResult);
        sum = 0;
    }
}

}// namespace NES::Runtime::Execution::Operators