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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Relational/ThresholdWindow.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

void NES::Runtime::Execution::Operators::ThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    NES_DEBUG("record count");
    const Expressions::ExpressionPtr subExpression;
    // evaluate the predicate
    if (expression->execute(record)) {
        if (child != nullptr) {
            // TODO 3138 this should be part of the aggregation operator, which should be the child of this threshold window operator
            auto readF2 = std::make_shared<Expressions::ReadFieldExpression>("f2"); // TODO 3138: f2 should be taken from the aggregation operator
            auto res = readF2->execute(record);

            sum = sum + res.as<Int32>()->getRawInt();
            child->execute(ctx, record);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators
