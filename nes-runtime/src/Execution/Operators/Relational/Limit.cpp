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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Limit.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

bool IncrementThreadSaveAndCheckLimit(void* op) {
    NES_ASSERT2_FMT(op != nullptr, "operator handler context should not be null");
    auto* opHandler = static_cast<LimitOperatorHandler*>(op);

    auto value = opHandler->counter++;
    // Are we under the limit?
    return value < opHandler->limit;
}

void Limit::execute(ExecutionContext& ctx, Record& record) const {
    // 1) get the global operator state
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // 2) check if the limit was reached
    if (Nautilus::FunctionCall("IncrementThreadSaveAndCheckLimit", IncrementThreadSaveAndCheckLimit, globalOperatorHandler)) {
        child->execute(ctx, record);
    } else {
        // In the future we need here to somehow signal the parent or data source that we already finished
        return;
    }
}

}// namespace NES::Runtime::Execution::Operators