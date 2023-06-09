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

#include <Experimental/Operators/SynopsesOperator.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Operators {

SynopsesOperator::SynopsesOperator(uint64_t handlerIndex, const ASP::AbstractSynopsesPtr& synopses) : handlerIndex(handlerIndex),
                                                                                                      synopses(synopses) {}


void SynopsesOperator::execute(ExecutionContext& ctx, Record& record) const {
    // Retrieve operator state
    Runtime::Execution::Operators::OperatorState* state = nullptr;
    if (hasLocalState) {
        state = ctx.getLocalState(this);
    }

    // For now, we do not have to care about calling getApproximate once the window is done #3628
    synopses->addToSynopsis(handlerIndex, ctx, record, state);
}

void SynopsesOperator::setup(ExecutionContext& ctx) const {
    synopses->setup(handlerIndex, ctx);
}

void SynopsesOperator::open(ExecutionContext &executionCtx, RecordBuffer &recordBuffer) const {
    synopses->storeLocalOperatorState(handlerIndex, this, executionCtx, recordBuffer);
}

void SynopsesOperator::setHasLocalState(bool hasLocalState) {
    SynopsesOperator::hasLocalState = hasLocalState;
}

} // namespace NES::Runtime::Execution::Operators