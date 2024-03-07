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

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Runtime::Execution::Operators {

void Operator::setup(ExecutionContext& executionCtx) const {
    if (hasChild()) {
        child->setup(executionCtx);
    }
}

void Operator::open(ExecutionContext& executionCtx, RecordBuffer& rb) const {
    /** We need to set the statisticId in the emitted tuplebuffer. With our current query compiler interface, we
     * can not access the input tuple buffer in the emit operator. Therefore, we use the ExecutionContext as a bridge
     * between Scan (access to the input buffer) and Emit (access to the output buffer).
     * As the statistic id is tied to a tuple buffer, we have to set it again for each tuple buffer.
     */
    executionCtx.setCurrentStatisticId(statisticId);
    if (hasChild()) {
        child->open(executionCtx, rb);
    }
}

void Operator::close(ExecutionContext& executionCtx, RecordBuffer& rb) const {
    if (hasChild()) {
        child->close(executionCtx, rb);
    }
}

bool Operator::hasChild() const { return child != nullptr; }

void Operator::setChild(Operators::ExecuteOperatorPtr child) {
    if (hasChild()) {
        NES_THROW_RUNTIME_ERROR("This operator already has a child operator");
    }
    this->child = std::move(child);
}

void Operator::setStatisticId(StatisticId statisticId) {
    this->statisticId = statisticId;
}

void Operator::terminate(ExecutionContext& executionCtx) const {
    if (hasChild()) {
        child->terminate(executionCtx);
    }
}

Operator::~Operator() {}

}// namespace NES::Runtime::Execution::Operators