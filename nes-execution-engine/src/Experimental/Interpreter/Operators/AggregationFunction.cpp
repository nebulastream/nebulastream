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

#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {



std::unique_ptr<AggregationState> SumFunction::createState() { return std::make_unique<SumState>(); }

SumFunction::SumFunction(std::shared_ptr<ReadFieldExpression> readFieldExpression) : readFieldExpression(readFieldExpression) {}

void SumFunction::liftCombine(std::unique_ptr<AggregationState>& state, Record& record) {
    auto sumState = (SumState*) state.get();
    auto value = readFieldExpression->execute(record);
    sumState->currentSum = sumState->currentSum + value;
}

void SumFunction::combine(std::unique_ptr<AggregationState>& leftState, std::unique_ptr<AggregationState>& rightState) {
    auto leftSum = (SumState*) leftState.get();
    auto rightSum = (SumState*) rightState.get();
    leftSum->currentSum = leftSum->currentSum + rightSum->currentSum;
}

Value<Any> SumFunction::lower(std::unique_ptr<AggregationState>& state) {
    auto sumState = (SumState*) state.get();
    return sumState->currentSum;
}
std::unique_ptr<AggregationState> SumFunction::loadState(Value<MemRef>& ref) {
    auto value = ref.load<Integer>();
    return std::make_unique<SumState>(value);
}
void SumFunction::storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) {
    auto sumState = (SumState*) state.get();
    ref.store(sumState->currentSum);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter