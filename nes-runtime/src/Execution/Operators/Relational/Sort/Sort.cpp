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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Relational/Sort/Sort.hpp>
#include <Execution/Operators/Relational/Sort/SortOperatorHandler.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

Sort::Sort(const uint64_t operatorHandlerIndex, const std::vector<Expressions::ExpressionPtr>& sortExpressions, const std::vector<PhysicalTypePtr>& sortDataTypes)
    : operatorHandlerIndex(operatorHandlerIndex), sortExpressions(sortExpressions), sortDataTypes(sortDataTypes) {

    for (auto& sortType : sortDataTypes) {
        keySize = keySize + sortType->size();
    }
}

void Sort::execute(ExecutionContext& ctx, Record& record) const {
    // get local state
    auto state = reinterpret_cast<SortOperatorHandler*>(ctx.getLocalState(this));
    auto stack = state->getState();

    // create entry and store it in stack
    auto entry = stack.allocateEntry();

    auto fields = record.getAllFields();
    for (size_t i = 0; i < fields.size(); ++i) {
        auto val = record.read(fields[i]);
        entry.store(val);
        entry = entry + sortDataTypes[i]->size();
    }
}

// This will be called when the tuple was fully processed
void Sort::close(ExecutionContext& executionCtx,  RecordBuffer& recordBuffer) const {
    // Perform the actual sort
    //RadixSortMSD(..)
}

    // Radix Sort
    // Byte-by-byte sorting

    // 1. derive sort values
    /*std::vector<Value<>> SortValues;
    for (const auto& exp : sortExpressions) {
        sortValues.emplace_back(exp->execute(record));
    }*/

    // 2. Store them on the

    // TODO: support multicolumn sorting
    // TODO: support descending order
    // TODO: support column oriented record layouts
    // TODO: depending on the data characteristics, we may want to use a different sorting algorithm

    // Morsel Driven Parallelism
    // 1. Split data into morsels
    // 2. Sort morsels in parallel
    // 3. Merge morsels
    // 4. Repeat 2-3 until only one morsel is left


    // K-way merge

    // evaluate expression and call child operator if expression is valid
    /*if (expression->execute(record)) {
        if (child != nullptr) {
            child->execute(ctx, record);
        }
    }*/
}// namespace NES::Runtime::Execution::Operators