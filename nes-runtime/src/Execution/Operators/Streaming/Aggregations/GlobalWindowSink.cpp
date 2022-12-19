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
#include <Execution/Operators/Streaming/Aggregations/GlobalSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalWindowSink.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalWindowSinkHandler.hpp>
#include <Execution/Operators/Streaming/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

GlobalWindowSink::GlobalWindowSink(const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions)
    : aggregationFunctions(aggregationFunctions) {}

void GlobalWindowSink::setup(ExecutionContext&) const {}

void GlobalWindowSink::open(ExecutionContext&, RecordBuffer&) const {}

void GlobalWindowSink::execute(ExecutionContext& ctx, Record& record) const {
    auto sliceState = record.read("sliceState").as<MemRef>();
    Record resultWindow;
    for (const auto& function : aggregationFunctions) {
        auto finalAggregationValue = function->lower(sliceState);
        resultWindow.write("f1", finalAggregationValue);
        sliceState = sliceState + function->getSize();
    }
    child->execute(ctx, resultWindow);
}

void GlobalWindowSink::close(ExecutionContext&, RecordBuffer&) const {}

}// namespace NES::Runtime::Execution::Operators