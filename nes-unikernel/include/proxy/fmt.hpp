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

#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <fmt/format.h>

namespace fmt {

template<>
struct formatter<NES::Runtime::Execution::WindowInfo> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::WindowInfo& window, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "WindowInfo(start={}, end={}, id={})",
                              window.windowStart,
                              window.windowEnd,
                              window.windowId);
    }
};
template<>
struct formatter<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask& task, format_context& ctx)
        -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "EmittedNLJWindowTriggerTask(window={}, leftIdentifier={}, rightIdentifier={})",
                              task.windowInfo,
                              task.leftSliceIdentifier,
                              task.rightSliceIdentifier);
    }
};
template<>
struct formatter<NES::Nautilus::Interface::PagedVector> : formatter<std::string> {
    auto format(const NES::Nautilus::Interface::PagedVector& vector, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "PagedVector(capacity={}, pageSize={}, numberOfEntriesOnCurrentPage={}, numberOfEntries={})",
                              vector.getCapacityPerPage(),
                              vector.getPageSize(),
                              vector.getNumberOfEntriesOnCurrentPage(),
                              vector.getNumberOfEntries());
    }
};
template<>
struct formatter<NES::Runtime::Execution::NLJSlice> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::NLJSlice& slice, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "NLJSlice(identifier={}, start={}, end={})",
                              slice.getSliceIdentifier(),
                              slice.getSliceStart(),
                              slice.getSliceEnd());
    }
};
template<>
struct formatter<NES::Runtime::Execution::Operators::NonKeyedSlice> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::NonKeyedSlice& slice, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "NonKeyedSlice(start={}, end={})", slice.getStart(), slice.getEnd());
    }
};

template<>
struct formatter<NES::Runtime::Execution::Operators::KeyedSlice> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::KeyedSlice& slice, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "KeyedSlice(start={}, end={})", slice.getStart(), slice.getEnd());
    }
};

template<>
struct formatter<NES::Runtime::Execution::Operators::State> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::State& state, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "State(stateSize={})", state.stateSize);
    }
};
template<>
struct formatter<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>>
    : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>& task,
                format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "SliceMergeTask<NonKeyedSlice>(seq={}, start={}, end={})",
                              task.sequenceNumber,
                              task.startSlice,
                              task.endSlice);
    }
};
template<>
struct formatter<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>>
    : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>& task,
                format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "SliceMergeTask<KeyedSlice>(seq={}, start={}, end={})",
                              task.sequenceNumber,
                              task.startSlice,
                              task.endSlice);
    }
};
}// namespace fmt
