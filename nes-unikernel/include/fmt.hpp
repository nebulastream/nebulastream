#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <fmt/format.h>

namespace fmt {
template<>
struct formatter<NES::Runtime::Execution::Operators::NonKeyedSlice> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::NonKeyedSlice& slice, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "NonKeyedSlice(start={}, end={})", slice.getStart(), slice.getEnd());
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
struct formatter<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler> : formatter<std::string> {
    auto format(const NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler& handler, format_context& ctx)
        -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "NonKeyedSliceMergingHandler(defaultState={})", *handler.getDefaultState());
    }
};
}// namespace fmt
