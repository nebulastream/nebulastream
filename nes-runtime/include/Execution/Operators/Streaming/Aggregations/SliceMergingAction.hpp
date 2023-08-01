//
// Created by pgrulich on 01.08.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_SLICEMERGINGACTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_SLICEMERGINGACTION_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowType.hpp>
namespace NES::Runtime::Execution::Operators {

class SliceMergingAction {
  public:
    virtual void emitSlice(ExecutionContext& ctx,
                           ExecuteOperatorPtr& child,
                           Value<UInt64>& windowStart,
                           Value<UInt64>& windowEnd,
                           Value<UInt64>& sequenceNumber,
                           Value<MemRef>& globalSlice) const = 0;

    virtual ~SliceMergingAction() = default;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_SLICEMERGINGACTION_HPP_
