#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDWINDOWEMITACTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDWINDOWEMITACTION_HPP_
#include <Execution/Operators/Streaming/Aggregations/SliceMergingAction.hpp>
namespace NES::Runtime::Execution::Operators {

class NonKeyedWindowEmitAction : public SliceMergingAction {
  public:
    NonKeyedWindowEmitAction(const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                             const std::string& startTsFieldName,
                             const std::string& endTsFieldName,
                             uint64_t resultOriginId);

    void emitSlice(ExecutionContext& ctx,
                   ExecuteOperatorPtr& child,
                   Value<UInt64>& windowStart,
                   Value<UInt64>& windowEnd,
                   Value<UInt64>& sequenceNumber,
                   Value<MemRef>& globalSlice) const override;
  private:
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::string startTsFieldName;
    const std::string endTsFieldName;
    const uint64_t resultOriginId;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDWINDOWEMITACTION_HPP_
