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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINBUILDBUCKETING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINBUILDBUCKETING_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>

namespace NES::Runtime::Execution::Operators {
class StreamJoinBuildBucketing : public StreamJoinBuild {
  public:
    /**
     * @brief This class stores the no. windows to fill and the pointer to the windows for a given range
     */
    class LocalStateBucketing : public OperatorState {
      public:
        explicit LocalStateBucketing(const Value<MemRef>& allWindowsToFill)
            : allWindowsToFill(allWindowsToFill), numWindowsToFill(0_u64), minWindowStart(0_u64), maxWindowEnd(0_u64) {}

        Value<MemRef> allWindowsToFill;
        Value<UInt64> numWindowsToFill;
        Value<UInt64> minWindowStart;
        Value<UInt64> maxWindowEnd;
    };

    /**
     * @brief Constructor for a StreamJoinBuildBucketing
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     * @param entrySize
     * @param timeFunction
     * @param joinStrategy
     * @param windowingStrategy
     * @param windowSize
     * @param windowSlide
     */
    StreamJoinBuildBucketing(const uint64_t operatorHandlerIndex,
                             const SchemaPtr& schema,
                             const std::string& joinFieldName,
                             const QueryCompilation::JoinBuildSideType joinBuildSide,
                             const uint64_t entrySize,
                             TimeFunctionPtr timeFunction,
                             QueryCompilation::StreamJoinStrategy joinStrategy,
                             QueryCompilation::WindowingStrategy windowingStrategy,
                             uint64_t windowSize,
                             uint64_t windowSlide);

    ~StreamJoinBuildBucketing() override = default;

    /**
     * @brief Updates the local hash join state, so that the correct windows are stored for the timestamp (ts)
     * @param localStateBucketing
     * @param opHandlerMemRef
     * @param ts
     * @param workerId
     */
    void updateLocalState(LocalStateBucketing* localStateBucketing, Value<MemRef>& opHandlerMemRef, Value<UInt64>& ts, Value<UInt64>& workerId) const;

    /**
     * @brief Checks if the current local hash join state is up-to-date, meaning the correct windows are stored
     * @param ts
     * @param localStateBucketing
     * @return Boolean
     */
    Value<Boolean> checkIfLocalStateUpToDate(Value<UInt64>& ts, LocalStateBucketing* localStateBucketing) const;

    /**
     * @brief Calculates the minimum window start for the timestamp (ts).
     * @param ts
     * @return Value<UINT64>
     */
    Value<UInt64> getMinWindowStartForTs(Value<UInt64>& ts) const;

    /**
     * @brief Calculates the maximum window end for the timestamp (ts).
     * @param ts
     * @return Value<UINT64>
     */
    Value<UInt64> getMaxWindowStartForTs(Value<UInt64>& ts) const;

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
     * @brief Inserts the record for the current index
     * @param allWindowsToFill
     * @param curIndex
     * @param workerId
     * @param record
     */
    virtual void insertRecordForWindow(Value<MemRef>& allWindowsToFill, Value<UInt64>& curIndex,
                                       Value<UInt64>& workerId, Record& record) const = 0;

  private:
    /**
     * @brief Calculates the last start for the given timestamp (ts)
     * @param ts
     * @return Value<INT64>
     */
    inline Value<UInt64> calcLastStartForTs(Value<UInt64>& ts) const;

    /**
     * @brief Calculates the num of windows for the given timestamp (ts)
     * @param ts
     * @return Value<INT64>
     */
    inline Value<UInt64> calcNumWindows(Value<UInt64>& ts) const;

    uint64_t windowSize;
    uint64_t windowSlide;
};
}
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINBUILDBUCKETING_HPP_
