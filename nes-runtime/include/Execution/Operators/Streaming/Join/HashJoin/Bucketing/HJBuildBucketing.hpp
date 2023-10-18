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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_BUCKETING_HJBUILDBUCKETING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_BUCKETING_HJBUILDBUCKETING_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinBuildBucketing.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class implements how the hash join inserts records into the multiple buckets by implementing insertRecordForWindow.
 * It iterates over all windows, it has to fill and then inserts the record into each hash table of each window.
 * The insertion is done via a Nautilus::FunctionCall that proxies to the hashTable->insert() and returns a pointer to allocated space for the tuple.
 */
class HJBuildBucketing : public StreamJoinBuildBucketing {
  public:
    /**
     * @brief Constructor for a HJBuildBucketing join phase
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
    HJBuildBucketing(const uint64_t operatorHandlerIndex,
                     const SchemaPtr& schema,
                     const std::string& joinFieldName,
                     const QueryCompilation::JoinBuildSideType joinBuildSide,
                     const uint64_t entrySize,
                     TimeFunctionPtr timeFunction,
                     QueryCompilation::StreamJoinStrategy joinStrategy,
                     QueryCompilation::WindowingStrategy windowingStrategy,
                     const uint64_t windowSize,
                     const uint64_t windowSlide);

    void insertRecordForWindow(Value<MemRef>& allWindowsToFill, Value<UInt64>& curIndex,
                                      Value<UInt64>& workerId, Record& record) const override;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_BUCKETING_HJBUILDBUCKETING_HPP_
