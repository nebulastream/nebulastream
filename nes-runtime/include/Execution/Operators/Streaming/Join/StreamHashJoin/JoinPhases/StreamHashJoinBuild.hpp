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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_JOINPHASES_STREAMHASHJOINBUILD_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_JOINPHASES_STREAMHASHJOINBUILD_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {

class StreamHashJoinBuild;
using StreamHashJoinBuildPtr = std::shared_ptr<StreamHashJoinBuild>;

class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;
/**
 * @brief This class is the first phase of the StreamJoin. Each thread builds a LocalHashTable until the slice is finished.
 * Then, each threads inserts the LocalHashTable into the SharedHashTable.
 * Afterwards, the second phase (StreamJoinSink) will start if both sides of the join have seen the end of the slice.
 */
class StreamHashJoinBuild : public ExecutableOperator {

  public:
    /**
     * @brief Constructors for a StreamJoinBuild
     * @param handlerIndex
     * @param joinBuildSide
     * @param joinFieldName
     * @param timeStampField
     * @param schema
     */
    StreamHashJoinBuild(uint64_t handlerIndex,
                        const QueryCompilation::JoinBuildSideType joinBuildSide,
                        const std::string& joinFieldName,
                        const std::string& timeStampField,
                        SchemaPtr inputSchema,
                        TimeFunctionPtr timeFunction);

    /**
     * @brief Setting up the pipeline by initializing the operator handler
     * @param executionCtx
     */
    void setup(ExecutionContext& executionCtx) const override;

    /**
     * @brief builds a hash table with the record
     * @param ctx
     * @param record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
     * @brief Updates the watermark and if needed, pass some slices to the second join phase (StreamHashJoinProbe) for further processing
     * @param ctx
     * @param recordBuffer
     */
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Open is called for each record buffer and is used to initializes execution local state.
     * @param recordBuffer
     */
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    uint64_t handlerIndex;
    const QueryCompilation::JoinBuildSideType joinBuildSide;
    std::string joinFieldName;
    std::string timeStampField;
    SchemaPtr inputSchema;
    TimeFunctionPtr timeFunction;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_JOINPHASES_STREAMHASHJOINBUILD_HPP_
