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
#ifndef NES_STREAMJOINBUILD_HPP
#define NES_STREAMJOINBUILD_HPP

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

class StreamHashJoinBuild;
using StreamHashJoinBuildPtr = std::shared_ptr<StreamHashJoinBuild>;

class TimeFunction;
/**
 * @brief This class is the first phase of the StreamJoin. Each thread builds a LocalHashTable until the window is finished.
 * Then, each threads inserts the LocalHashTable into the SharedHashTable.
 * Afterwards, the second phase (StreamJoinSink) will start if both sides of the join have seen the end of the window.
 */
class StreamHashJoinBuild : public ExecutableOperator {

  public:
    /**
     * @brief Constructors for a StreamJoinBuild
     * @param handlerIndex
     * @param isLeftSide
     * @param joinFieldName
     * @param timeStampField
     * @param schema
     */
    StreamHashJoinBuild(uint64_t handlerIndex,
                        bool isLeftSide,
                        const std::string& joinFieldName,
                        const std::string& timeStampField,
                        SchemaPtr schema,
                        std::shared_ptr<TimeFunction> timeFunction);

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
     * @brief Updates the watermark and if needed, pass some windows to the second join phase (NLJSink) for further processing
     * @param ctx
     * @param recordBuffer
     */
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    uint64_t handlerIndex;
    bool isLeftSide;
    std::string joinFieldName;
    std::string timeStampField;
    SchemaPtr schema;
    std::shared_ptr<TimeFunction> timeFunction;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_STREAMJOINBUILD_HPP
