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
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

class StreamJoinBuild;
using StreamJoinBuildPtr = std::shared_ptr<StreamJoinBuild>;

/**
 * @brief This class is the first phase of the StreamJoin. Each thread builds a LocalHashTable until the window is finished.
 * Then, each threads inserts the LocalHashTable into the SharedHashTable.
 * Afterwards, the second phase (StreamJoinSink) will start if both sides of the join have seen the end of the window.
 */
class StreamJoinBuild : public ExecutableOperator {

  public:
    /**
     * @brief Constructors for a StreamJoinBuild
     * @param handlerIndex
     * @param isLeftSide
     * @param joinFieldName
     * @param timeStampField
     * @param schema
     */
    StreamJoinBuild(uint64_t handlerIndex,
                    bool isLeftSide,
                    const std::string& joinFieldName,
                    const std::string& timeStampField,
                    SchemaPtr schema);

    /**
     * @brief builds a hash table with the record
     * @param ctx
     * @param record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    uint64_t handlerIndex;
    bool isLeftSide;
    std::string joinFieldName;
    std::string timeStampField;
    SchemaPtr schema;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_STREAMJOINBUILD_HPP
