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

#pragma once
#include <mutex>
#include <Sinks/Formats/SinkFormat.hpp>
#include <Executable.hpp>

namespace NES
{

enum class SinkMediumTypes : uint8_t
{
    PRINT_SINK,
    FILE_SINK,
};

/**
 * @brief Base class for all data sinks in NES
 * @note this code is not thread safe
 */
class SinkMedium : public Runtime::Execution::ExecutablePipelineStage
{
public:
    /**
     * @brief public constructor for data sink
     */
    explicit SinkMedium(SinkFormatPtr sinkFormat, uint32_t numOfProducers, QueryId QueryId);

    /**
     * @brief public constructor for data sink
     */
    explicit SinkMedium(SinkFormatPtr sinkFormat, uint32_t numOfProducers, QueryId queryId, uint64_t numberOfOrigins);

    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override = 0;
    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override
        = 0;
    uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override = 0;

    /**
     * @brief get the id of query
     * @return QueryId
     */
    QueryId getQueryId() const;

    /**
     * @brief debug function for testing to get number of written buffers
     * @return number of sent buffer
     */
    uint64_t getNumberOfWrittenOutBuffers();

    /**
     * @brief debug function for testing to get number of written tuples
     * @return number of sent buffer
     */
    uint64_t getNumberOfWrittenOutTuples();

    /**
     * @brief virtual function to get a string describing the particular sink
     * @Note this function is overwritten by the particular data sink
     * @return string with name and additional information about the sink
     */
    virtual std::string toString() const = 0;

    /**
   * @brief method to return the current schema of the sink
   * @return schema description of the sink
   */
    SchemaPtr getSchemaPtr() const;

    /**
     * @brief method to get the format as string
     * @return format as string
     */
    std::string getSinkFormat();

    /**
      * @brief method to return the type of medium
      * @return type of medium
      */
    virtual SinkMediumTypes getSinkMediumType() = 0;

    /**
     * @brief
     * @return
     */
    OperatorId getOperatorId() const;

protected:
    SinkFormatPtr sinkFormat;
    bool schemaWritten;

    QueryId queryId;
    uint64_t numberOfOrigins;
    uint64_t sentBuffer{0};
    uint64_t sentTuples{0};
    std::recursive_mutex writeMutex;
};

using DataSinkPtr = std::shared_ptr<SinkMedium>;

}
