/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Sinks/Formats/SinkFormat.hpp>

namespace NES {
class TupleBuffer;

enum SinkMediumTypes {
    ZMQ_SINK,
    PRINT_SINK,
    KAFKA_SINK,
    FILE_SINK,
    NETWORK_SINK,
    OPC_SINK
};
/**
 * @brief Base class for all data sinks in NES
 * @note this code is not thread safe
 */
class SinkMedium : public Reconfigurable {

  public:
    /**
     * @brief public constructor for data sink
     */
    explicit SinkMedium(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId);

    /**
     * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
     * @Note must be public because of boost serialize
     */
    virtual ~SinkMedium();

    /**
     * @brief virtual method to setup sink
     * @Note this method will be overwritten by derived classes
     */
    virtual void setup() = 0;

    /**
     * @brief virtual method to shutdown sink
     * @Note this method will be overwritten by derived classes
     */
    virtual void shutdown() = 0;

    /**
     * @brief method to write a TupleBuffer
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    virtual bool writeData(TupleBuffer& inputBuffer, WorkerContext& workerContext) = 0;

    /**
     * @brief get the id of the owning plan
     * @return the id
     */
    QuerySubPlanId getParentPlanId() {
        return parentPlanId;
    }

    /**
     * @brief debug function for testing to get number of written buffers
     * @return number of sent buffer
     */
    size_t getNumberOfWrittenOutBuffers();

    /**
     * @brief debug function for testing to get number of written tuples
     * @return number of sent buffer
     */
    size_t getNumberOfWrittenOutTuples();

    /**
     * @brief virtual function to get a string describing the particular sink
     * @Note this function is overwritten by the particular data sink
     * @return string with name and additional information about the sink
     */
    virtual const std::string toString() const = 0;

    /**
   * @brief method to return the current schema of the sink
   * @return schema description of the sink
   */
    SchemaPtr getSchemaPtr() const;

    /**
      * @brief method to return the type
      * @return type
      */
    virtual std::string toString() = 0;

    /**
     * @brief method to get the format as string
     * @return format as string
     */
    std::string getSinkFormat();

    /**T
     * @brief method to return if the sink is appended
     * @return bool indicating append
     */
    bool getAppendAsBool();

    /**
     * @brief method to return if the sink is append or overwrite
     * @return string of mode
     */
    std::string getAppendAsString();

    /**
      * @brief method to return the type of medium
      * @return type of medium
      */
    virtual SinkMediumTypes getSinkMediumType() = 0;

  protected:
    SinkFormatPtr sinkFormat;
    bool append;
    std::atomic_bool schemaWritten;

    QuerySubPlanId parentPlanId;

    size_t sentBuffer;
    size_t sentTuples;
    std::mutex writeMutex;
};

typedef std::shared_ptr<SinkMedium> DataSinkPtr;

}// namespace NES

#endif// INCLUDE_DATASINK_H_
