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

#ifndef NES_INCLUDE_SINKS_MEDIUMS_SINK_MEDIUM_HPP_
#define NES_INCLUDE_SINKS_MEDIUMS_SINK_MEDIUM_HPP_

#include <API/Schema.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Sinks/Formats/SinkFormat.hpp>
#include <mutex>

namespace NES {

enum SinkMediumTypes { ZMQ_SINK, PRINT_SINK, KAFKA_SINK, FILE_SINK, NETWORK_SINK, OPC_SINK, MQTT_SINK, NULL_SINK };
/**
 * @brief Base class for all data sinks in NES
 * @note this code is not thread safe
 */
class SinkMedium : public Runtime::Reconfigurable {

  public:
    /**FF
     * @brief public constructor for data sink
     */
    explicit SinkMedium(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId);

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
    virtual bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) = 0;

    /**
     * @brief get the id of the owning plan
     * @return the id
     */
    QuerySubPlanId getParentPlanId() const { return parentPlanId; }

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
     *
     */
    virtual uint64_t getOperatorId();
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

    /**T
     * @brief method to return if the sink is appended
     * @return bool indicating append
     */
    bool getAppendAsBool() const;

    /**
     * @brief method to return if the sink is append or overwrite
     * @return string of mode
     */
    std::string getAppendAsString() const;

    /**
      * @brief method to return the type of medium
      * @return type of medium
      */
    virtual SinkMediumTypes getSinkMediumType() = 0;

    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override;
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  protected:
    SinkFormatPtr sinkFormat;
    bool append{false};
    std::atomic_bool schemaWritten{false};

    QuerySubPlanId parentPlanId;

    uint64_t sentBuffer{0};
    uint64_t sentTuples{0};
    std::mutex writeMutex{};
};

using DataSinkPtr = std::shared_ptr<SinkMedium>;

}// namespace NES

#endif// NES_INCLUDE_SINKS_MEDIUMS_SINK_MEDIUM_HPP_
