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

#ifndef INCLUDE_DATASOURCE_H_
#define INCLUDE_DATASOURCE_H_

#include <API/Schema.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <Operators/OperatorId.hpp>
#include <Windowing/Watermark/Watermark.hpp>
#include <atomic>
#include <mutex>
#include <optional>
#include <thread>
namespace NES {
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class TupleBuffer;

enum SourceType {
    OPC_SOURCE,
    ZMQ_SOURCE,
    CSV_SOURCE,
    KAFKA_SOURCE,
    TEST_SOURCE,
    BINARY_SOURCE,
    SENSE_SOURCE,
    DEFAULT_SOURCE,
    NETWORK_SOURCE,
    ADAPTIVE_SOURCE,
    YSB_SOURCE
};

/**
 * @brief Base class for all data sources in NES
 */
class DataSource : public Reconfigurable {
  public:
    /**
     * @brief public constructor for data source
     * @Note the number of buffers to process is set to UINT64_MAX and the value is needed
     * by some test to produce a deterministic behavior
     * @param schema of the data that this source produces
     */
    explicit DataSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, OperatorId operatorId);

    DataSource() = delete;

    /**
     * @brief method to start the source.
     * 1.) check if bool running is true, if true return if not start source
     * 2.) start new thread with runningRoutine
     */
    virtual bool start();

    /**
     * @brief method to stop the source.
     * 1.) check if bool running is false, if false return, if not stop source
     * 2.) stop thread by join
     */
    virtual bool stop();

    /**
     * @brief running routine while source is active
     * 1.) check if running is still true
     * 2.) check if max number of buffer to produced is reached (num_buffers_to_process)
     * 3.) If not call receiveData in a blocking fashion
     * 4.) If call returns and a buffer is there to process, add a task to the dispatcher
     */
    virtual void runningRoutine(BufferManagerPtr bufferManager, QueryManagerPtr queryManager);

    /**
     * @brief virtual function to receive a buffer
     * @Note this function is overwritten by the particular data source
     * @return returns a tuple buffer
     */
    virtual std::optional<TupleBuffer> receiveData() = 0;

    /**
     * @brief virtual function to get a string describing the particular source
     * @Note this function is overwritten by the particular data source
     * @return string with name and additional information about the source
     */
    virtual const std::string toString() const = 0;

    /**
     * @brief get source Type
     * @return
     */
    virtual SourceType getType() const = 0;

    /**
     * @brief method to return the current schema of the source
     * @return schema description of the source
     */
    SchemaPtr getSchema() const;

    /**
     * @brief method to return the current schema of the source as string
     * @return schema description of the source as string
     */
    std::string getSourceSchemaAsString();

    /**
     * @brief debug function for testing to test if source is running
     * @return bool indicating if source is running
     */
    virtual bool isRunning();

    /**
     * @brief debug function for testing to set different buffer counts to be processed
     * @param number of buffers to be processed
     */
    void setNumBuffersToProcess(size_t cnt);

    /**
     * @brief debug function for testing to get number of generated tuples
     * @return number of generated tuples
     */
    size_t getNumberOfGeneratedTuples();

    /**
     * @brief debug function for testing to get number of generated buffer
     * @return number of generated buffer
     */
    size_t getNumberOfGeneratedBuffers();

    /**
     * @brief method to set the sampling interval
     * @note the source will sleep for interval seconds and then produce the next buffer
     * @param interal to gather
     */
    void setGatheringInterval(size_t interval);

    /**
     * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
     * @Note must be public because of boost serialize
     */
    virtual ~DataSource();

    /**
     * @brief Get number of buffers to be processed
     */
    size_t getNumBuffersToProcess() const;

    /**
     * @brief Get frequency of gathering the data
     */
    size_t getGatheringInterval() const;

    OperatorId getOperatorId();

  protected:
    SchemaPtr schema;
    size_t generatedTuples;
    size_t generatedBuffers;
    size_t numBuffersToProcess;
    std::atomic<size_t> gatheringInterval;
    OperatorId operatorId;
    SourceType type;
    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    Windowing::WatermarkPtr watermark;

  private:
    //bool indicating if the source is currently running'
    std::mutex startStopMutex;
    std::atomic_bool running;
    std::shared_ptr<std::thread> thread;
};

typedef std::shared_ptr<DataSource> DataSourcePtr;

}// namespace NES

#endif /* INCLUDE_DATASOURCE_H_ */
