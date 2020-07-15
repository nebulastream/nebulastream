#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>

namespace NES {
class TupleBuffer;

enum SinkType {
    ZMQ_SINK,
    FILE_SINK,
    KAFKA_SINK,
    PRINT_SINK,
    YSB_SINK,
    NETWORK_SINK
};

/**
 * @brief Base class for all data sinks in NES
 */
class DataSink {

  public:
    /**
     * @brief public constructor for data sink
     */
    DataSink();

    /**
   * @brief public constructor for data sink with schema provisioning
   */
    DataSink(SchemaPtr schema);

    /**
     * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
     * @Note must be public because of boost serialize
     */
    virtual ~DataSink();

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
     * @brief method to write a vector of TupleBuffers
     * @param vector of tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeDataInBatch(std::vector<TupleBuffer>& input_buffers);

    /**
     * @brief method to write a TupleBuffer
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    virtual bool writeData(TupleBuffer& input_buffer) = 0;

    /**
     * @brief debug function for testing to get number of sent buffers
     * @return number of sent buffer
     */
    size_t getNumberOfSentBuffers();

    /**
     * @brief debug function for testing to get number of sent tuples
     * @return number of sent buffer
     */
    size_t getNumberOfSentTuples();

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
    SchemaPtr getSchema() const;

    /**
   * @brief method to set the current schema of the sink
   * @param schema description of the sink
   */
    void setSchema(SchemaPtr pSchema);

    virtual SinkType getType() const = 0;

  protected:
    SchemaPtr schema;
    size_t sentBuffer;
    size_t sentTuples;
};

typedef std::shared_ptr<DataSink> DataSinkPtr;

}// namespace NES

#endif// INCLUDE_DATASINK_H_
