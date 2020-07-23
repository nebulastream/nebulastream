#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>
#include <Sinks/Formats/SinkFormat.hpp>

namespace NES {
class TupleBuffer;

enum SinkMediumTypes{
    ZMQ_SINK,
    PRINT_SINK,
    KAFKA_SINK,
    FILE_SINK,
    NETWORK_SINK
};
/**
 * @brief Base class for all data sinks in NES
 * @note this code is not thread safe
 */
class SinkMedium {

  public:

    /**
     * @brief public constructor for data sink
     */
    SinkMedium(SinkFormatPtr sinkFormat);

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
    virtual bool writeData(TupleBuffer& inputBuffer) = 0;

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

//    /**
//   * @brief method to set the current schema of the sink
//   * @param schema description of the sink
//   */
//    void setSchemaPtr(SchemaPtr pSchema);

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
    bool schemaWritten;

    size_t sentBuffer;
    size_t sentTuples;
};

typedef std::shared_ptr<SinkMedium> DataSinkPtr;

}// namespace NES

#endif// INCLUDE_DATASINK_H_
