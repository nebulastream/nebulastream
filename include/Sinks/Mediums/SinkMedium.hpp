#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>
#include <Sinks/Formats/SinkFormat.hpp>

namespace NES {
class TupleBuffer;

/**
 * @brief Base class for all data sinks in NES
 */
class SinkMedium {

  public:
    /**
     * @brief public constructor for data sink
     */
     SinkMedium();

    /**
   * @brief public constructor for data sink with schema provisioning
   */
    SinkMedium(SchemaPtr schema);

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
    virtual bool writeData(TupleBuffer& input_buffer) = 0;

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
    SchemaPtr getSchema() const;

    /**
   * @brief method to set the current schema of the sink
   * @param schema description of the sink
   */
    void setSchema(SchemaPtr pSchema);

    /**
      * @brief method to return the type
      * @return type
      */
    virtual std::string getMediumAsString() = 0;

    std::string getSinkFormat();

    bool getAppend();

    std::string getWriteMode();

  protected:
    SinkFormatPtr sinkFormat;
    bool append;
    SchemaPtr schema;
    bool schemaWritten;


    size_t sentBuffer;
    size_t sentTuples;
};

typedef std::shared_ptr<SinkMedium> DataSinkPtr;

}// namespace NES

#endif// INCLUDE_DATASINK_H_
