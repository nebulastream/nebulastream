#ifndef NES_INCLUDE_SINKS_SINKFORMAT_HPP_
#define NES_INCLUDE_SINKS_SINKFORMAT_HPP_
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>

#include <fstream>
/**
 * @brief this class covers the different output formats that we offer in NES
 */
namespace NES {

enum SinkFormats {
    CSV_FORMAT,
    JSON_FORMAT,
    NES_FORMAT,
    TEXT_FORMAT
};

class SinkFormat {
  public:


    /**
     * @brief constructor for a sink format
     * @param schema
     * @param append
     */
    SinkFormat(SchemaPtr schema, BufferManagerPtr bufferManager);

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return vector of Tuple buffer containing the content of the tuplebuffer
     */
    virtual std::vector<TupleBuffer> getData(TupleBuffer& inputBuffer) = 0;

    /**
    * @brief method to write the schema of the data
    * @return TupleBuffer containing the schema
    */
    virtual std::optional<TupleBuffer> getSchema() = 0;

    /**
     * @brief method to return the format as a string
     * @return format as string
     */
    virtual std::string toString() = 0;

    virtual SinkFormats getSinkFormat() = 0;

  protected:
    SchemaPtr schema;
    BufferManagerPtr bufferManager;
};

typedef std::shared_ptr<SinkFormat> SinkFormatPtr;

}// namespace NES
#endif//NES_INCLUDE_SINKS_SINKFORMAT_HPP_
