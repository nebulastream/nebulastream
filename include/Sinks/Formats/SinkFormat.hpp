#ifndef NES_INCLUDE_SINKS_SINKFORMAT_HPP_
#define NES_INCLUDE_SINKS_SINKFORMAT_HPP_
#include <NodeEngine/TupleBuffer.hpp>
#include <fstream>
/**
 * @brief this class covers the different output formats that we offer in NES
 */
namespace NES {

class SinkFormat {
  public:
    SinkFormat(SchemaPtr schema, std::string filePath, bool append);

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return bool indicating if the write was complete
     */
    virtual bool writeData(TupleBuffer& inputBuffer) = 0;

    /**
    * @brief method to write the schema of the data
     * @note this will reset the current file
    */
    virtual bool writeSchema() = 0;

    virtual std::string getFormatAsString() = 0;

  protected:
    SchemaPtr schema;
    std::string filePath;
    bool append;
};

typedef std::shared_ptr<SinkFormat> SinkFormatPtr;

}// namespace NES
#endif//NES_INCLUDE_SINKS_SINKFORMAT_HPP_
