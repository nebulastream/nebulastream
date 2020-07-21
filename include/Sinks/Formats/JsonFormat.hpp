#ifndef NES_INCLUDE_SINKS_FORMATS_JSONFORMAT_HPP_
#define NES_INCLUDE_SINKS_FORMATS_JSONFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class JsonFormat : public SinkFormat {
  public:
    JsonFormat(SchemaPtr schema, std::string filePath, bool append);

    /**
   * @brief method to write a TupleBuffer in json format
   * @param a tuple buffers pointer
    * @return bool indicating if the write was complete
    */
    bool writeData(TupleBuffer& inputBuffer);

    /**
    * @brief method to write the schema of the data
    */
    bool writeSchema();

    std::string toString();
};
}// namespace NES
#endif//NES_INCLUDE_SINKS_FORMATS_JSONFORMAT_HPP_
