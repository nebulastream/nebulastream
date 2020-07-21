#ifndef NES_INCLUDE_SINKS_FORMATS_NESFORMAT_HPP_
#define NES_INCLUDE_SINKS_FORMATS_NESFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class NesFormat : public SinkFormat {
  public:
    NesFormat(SchemaPtr schema, std::string filePath, bool append);

    /**
   * @brief method to write a TupleBuffer in nes format
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
#endif//NES_INCLUDE_SINKS_FORMATS_NESFORMAT_HPP_
