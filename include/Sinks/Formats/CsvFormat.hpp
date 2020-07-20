#ifndef NES_INCLUDE_SINKS_FORMATS_CSVFORMAT_HPP_
#define NES_INCLUDE_SINKS_FORMATS_CSVFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class CsvFormat : public SinkFormat {
  public:
    CsvFormat(SchemaPtr schema, std::string filePath, bool append);

    /**
   * @brief method to write a TupleBuffer in csv format
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
}
#endif//NES_INCLUDE_SINKS_FORMATS_CSVFORMAT_HPP_
