#ifndef NES_INCLUDE_SINKS_FORMATS_TEXTFORMAT_HPP_
#define NES_INCLUDE_SINKS_FORMATS_TEXTFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class TextFormat : public SinkFormat {
  public:
    TextFormat(SchemaPtr schema, BufferManagerPtr bufferManager);

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return vector of Tuple buffer containing the content of the tuplebuffer
     */
    std::vector<TupleBuffer> getData(TupleBuffer& inputBuffer);

    /**
    * @brief method to write the schema of the data
    * @return TupleBuffer containing the schema
    */
    std::optional<TupleBuffer> getSchema();

    /**
   * @brief method to return the format as a string
   * @return format as string
   */
    std::string toString();
};
}// namespace NES
#endif//NES_INCLUDE_SINKS_FORMATS_TEXTFORMAT_HPP_
