#ifndef INCLUDE_BINARYSINK_HPP_
#define INCLUDE_BINARYSINK_HPP_

#include <SourceSink/FileOutputSink.hpp>

namespace NES {
class TupleBuffer;

class BinarySink : public FileOutputSink {
  public:
    /**
     * @brief
     * @param filePath
     */
    explicit BinarySink(std::string filePath);

    /**
     * @brief
     * @param schema
     * @param filePath
     */
    explicit BinarySink(SchemaPtr schema, std::string filePath);

    /**
     * @brief
     * @param input_buffer
     * @return
     */
    bool writeData(TupleBuffer& input_buffer);

  private:
    /**
     * @brief
     * @param tupleBuffer
     * @param schema
     * @return
     */
    std::string outputBufferWithSchema(TupleBuffer& tupleBuffer, SchemaPtr schema) const override;
};

typedef std::shared_ptr<BinarySink> BinarySinkPtr;
}// namespace NES

#endif//INCLUDE_BINARYSINK_HPP_
