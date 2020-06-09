#ifndef INCLUDE_BINARYSINK_HPP_
#define INCLUDE_BINARYSINK_HPP_

#include <SourceSink/FileOutputSink.hpp>

namespace NES {
class TupleBuffer;

class BinarySink : public FileOutputSink {
  public:
    /**
     * @brief path constructor
     * @param filePath
     */
    explicit BinarySink(std::string filePath);

    /**
     * @brief schema and path constructor
     * @param schema the scema to expect as output
     * @param mode enum between APPEND and OVERWRITE
     */
    explicit BinarySink(SchemaPtr schema, std::string filePath);

    /**
     * @brief implementation of data output
     * @param input_buffer
     * @return true if the tuples have been written, false otherwise
     */
    bool writeData(TupleBuffer& input_buffer);

  private:
    /**
     * @brief creates string lines of output
     * @param tupleBuffer
     * @param schema
     * @return the file output, in binary format
     */
    std::string outputBufferWithSchema(TupleBuffer& tupleBuffer, SchemaPtr schema) const override;
};

typedef std::shared_ptr<BinarySink> BinarySinkPtr;
}// namespace NES

#endif//INCLUDE_BINARYSINK_HPP_
