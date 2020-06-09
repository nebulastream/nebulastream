#ifndef INCLUDE_CSVSINK_HPP_
#define INCLUDE_CSVSINK_HPP_

#include <SourceSink/FileOutputSink.hpp>

namespace NES {
class TupleBuffer;

class CSVSink : public FileOutputSink {
  public:
    /**
     * @brief
     * @param schema
     * @param filepath
     */
    explicit CSVSink(SchemaPtr schema, std::string filepath);

    /**
     * @brief
     * @param schema
     * @param filePath
     * @param mode
     */
    explicit CSVSink(SchemaPtr schema, std::string filePath, FileOutputMode mode);

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

typedef std::shared_ptr<CSVSink> CSVSinkPtr;
}// namespace NES

#endif//INCLUDE_CSVSINK_HPP_
