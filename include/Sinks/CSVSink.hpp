#ifndef INCLUDE_CSVSINK_HPP_
#define INCLUDE_CSVSINK_HPP_

#include <Sinks/FileOutputSink.hpp>

namespace NES {
class TupleBuffer;

class CSVSink : public FileOutputSink {
  public:
    /**
     * @brief schema and path constructor
     * @param schema the scema to expect as output
     * @param filepath the path to perform outout
     */
    explicit CSVSink(SchemaPtr schema, std::string filepath);

    /**
     * @brief schema, path, and output mode constructor
     * @param schema the scema to expect as output
     * @param filepath the path to perform outout
     * @param mode enum between APPEND and OVERWRITE
     */
    explicit CSVSink(SchemaPtr schema, std::string filePath, FileOutputMode mode);

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
     * @return the file output, as string lines with comma separation
     */
    std::string outputBufferWithSchema(TupleBuffer& tupleBuffer, SchemaPtr schema) const override;
};

typedef std::shared_ptr<CSVSink> CSVSinkPtr;
}// namespace NES

#endif//INCLUDE_CSVSINK_HPP_
