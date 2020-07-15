#ifndef FileSink_HPP
#define FileSink_HPP

#include <Sinks/DataSink.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief this class implements the File sing
 */
class FileSink : public DataSink {

  public:
    /**
     * @brief default constructor that creates an empty file sink
     */
    FileSink();

    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param format in which the data is written
     * @param filePath location of file on sink server
     * @param modus of writting (overwrite or append)
     */
    FileSink(SchemaPtr schema, SinkFormat format, std::string filePath, FileOutputMode mode);

    /**
     * @brief method to override virtual setup function
     * @Note currently the method does nothing
     */
    void setup() override;

    /**
     * @brief method to override virtual shutdown function
     * @Note currently the method does nothing
     */
    void shutdown() override;

    /**
     * @brief method to write a TupleBuffer
     * @param a tuple buffers pointer
     * @return bool indicating if the write was complete
     */
    bool writeData(TupleBuffer& input_buffer);

    /**
     * @brief method to write the schema of the data
     */
    bool writeSchema();

    /**
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
    const std::string toString() const override;

    /**
     * @brief get file path
     */
    const std::string getFilePath() const;

    /**
     * @brief Get file output mode
     */
    FileOutputMode getOutputMode() const;

    /**
     * @brief Get sink type
     */
    SinkMedium getType() const override;

    SinkFormat getSinkFormat() const;

    std::string getFileOutputModeAsString();

  protected:
    std::string filePath;
    FileOutputMode fileOutputMode;
};
typedef std::shared_ptr<FileSink> FileSinkPtr;
}// namespace NES

#endif// FileSink_HPP
