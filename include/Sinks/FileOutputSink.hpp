#ifndef FILEOUTPUTSINK_HPP
#define FILEOUTPUTSINK_HPP

#include <Sinks/DataSink.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief this class implements the File output sink
 * @default default type is Binary and default mode is append
 */
class FileOutputSink : public DataSink {

  public:
    enum FileOutputType {
        BINARY_TYPE,
        CSV_TYPE
    };

    enum FileOutputMode {
        FILE_OVERWRITE,
        FILE_APPEND
    };

    /**
     * @brief default constructor that creates an empty file sink
     */
    FileOutputSink();

    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     */
    FileOutputSink(SchemaPtr schema);

    /**
    * @brief constructor that creates an empty file sink using a schema
    * @param filePath location of file on sink server
    */
    FileOutputSink(std::string filePath);

    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param filePath location of file on sink server
     */
    FileOutputSink(SchemaPtr schema, std::string filePath);

    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param filePath location of file on sink server
     * @param type of file to write
     * @param mode to write
     */
    FileOutputSink(SchemaPtr schema, std::string filePath, FileOutputType type, FileOutputMode mode);

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
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
    const std::string toString() const override;

    /**
     * @brief get file path
     */
    const std::string getFilePath() const;

    /**
     * @brief Get file output type
     */
    FileOutputType getOutputType() const;

    /**
     * @brief Get file output mode
     */
    FileOutputMode getOutputMode() const;

    /**
     * @brief Get sink type
     */
    SinkType getType() const override;

  protected:
    std::string filePath;

    FileOutputType outputType;
    FileOutputMode outputMode;

    /**
     * @brief Manipulate a tupleBuffer with a schema as specific output.
     * @param tupleBuffer
     * @param schema
     * @return formatted output according to the derived class
     */
    virtual std::string outputBufferWithSchema(TupleBuffer& tupleBuffer, SchemaPtr schema) const = 0;
};
typedef std::shared_ptr<FileOutputSink> FileOutputSinkPtr;
}// namespace NES

#endif// FILEOUTPUTSINK_HPP
