#ifndef FileSink_HPP
#define FileSink_HPP

#include <Sinks/Mediums/SinkMedium.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief this class implements the File sing
 */
class FileSink : public SinkMedium {
  public:
    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param format in which the data is written
     * @param filePath location of file on sink server
     * @param modus of writting (overwrite or append)
     */
    FileSink(SinkFormatPtr format, std::string filePath, bool append);

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
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
    const std::string toString() const override;

    /**
     * @brief get file path
     */
    const std::string getFilePath() const;

    /**
     * @brief Get sink type
     */
    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();
  protected:
    std::string filePath;
};
typedef std::shared_ptr<FileSink> FileSinkPtr;
}// namespace NES

#endif// FileSink_HPP
