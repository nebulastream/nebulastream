#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

enum FileOutputType {
    BINARY_TYPE,
    CSV_TYPE
};

enum FileOutputMode {
    FILE_OVERWRITE,
    FILE_APPEND
};

/**
 * @brief Descriptor defining properties used for creating physical file sink
 */
class FileSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new file sink descriptor
     * @param schema output schema of this sink descriptor
     * @param filePath the path to the output file
     * @param fileOutputMode the file output mode (FILE_OVERWRITE, FILE_APPEND)
     * @param fileOutputType the file output type (BINARY_TYPE, CSV_TYPE)
     * @return descriptor for file sink
     */
    static SinkDescriptorPtr create(std::string filePath, FileOutputMode fileOutputMode, FileOutputType fileOutputType);

    /**
     * @brief Get the file name where the data is to be written
     */
    const std::string& getFileName() const;

    /**
     * @brief get the file output mode (Overwritten or Append)
     */
    FileOutputMode getFileOutputMode() const;

    /**
     * @brief get the file output type (CSV or Binary)
     */
    FileOutputType getFileOutputType() const;

  private:
    explicit FileSinkDescriptor(std::string fileName, FileOutputMode fileOutputMode, FileOutputType fileOutputType);

    std::string fileName;
    FileOutputMode fileOutputMode;
    FileOutputType fileOutputType;
};

typedef std::shared_ptr<FileSinkDescriptor> FileSinkDescriptorPtr;
}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
