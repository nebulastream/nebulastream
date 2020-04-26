#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

enum FileOutPutType {
    BINARY_TYPE, CSV_TYPE
};

enum FileOutPutMode {
    FILE_OVERWRITE, FILE_APPEND
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
     * @return
     */
    static SinkDescriptorPtr create(SchemaPtr schema, std::string filePath, FileOutPutMode fileOutputMode, FileOutPutType fileOutputType);

    SinkDescriptorType getType() override;

    /**
     * @brief Get the file name where the data is to be written
     */
    const std::string& getFileName() const;

    /**
     * @brief get the file output mode (Overwritten or Append)
     */
    FileOutPutMode getFileOutPutMode() const;

    /**
     * @brief get the file output type (CSV or Binary)
     */
    FileOutPutType getFileOutPutType() const;

  private:
    FileSinkDescriptor(SchemaPtr schema, std::string fileName, FileOutPutMode fileOutputMode, FileOutPutType fileOutputType);

    std::string fileName;
    FileOutPutMode fileOutPutMode;
    FileOutPutType fileOutPutType;
};

typedef std::shared_ptr<FileSinkDescriptor> FileSinkDescriptorPtr;
}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
