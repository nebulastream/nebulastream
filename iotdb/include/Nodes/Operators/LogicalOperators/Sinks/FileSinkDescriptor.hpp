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

class FileSinkDescriptor : public SinkDescriptor {

  public:

    FileSinkDescriptor(SchemaPtr schema, std::string fileName, FileOutPutMode fileOutputMode, FileOutPutType fileOutputType);

    SinkDescriptorType getType() override;

    const std::string& getFileName() const;
    FileOutPutMode getFileOutPutMode() const;
    FileOutPutType getFileOutPutType() const;
  private:

    FileSinkDescriptor() = default;

    std::string fileName;
    FileOutPutMode fileOutPutMode;
    FileOutPutType fileOutPutType;
};

typedef std::shared_ptr<FileSinkDescriptor> FileSinkDescriptorPtr;
}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
