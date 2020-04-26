#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(SchemaPtr schema, std::string fileName, FileOutPutMode fileOutputMode, FileOutPutType fileOutputType) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(schema, fileName, fileOutputMode, fileOutputType));
}

FileSinkDescriptor::FileSinkDescriptor(SchemaPtr schema,
                                       std::string fileName,
                                       FileOutPutMode fileOutputMode,
                                       FileOutPutType fileOutputType)
    : SinkDescriptor(schema), fileName(fileName), fileOutPutMode(fileOutputMode), fileOutPutType(fileOutPutType) {}

SinkDescriptorType FileSinkDescriptor::getType() {
    return FileSinkDescriptorType;
}

const std::string& FileSinkDescriptor::getFileName() const {
    return fileName;
}
FileOutPutMode FileSinkDescriptor::getFileOutPutMode() const {
    return fileOutPutMode;
}
FileOutPutType FileSinkDescriptor::getFileOutPutType() const {
    return fileOutPutType;
}

}