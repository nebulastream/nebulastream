#include <Nodes/Operators/OperatorNode.hpp>
#include "Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp"

namespace NES {

FileSinkDescriptor::FileSinkDescriptor(SchemaPtr schema,
                                       std::string fileName,
                                       FileOutPutMode fileOutputMode,
                                       FileOutPutType fileOutputType)
    : SinkDescriptor(schema), fileName(fileName), fileOutPutMode(fileOutputMode), fileOutPutType(fileOutPutType) {}

SinkDescriptorType FileSinkDescriptor::getType() {
    return SinkDescriptorType::FileDescriptor;
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