#include "Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp"

namespace NES {

FileSinkDescriptor::FileSinkDescriptor(std::string fileName,
                                       FileOutPutMode fileOutputMode,
                                       FileOutPutType fileOutputType)
    : fileName(fileName), fileOutPutMode(fileOutputMode), fileOutPutType(fileOutPutType) {}

SinkDescriptorType FileSinkDescriptor::getType() {
    return FileDescriptor;
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