#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName, FileOutputMode fileOutputMode, FileOutputType fileOutputType) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName, fileOutputMode, fileOutputType));
}

FileSinkDescriptor::FileSinkDescriptor(std::string fileName,
                                       FileOutputMode fileOutputMode,
                                       FileOutputType fileOutputType)
    : fileName(fileName), fileOutputMode(fileOutputMode), fileOutputType(fileOutputType) {}

const std::string& FileSinkDescriptor::getFileName() const {
    return fileName;
}
FileOutputMode FileSinkDescriptor::getFileOutputMode() const {
    return fileOutputMode;
}
FileOutputType FileSinkDescriptor::getFileOutputType() const {
    return fileOutputType;
}
const std::string& FileSinkDescriptor::toString() {
    return "FileSinkDescriptor()";
}
bool FileSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<FileSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<FileSinkDescriptor>();
    return fileOutputType == otherSinkDescriptor->fileOutputType && fileOutputType == otherSinkDescriptor->getFileOutputType() && fileName == otherSinkDescriptor->fileName:
}

}// namespace NES