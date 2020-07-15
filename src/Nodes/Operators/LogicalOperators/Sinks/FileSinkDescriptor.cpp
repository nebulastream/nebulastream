#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName, FileOutputMode fileOutputMode, SinkFormat sinkFormat) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName, fileOutputMode, sinkFormat));
}

FileSinkDescriptor::FileSinkDescriptor(std::string fileName, FileOutputMode fileOutputMode, SinkFormat sinkFormat)
    : fileName(fileName), fileOutputMode(fileOutputMode), sinkFormat(sinkFormat) {}

const std::string& FileSinkDescriptor::getFileName() const {
    return fileName;
}

FileOutputMode FileSinkDescriptor::getFileOutputMode()
{
    return fileOutputMode;
}
SinkFormat FileSinkDescriptor::getSinkFormat()
{
    return sinkFormat;
}

std::string FileSinkDescriptor::toString() {
    return "FileSinkDescriptor()";
}

bool FileSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<FileSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<FileSinkDescriptor>();
    return fileName == otherSinkDescriptor->fileName;
}

}// namespace NES