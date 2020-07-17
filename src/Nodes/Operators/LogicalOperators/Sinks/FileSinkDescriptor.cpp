#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {



SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName, "CSV_FORMAT", true));
}

SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName, std::string sinkFormat, bool append) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName, sinkFormat, append));
}

FileSinkDescriptor::FileSinkDescriptor(std::string fileName, std::string sinkFormat, bool append)
    : fileName(fileName), sinkFormat(sinkFormat), append(append) {}

const std::string& FileSinkDescriptor::getFileName() const {
    return fileName;
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

bool FileSinkDescriptor::getAppend()
{
    return append;
}

std::string FileSinkDescriptor::getSinkFormatAsString()
{
    return sinkFormat;
}

}// namespace NES