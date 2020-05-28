#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName));
}

FileSinkDescriptor::FileSinkDescriptor(std::string fileName)
    : fileName(fileName) {}

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

}// namespace NES