#include <Nodes/Operators/LogicalOperators/Sinks/CsvSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

SinkDescriptorPtr CsvSinkDescriptor::create(std::string fileName, CsvSinkDescriptor::FileOutputMode fileOutputMode) {
    return std::make_shared<CsvSinkDescriptor>(CsvSinkDescriptor(fileName, fileOutputMode));
}

CsvSinkDescriptor::CsvSinkDescriptor(std::string fileName,
                                     CsvSinkDescriptor::FileOutputMode fileOutputMode)
    : fileName(fileName), fileOutputMode(fileOutputMode) {}

const std::string& CsvSinkDescriptor::getFileName() const {
    return fileName;
}

CsvSinkDescriptor::FileOutputMode CsvSinkDescriptor::getFileOutputMode() const {
    return fileOutputMode;
}

const std::string& CsvSinkDescriptor::toString() {
    return "CSVSinkDescriptor()";
}

bool CsvSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<CsvSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<CsvSinkDescriptor>();
    return fileName == otherSinkDescriptor->fileName && fileOutputMode == otherSinkDescriptor->getFileOutputMode();
}

}// namespace NES