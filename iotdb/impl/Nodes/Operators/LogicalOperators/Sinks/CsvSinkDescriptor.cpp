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

}// namespace NES