#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(SchemaPtr schema, std::string fileName, FileOutputMode fileOutputMode, FileOutputType fileOutputType) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(schema, fileName, fileOutputMode, fileOutputType));
}

FileSinkDescriptor::FileSinkDescriptor(SchemaPtr schema,
                                       std::string fileName,
                                       FileOutputMode fileOutputMode,
                                       FileOutputType fileOutputType)
    : SinkDescriptor(schema), fileName(fileName), fileOutputMode(fileOutputMode), fileOutputType(fileOutputType) {}

const std::string& FileSinkDescriptor::getFileName() const {
    return fileName;
}
FileOutputMode FileSinkDescriptor::getFileOutputMode() const {
    return fileOutputMode;
}
FileOutputType FileSinkDescriptor::getFileOutputType() const {
    return fileOutputType;
}

}// namespace NES