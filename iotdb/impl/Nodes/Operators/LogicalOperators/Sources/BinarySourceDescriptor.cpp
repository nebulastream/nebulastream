#include "Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp"

namespace NES {

BinarySourceDescriptor::BinarySourceDescriptor(std::string filePath) : filePath(filePath){}

SourceDescriptorType BinarySourceDescriptor::getType() {
    return BinaryDescriptor;
}

const std::string& BinarySourceDescriptor::getFilePath() const {
    return filePath;
}


}

