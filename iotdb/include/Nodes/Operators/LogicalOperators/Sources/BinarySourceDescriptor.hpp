#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class BinarySourceDescriptor : public SourceDescriptor {

  public:
    BinarySourceDescriptor(std::string filePath);

    SourceDescriptorType getType() override;
    const std::string& getFilePath() const;

  private:
    std::string filePath;

};

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
