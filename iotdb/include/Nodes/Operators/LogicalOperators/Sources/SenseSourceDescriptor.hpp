#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class SenseSourceDescriptor : public SourceDescriptor {

  public:

    SenseSourceDescriptor(std::string udfs);

    SourceDescriptorType getType() override;

    const std::string& getUdfs() const;

  private:

    SenseSourceDescriptor()=default;

    std::string udfs;
};

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_
