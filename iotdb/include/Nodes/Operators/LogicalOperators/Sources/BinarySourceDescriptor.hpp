#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical binary source
 */
class BinarySourceDescriptor : public SourceDescriptor {

  public:
    BinarySourceDescriptor(SchemaPtr schema, std::string filePath);

    SourceDescriptorType getType() override;

    /**
     * @brief Get the path of binary file
     * @return
     */
    const std::string& getFilePath() const;

  private:
    std::string filePath;
};

typedef std::shared_ptr<BinarySourceDescriptor> BinarySourceDescriptorPtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
